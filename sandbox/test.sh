#!/usr/bin/env bash
# test.sh <base image> <documents image> [<code image>] — checks of the
# built-in sandbox profiles (FTR.NAB.CMN-0008 qa, PRF-01…07; FTR.HMR.CMN-0007
# qa, PF-01, PF-02, PF-05): the images run as user 1000 with a read-only root and /tmp as
# tmpfs, like in a sandbox pod.
set -euo pipefail
base=$1 documents=$2 code=${3:-}
run() { local img=$1; shift; docker run --rm --read-only --user 1000:1000 --tmpfs /tmp:rw,size=256m --network none "$img" bash -c "$*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

echo "PRF-01: tools of base"
for img in "$base" "$documents"; do
  run "$img" 'for c in zip unzip xz bzip2 jq rg file tree patch less bc sqlite3 node git python3; do command -v $c >/dev/null || { echo "no $c"; exit 1; }; done' ||
    fail "PRF-01 $img"
done

echo "PRF-02: Python libraries of base"
run "$base" 'python3 -c "import yaml, jinja2, dateutil, charset_normalizer, tabulate, openpyxl, PIL, pypdf"' || fail PRF-02

echo "PRF-03: libraries and tools of documents"
run "$documents" 'python3 -c "import docx, pptx, docxtpl, lxml, pandas, numpy, matplotlib, weasyprint, fpdf, markdown, reportlab" &&
  typst --version && pdftotext -v 2>&1 | head -1 && pdftoppm -v 2>&1 | head -1 && qpdf --version | head -1' || fail PRF-03

echo "PRF-04: HTML assets"
run "$documents" 'test -s /opt/nabu/assets/chart.js/chart.umd.min.js && test -s /opt/nabu/assets/echarts/echarts.min.js &&
  test -s /opt/nabu/assets/mermaid/mermaid.min.js && test -s /opt/nabu/assets/katex/katex.min.js && test -d /opt/nabu/assets/katex/fonts' ||
  fail PRF-04

echo "PRF-05: fonts with Cyrillic"
for img in "$base" "$documents"; do
  fonts=$(run "$img" 'fc-list :lang=ru family')
  for f in "DejaVu Sans" "Liberation Sans" "PT Sans" "PT Serif"; do
    grep -q "$f" <<<"$fonts" || fail "PRF-05 $img: no $f"
  done
done

echo "PRF-06: no package managers"
for img in "$base" "$documents"; do
  run "$img" '! command -v pip && ! command -v pip3 && ! command -v npm && ! python3 -m pip --version 2>/dev/null &&
    ! python3 -m ensurepip --version 2>/dev/null' >/dev/null || fail "PRF-06 $img"
  if run "$img" 'apt-get install -y curl' >/dev/null 2>&1; then fail "PRF-06 $img: apt-get works"; fi
done

echo "PRF-07: charts and PDF on a read-only root"
run "$documents" 'cd /tmp && mkdir -p mpl && cp -r /opt/nabu/mpl-cache/. mpl/ &&
  python3 -c "
import matplotlib; matplotlib.use(\"Agg\")
import matplotlib.pyplot as plt
plt.title(\"Продажи по месяцам\"); plt.bar([1, 2, 3], [3, 5, 2]); plt.savefig(\"/tmp/chart.png\")
from weasyprint import HTML
HTML(string=\"<h1>Отчёт</h1><p>Проверка</p>\").write_pdf(\"/tmp/w.pdf\")
" 2>/tmp/err && ! grep -qi "not writable\|read-only" /tmp/err &&
  printf "= Отчёт\nПроверка" >/tmp/t.typ && typst compile /tmp/t.typ /tmp/t.pdf &&
  test -s /tmp/chart.png && test -s /tmp/w.pdf && test -s /tmp/t.pdf && pdftotext /tmp/w.pdf - | grep -q Отчёт' || fail PRF-07

if [[ -n $code ]]; then
  echo "PF-01: tools of code"
  run "$code" 'for c in go gopls gofumpt golangci-lint node npm npx pnpm tsc eslint prettier uv ruff pytest mypy git python3; do
    command -v $c >/dev/null || { echo "no $c"; exit 1; }; done && test ! -e /usr/bin/apt-get -o ! -w /var/lib/dpkg' || fail "PF-01"
  echo "PF-05: curl and procps in code, not in base and documents"
  run "$code" 'command -v curl && command -v ps && command -v pkill && ps -e >/dev/null' >/dev/null || fail "PF-05"
  for img in "$base" "$documents"; do
    if run "$img" 'command -v curl' >/dev/null; then fail "PF-05 $img: curl"; fi
  done
  echo "PF-02: Go, Python and JavaScript work offline in /work"
  # exec on /tmp and /work as on the emptyDir of a pod: go test runs the binaries it builds
  docker run --rm --read-only --user 1000:1000 --tmpfs /tmp:rw,exec,size=256m --tmpfs /work:rw,exec,size=1g,uid=1000 --network none "$code" bash -c '
    cd /work && mkdir g && cd g && printf "module x\ngo 1.27\n" >go.mod && printf "package x\nfunc F() int { return 1 }\n" >x.go &&
    printf "package x\nimport \"testing\"\nfunc TestF(t *testing.T) { if F() != 1 { t.Fatal() } }\n" >x_test.go && go test ./... &&
    cd /work && uv venv -q .venv && printf "def test_a():\n    assert 1\n" >t_a.py && pytest -q t_a.py && ruff check t_a.py &&
    printf "const a: number = 1; console.log(a)\n" >a.ts && tsc --strict a.ts && node a.js' || fail "PF-02"
fi

echo "ok"
