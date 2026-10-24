package sandbox

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/GreenOnGrey/nabu-core/internal/sandboxprofile"
)

// FTR.NAB.CMN-0008: the init container of a sandbox and the probe of a custom
// image. Both run the nabu binary of the current version, so the image of a
// profile needs nothing of Nabu but bash.

const (
	nabuDir      = "/nabu"
	profileFile  = nabuDir + "/sandbox-profile.json"
	mplCacheFrom = "/opt/nabu/mpl-cache"
)

// InitConfig configures sandbox-init.
type InitConfig struct {
	BinaryOnly bool // probe pods and the preloading DaemonSet
	Profile    string
	APIURL     string
	Token      string
	Dir        string // /nabu; tests set it
}

// Init copies this binary to /nabu/bin/nabu and, for a sandbox, writes the
// manifest of its profile. A failure to read the manifest does not stop the
// sandbox: the agent then sees only the name of the profile.
func Init(ctx context.Context, cfg InitConfig) error {
	dir := cfg.Dir
	if dir == "" {
		dir = nabuDir
	}
	self, err := os.Executable()
	if err != nil {
		return err
	}
	if err := copyFile(self, filepath.Join(dir, "bin", "nabu"), 0o555); err != nil {
		return fmt.Errorf("sandbox-init: %w", err)
	}
	if cfg.BinaryOnly {
		return nil
	}
	m := []byte(fmt.Sprintf(`{"name":%q}`, cfg.Profile))
	if cfg.APIURL != "" && cfg.Token != "" {
		if b, err := getJSON(ctx, cfg.APIURL+"/internal/v1/sandbox/profile", cfg.Token); err == nil {
			m = b
		} else {
			slog.Warn("sandbox profile manifest", "err", err)
		}
	}
	return os.WriteFile(filepath.Join(dir, "sandbox-profile.json"), m, 0o444)
}

// PrepareProfile readies the caches of a profile image before commands run:
// the font cache of matplotlib built into the documents image goes to the
// writable MPLCONFIGDIR, so the first chart does not scan the fonts.
func PrepareProfile() {
	dst := os.Getenv("MPLCONFIGDIR")
	if dst == "" {
		return
	}
	if _, err := os.Stat(mplCacheFrom); err != nil {
		return
	}
	if _, err := os.Stat(dst); err == nil {
		return
	}
	_ = filepath.WalkDir(mplCacheFrom, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		rel, _ := filepath.Rel(mplCacheFrom, p)
		if d.IsDir() {
			return os.MkdirAll(filepath.Join(dst, rel), 0o755)
		}
		return copyFile(p, filepath.Join(dst, rel), 0o644)
	})
}

func copyFile(src, dst string, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp := dst + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmp, mode); err != nil {
		return err
	}
	return os.Rename(tmp, dst)
}

func getJSON(ctx context.Context, u, token string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK || !json.Valid(b) {
		return nil, fmt.Errorf("GET %s: %d", u, resp.StatusCode)
	}
	return b, nil
}

// ─── Probe ──────────────────────────────────────────────────────────

// Probe checks the image it runs in (bash for user 1000, a read-only root, a
// writable /tmp), collects its contents and reports them (tech §5.7). It
// exits with an error when a check fails.
func Probe(ctx context.Context, apiURL, token string) error {
	rep := probe(ctx)
	b, _ := json.Marshal(rep)
	if len(b) > 2<<20 {
		// Too many packages: drop the command list first, then system packages.
		rep.Inventory.Commands = nil
		if b, _ = json.Marshal(rep); len(b) > 2<<20 {
			rep.Inventory.System = nil
			b, _ = json.Marshal(rep)
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL+"/internal/v1/sandbox/probe", bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("probe report: %d", resp.StatusCode)
	}
	if !rep.OK {
		return errors.New(rep.Error)
	}
	return nil
}

// The report and the inventory travel in the types of the api (tech §5.7).
type (
	probeReport = sandboxprofile.ProbeReport
	pkg         = sandboxprofile.Package
)

func probe(ctx context.Context) probeReport {
	rep := probeReport{Inventory: &sandboxprofile.Manifest{Kind: sandboxprofile.KindCustom, Version: os.Getenv("NABU_BASE_VERSION")}}
	if _, err := exec.LookPath("bash"); err != nil {
		rep.Error = "sandbox_probe_no_bash"
		return rep
	}
	out, err := exec.CommandContext(ctx, "bash", "-c", "id -u").Output()
	if err != nil || strings.TrimSpace(string(out)) != "1000" {
		rep.Error = "sandbox_probe_user"
		return rep
	}
	if f, err := os.CreateTemp("/", ".nabu-probe-"); err == nil {
		f.Close()
		_ = os.Remove(f.Name())
		rep.Error = "sandbox_probe_fs"
		return rep
	}
	if f, err := os.CreateTemp(os.TempDir(), ".nabu-probe-"); err != nil {
		rep.Error = "sandbox_probe_fs"
		return rep
	} else {
		f.Close()
		_ = os.Remove(f.Name())
	}
	rep.Inventory.System = systemPackages(ctx)
	rep.Inventory.Python = pythonPackages(ctx)
	rep.Inventory.Commands = commands()
	rep.OK = true
	return rep
}

// systemPackages lists packages with dpkg, rpm or apk, whichever exists.
func systemPackages(ctx context.Context) []pkg {
	for _, c := range [][]string{
		{"dpkg-query", "-W", "-f", "${Package}\t${Version}\n"},
		{"rpm", "-qa", "--qf", "%{NAME}\t%{VERSION}-%{RELEASE}\n"},
		{"apk", "info", "-v"},
	} {
		if _, err := exec.LookPath(c[0]); err != nil {
			continue
		}
		out, err := exec.CommandContext(ctx, c[0], c[1:]...).Output()
		if err != nil {
			continue
		}
		var list []pkg
		sc := bufio.NewScanner(bytes.NewReader(out))
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" {
				continue
			}
			if name, ver, ok := strings.Cut(line, "\t"); ok {
				list = append(list, pkg{Name: name, Version: ver})
			} else {
				list = append(list, pkg{Name: line}) // apk: name-version
			}
		}
		return list
	}
	return nil
}

const pyList = `import importlib.metadata as m, json
print(json.dumps(sorted({(d.metadata["Name"] or "", d.version) for d in m.distributions()})))`

// pythonPackages lists the distributions of every python3 in PATH.
func pythonPackages(ctx context.Context) []pkg {
	seen := map[string]bool{}
	var out []pkg
	for _, py := range interpreters() {
		b, err := exec.CommandContext(ctx, py, "-c", pyList).Output()
		if err != nil {
			continue
		}
		var list [][2]string
		if json.Unmarshal(b, &list) != nil {
			continue
		}
		for _, p := range list {
			if p[0] != "" && !seen[p[0]] {
				seen[p[0]] = true
				out = append(out, pkg{Name: p[0], Version: p[1]})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func interpreters() []string {
	seen := map[string]bool{}
	var out []string
	for _, dir := range filepath.SplitList(os.Getenv("PATH")) {
		matches, _ := filepath.Glob(filepath.Join(dir, "python3*"))
		for _, m := range matches {
			base := filepath.Base(m)
			if strings.Contains(base, "-config") {
				continue
			}
			real, err := filepath.EvalSymlinks(m)
			if err != nil || seen[real] {
				continue
			}
			seen[real] = true
			out = append(out, m)
		}
	}
	return out
}

// commands are the executables of PATH, at most 5000.
func commands() []string {
	seen := map[string]bool{}
	var out []string
	for _, dir := range filepath.SplitList(os.Getenv("PATH")) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if seen[e.Name()] || e.IsDir() {
				continue
			}
			info, err := e.Info()
			if err != nil || info.Mode()&0o111 == 0 && info.Mode()&fs.ModeSymlink == 0 {
				continue
			}
			seen[e.Name()] = true
			out = append(out, e.Name())
			if len(out) >= 5000 {
				sort.Strings(out)
				return out
			}
		}
	}
	sort.Strings(out)
	return out
}
