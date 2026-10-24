package sandboxprofile

import (
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/k8s"
)

func TestImageAt(t *testing.T) {
	for in, want := range map[string]string{
		"registry.local/nabu/tools:2025.10":       "registry.local/nabu/tools@sha256:ab",
		"registry.local:5000/nabu/tools:1":        "registry.local:5000/nabu/tools@sha256:ab",
		"registry.local:5000/nabu/tools":          "registry.local:5000/nabu/tools@sha256:ab",
		"registry.local/nabu/tools@sha256:00":     "registry.local/nabu/tools@sha256:ab",
		"registry.local/nabu/tools:1.0@sha256:00": "registry.local/nabu/tools@sha256:ab",
	} {
		if got := ImageAt(in, "sha256:ab"); got != want {
			t.Errorf("ImageAt(%q) = %q, want %q", in, got, want)
		}
	}
}

// CUS-02, CUS-03: only images under the prefixes of the stand.
func TestAllowedImage(t *testing.T) {
	s := &Service{Cfg: Config{CustomImages: true, Prefixes: []string{"registry.local/nabu/"}}}
	for ref, ok := range map[string]bool{
		"registry.local/nabu/tools:1":       true,
		"registry.local/nabu/x@sha256:aaaa": true,
		"docker.io/library/python:3.12":     false,
		"registry.local/nabu-evil/x":        false,
		"registry.local/nabu/../x":          false,
		"registry.local/nabu/":              false,
		"registry.local/nabu/x y":           false,
		"registry.local//nabu/x":            false,
	} {
		err := s.AllowedImage(ref)
		if ok != (err == nil) {
			t.Errorf("%q: %v", ref, err)
		}
		var ae *apperr.Error
		if err != nil && (!errors.As(err, &ae) || ae.Code != "sandbox_image_registry_denied") {
			t.Errorf("%q: code %v", ref, err)
		}
	}
}

func TestNormalizePyPI(t *testing.T) {
	for in, want := range map[string]string{"Opencv_Python": "opencv-python", "zope.interface": "zope-interface", "a__b--c": "a-b-c"} {
		if got := NormalizePyPI(in); got != want {
			t.Errorf("%q: %q", in, got)
		}
	}
}

// REQ-02: a package is found in a profile by its ecosystem.
func TestManifestHas(t *testing.T) {
	m := &Manifest{Python: []Package{{Name: "Pandas"}}, System: []Package{{Name: "poppler-utils"}},
		Tools: []Tool{{Name: "typst"}}, Commands: []string{"ffmpeg"}}
	for _, c := range []struct {
		eco, name string
		ok        bool
	}{{"pypi", "pandas", true}, {"apt", "poppler-utils", true}, {"binary", "typst", true}, {"binary", "ffmpeg", true},
		{"pypi", "numpy", false}, {"apt", "pandas", false}} {
		if m.Has(c.eco, c.name) != c.ok {
			t.Errorf("%s:%s", c.eco, c.name)
		}
	}
}

func TestDigestOf(t *testing.T) {
	for in, want := range map[string]string{
		"docker-pullable://registry.local/nabu/x@sha256:abc": "sha256:abc",
		"registry.local/nabu/x@sha256:abc":                   "sha256:abc",
		"sha256:abc":                                         "sha256:abc",
		"":                                                   "",
	} {
		if got := digestOf(in); got != want {
			t.Errorf("%q: %q", in, got)
		}
	}
}

// CUS-06: an image that cannot be pulled fails the check at once.
func TestPullFailed(t *testing.T) {
	var p k8s.Pod
	p.Status.ContainerStatuses = []k8s.ContainerStatus{{}}
	p.Status.ContainerStatuses[0].State.Waiting = &struct {
		Reason  string `json:"reason"`
		Message string `json:"message"`
	}{Reason: "ImagePullBackOff"}
	if !pullFailed(&p) {
		t.Fatal("CUS-06")
	}
}

// RUN-01, CUS-14: probe pods run the image under user 1000 with the binary of
// the init container on a read-only volume.
func TestProbeManifest(t *testing.T) {
	w := &Worker{Signer: jwt.NewSigner("https://nabu.test", []byte("0123456789abcdef0123456789abcdef")), Cfg: WorkerConfig{Namespace: "nabu-sandboxes", InitImage: "ghcr.io/x/nabu-core:v1", PullSecrets: []string{"ghcr-pull", "corp"}}}
	started := time.Date(2026, 10, 24, 12, 0, 0, 123456000, time.UTC)
	m := w.probeManifest("tools", "registry.local/nabu/tools:1", started)
	name := m["metadata"].(map[string]any)["name"].(string)
	if !regexp.MustCompile(`^sandbox-probe-tools-[0-9a-z]{5}$`).MatchString(name) || name != probePod("tools", started) {
		t.Fatalf("tech §5.7: a pod per check: %s", name)
	}
	if probePod("tools", started.Add(time.Microsecond)) == name {
		t.Fatal("another check, another pod")
	}
	spec := m["spec"].(map[string]any)
	if spec["restartPolicy"] != "Never" || spec["automountServiceAccountToken"] != false {
		t.Fatal("probe pod spec")
	}
	if sec := spec["securityContext"].(map[string]any); sec["runAsUser"] != 1000 || sec["runAsNonRoot"] != true {
		t.Fatal("CUS-14: user 1000")
	}
	if len(spec["imagePullSecrets"].([]map[string]string)) != 2 {
		t.Fatal("pull secrets")
	}
	c := spec["containers"].([]map[string]any)[0]
	if c["image"] != "registry.local/nabu/tools:1" || c["command"].([]string)[0] != "/nabu/bin/nabu" {
		t.Fatalf("container %v", c)
	}
	mounts := c["volumeMounts"].([]map[string]any)
	if mounts[0]["name"] != "nabu" || mounts[0]["readOnly"] != true {
		t.Fatal("RUN-07: /nabu read-only")
	}
	if cs := c["securityContext"].(map[string]any); cs["allowPrivilegeEscalation"] != false || cs["readOnlyRootFilesystem"] != true {
		t.Fatal("CUS-14: no escalation, read-only root")
	}
}
