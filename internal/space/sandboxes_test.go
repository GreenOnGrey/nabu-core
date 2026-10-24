package space

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/k8s"
	"github.com/GreenOnGrey/nabu-core/internal/sandboxprofile"
)

func testSandboxes() *Sandboxes {
	signer := jwt.NewSigner("https://nabu.test", []byte("0123456789abcdef0123456789abcdef"))
	return &Sandboxes{Space: &Service{Signer: signer}, Cfg: SandboxConfig{Namespace: "nabu-sandboxes", Image: "ghcr.io/x/nabu-core:v1",
		CPU: "1", Memory: "1Gi", Quota: 5 << 30, InitImage: "ghcr.io/x/nabu-core:v2", PullSecrets: []string{"ghcr-pull", "corp-pull"}}}
}

// FTR.NAB.CMN-0008 RUN-01: a profile image runs the nabu binary of the init
// container from a read-only /nabu, with the security of a sandbox.
func TestSandboxManifestProfile(t *testing.T) {
	m := testSandboxes()
	p := sandboxprofile.Profile{Name: "documents", Kind: "builtin", Image: "ghcr.io/x/nabu-sandbox-documents:v2"}
	man := m.manifest(uuid.New(), p)
	meta := man["metadata"].(map[string]any)
	if meta["labels"].(map[string]string)["nabu.io/profile"] != "documents" || meta["annotations"].(map[string]string)["nabu.io/image"] != p.Image {
		t.Fatal("RUN-01: label and annotation of the profile")
	}
	spec := man["spec"].(map[string]any)
	inits := spec["initContainers"].([]map[string]any)
	if len(inits) != 1 || inits[0]["image"] != "ghcr.io/x/nabu-core:v2" || inits[0]["args"].([]string)[0] != "sandbox-init" {
		t.Fatalf("RUN-01: init container %v", inits)
	}
	c := spec["containers"].([]map[string]any)[0]
	if c["image"] != p.Image || c["command"].([]string)[0] != "/nabu/bin/nabu" || c["args"].([]string)[0] != "sandbox" {
		t.Fatalf("RUN-01: container %v", c)
	}
	ro := false
	for _, vm := range c["volumeMounts"].([]map[string]any) {
		ro = ro || (vm["name"] == "nabu" && vm["readOnly"] == true)
	}
	if !ro {
		t.Fatal("RUN-07: /nabu is mounted read-only")
	}
	if sec := c["securityContext"].(map[string]any); sec["readOnlyRootFilesystem"] != true || sec["allowPrivilegeEscalation"] != false {
		t.Fatal("RUN-01: security context")
	}
	if len(spec["imagePullSecrets"].([]map[string]string)) != 2 {
		t.Fatal("pull secrets of the stand")
	}
}

// RUN-08: without SANDBOX_PROFILES the pod is as before the feature.
func TestSandboxManifestLegacy(t *testing.T) {
	m := testSandboxes()
	man := m.manifest(uuid.New(), sandboxprofile.Profile{Name: "base", Image: m.Cfg.Image, Legacy: true})
	spec := man["spec"].(map[string]any)
	if _, ok := spec["initContainers"]; ok {
		t.Fatal("RUN-08: no init container in the legacy mode")
	}
	c := spec["containers"].([]map[string]any)[0]
	if c["image"] != m.Cfg.Image || c["command"] != nil {
		t.Fatalf("RUN-08: %v", c)
	}
}

// FTR.NAB.CMN-0008 tech §7.10: an image that does not pull and a crashing
// container are failures at once, in the init container too; a pod that waits
// for its image may still start.
func TestStartFailure(t *testing.T) {
	for raw, want := range map[string]string{
		`{"status":{"containerStatuses":[{"state":{"waiting":{"reason":"ImagePullBackOff"}}}]}}`:          "pull",
		`{"status":{"initContainerStatuses":[{"state":{"waiting":{"reason":"ErrImagePull"}}}]}}`:          "pull",
		`{"status":{"containerStatuses":[{"state":{"waiting":{"reason":"CrashLoopBackOff"}}}]}}`:          "crash",
		`{"status":{"containerStatuses":[{"state":{"waiting":{"reason":"ContainerCreating"}}}]}}`:         "",
		`{"status":{"containerStatuses":[{"state":{"terminated":{"exitCode":0,"reason":"Completed"}}}]}}`: "",
	} {
		var p k8s.Pod
		if err := json.Unmarshal([]byte(raw), &p); err != nil {
			t.Fatal(err)
		}
		if got := startFailure(&p); got != want {
			t.Errorf("%s: %q", raw, got)
		}
	}
}
