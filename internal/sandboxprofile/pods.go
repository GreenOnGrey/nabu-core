package sandboxprofile

// Parts of the pods that run profile images (tech §5.3): sandboxes, temporary
// sandboxes of runs, probe pods and the preloading DaemonSet share the
// security context and the init container that puts the nabu binary into
// the volume /nabu.

// NabuDir is where the init container puts the binary and the manifest.
const NabuDir = "/nabu"

// PodSecurity is the security context of the pod: user 1000 whatever the
// image says (R26).
func PodSecurity() map[string]any {
	return map[string]any{"runAsNonRoot": true, "runAsUser": 1000, "runAsGroup": 1000, "fsGroup": 1000,
		"seccompProfile": map[string]string{"type": "RuntimeDefault"}}
}

// ContainerSecurity is the security context of every container: read-only
// root, no privilege escalation (setuid does not raise rights), no
// capabilities.
func ContainerSecurity() map[string]any {
	return map[string]any{"runAsNonRoot": true, "allowPrivilegeEscalation": false, "readOnlyRootFilesystem": true,
		"capabilities": map[string]any{"drop": []string{"ALL"}}}
}

// InitContainer is nabu-init: the nabu binary of the current version copies
// itself (and, for a sandbox, the manifest of the profile) to /nabu.
func InitContainer(image string, args []string, env []map[string]any) map[string]any {
	c := map[string]any{
		"name": "nabu-init", "image": image, "args": args,
		"volumeMounts":    []map[string]any{{"name": "nabu", "mountPath": NabuDir}},
		"securityContext": ContainerSecurity(),
		"resources": map[string]any{
			"requests": map[string]string{"cpu": "10m", "memory": "16Mi"},
			"limits":   map[string]string{"cpu": "200m", "memory": "64Mi"},
		},
	}
	if len(env) > 0 {
		c["env"] = env
	}
	return c
}

// NabuVolume is the volume of the binary and the manifest.
func NabuVolume() map[string]any {
	return map[string]any{"name": "nabu", "emptyDir": map[string]any{"sizeLimit": "64Mi"}}
}

// NabuMount mounts /nabu read-only into the container of the profile image:
// the image cannot replace the binary that talks to the relay.
func NabuMount() map[string]any {
	return map[string]any{"name": "nabu", "mountPath": NabuDir, "readOnly": true}
}

// PullSecrets are the imagePullSecrets of the pods.
func PullSecrets(names []string) []map[string]string {
	out := make([]map[string]string, 0, len(names))
	for _, n := range names {
		out = append(out, map[string]string{"name": n})
	}
	return out
}
