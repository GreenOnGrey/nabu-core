package storage

import "testing"

// An upload of unknown size goes in parts of 16 MiB, not in the part minio-go
// sizes for 5 TiB: each upload held ~550 MiB and the api died of OOM.
func TestPutOptions(t *testing.T) {
	if o := putOptions(-1, "application/octet-stream"); o.PartSize != unknownSizePart || o.ContentType != "application/octet-stream" {
		t.Fatalf("unknown size: %+v", o)
	}
	if o := putOptions(1024, "text/plain"); o.PartSize != 0 {
		t.Fatalf("a known size keeps the choice of minio-go: %+v", o)
	}
}
