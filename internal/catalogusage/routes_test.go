package catalogusage

import (
	"net/http"
	"testing"

	"github.com/go-chi/chi/v5"
)

// CTR-02: the report and the actions live on the same router as the catalog
// of the administration; the patterns of the feature do not conflict with
// the existing paths of /admin/api/v1 (chi panics on a conflict at mount).
func TestRoutesRegistration(t *testing.T) {
	r := chi.NewRouter()
	r.Route("/admin/api/v1", func(r chi.Router) {
		// the paths of FTR.NAB.CMN-0001 around the catalog
		r.Get("/catalog", func(http.ResponseWriter, *http.Request) {})
		r.Get("/catalog/{id}", func(http.ResponseWriter, *http.Request) {})
		r.Post("/catalog", func(http.ResponseWriter, *http.Request) {})
		r.Patch("/catalog/{id}", func(http.ResponseWriter, *http.Request) {})
		r.Delete("/catalog/{id}", func(http.ResponseWriter, *http.Request) {})
		r.Post("/catalog/{id}/check", func(http.ResponseWriter, *http.Request) {})
		r.Post("/catalog/{id}/sync", func(http.ResponseWriter, *http.Request) {})
		r.Post("/catalog/{id}/upload", func(http.ResponseWriter, *http.Request) {})
		(&Service{Cfg: DefaultConfig()}).Routes(r)
	})
	if r == nil {
		t.Fatal("the router was not built")
	}
}
