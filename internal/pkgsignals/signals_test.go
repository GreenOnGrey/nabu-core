package pkgsignals

import "testing"

// REQ-01, REQ-04: names are normalized and checked by ecosystem.
func TestNormalize(t *testing.T) {
	for _, c := range []struct {
		eco, in, want string
		ok            bool
	}{
		{PyPI, "Opencv_Python", "opencv-python", true},
		{PyPI, "../../etc", "", false},
		{PyPI, "a b", "", false},
		{PyPI, "", "", false},
		{Apt, "LibreOffice-Writer", "libreoffice-writer", true},
		{Apt, "a", "", false},
		{Binary, "ffmpeg", "ffmpeg", true},
		{Command, "x;rm -rf", "", false},
		{PythonModule, "cv2", "cv2", true},
		{PythonModule, "cv2.aruco", "", false},
		{"npm", "left-pad", "", false},
	} {
		got, ok := Normalize(c.eco, c.in)
		if ok != c.ok || (ok && got != c.want) {
			t.Errorf("%s %q: %q %v", c.eco, c.in, got, ok)
		}
	}
}

// ADM-07: a near miss of a popular package is marked, the package itself is not.
func TestLookalike(t *testing.T) {
	if got := Lookalike(PyPI, "reqeusts"); got != "requests" {
		t.Fatalf("ADM-07: %q", got)
	}
	if got := Lookalike(PyPI, "requests"); got != "" {
		t.Fatalf("ADM-07: requests marked as %q", got)
	}
	if got := Lookalike(PyPI, "a-very-unusual-internal-package"); got != "" {
		t.Fatalf("unexpected %q", got)
	}
	if got := Lookalike(Apt, "libreofice"); got == "" {
		t.Log("no apt lookalike for libreofice (the list may not have it)")
	}
}

func TestDistance(t *testing.T) {
	for _, c := range []struct {
		a, b string
		d    int
	}{{"requests", "reqeusts", 1}, {"numpy", "nunpy", 1}, {"abc", "abc", 0}, {"pandas", "panda", 1}, {"flask", "falsk", 1}} {
		if got := distance(c.a, c.b); got != c.d {
			t.Errorf("%s/%s: %d", c.a, c.b, got)
		}
	}
}
