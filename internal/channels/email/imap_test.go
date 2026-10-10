package email

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapclient"
	"github.com/emersion/go-imap/v2/imapserver"
	"github.com/emersion/go-imap/v2/imapserver/imapmemserver"
)

func selfSigned(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "imap.test"}, DNSNames: []string{"imap.test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

type literal struct{ *bytes.Reader }

func (l literal) Size() int64 { return l.Reader.Size() }

func lit(s string) literal {
	return literal{bytes.NewReader([]byte(strings.ReplaceAll(s, "\n", "\r\n")))}
}

// ML-01 (flag and folder), ML-05 (new letters during IDLE): the receiver
// against an IMAP server in memory.
func TestReceiverSession(t *testing.T) {
	cert := selfSigned(t)
	mem := imapmemserver.New()
	user := imapmemserver.NewUser("nabu@company.ru", "app-pass")
	if err := user.Create("INBOX", nil); err != nil {
		t.Fatal(err)
	}
	mem.AddUser(user)
	for _, id := range []string{"a", "b"} {
		if _, err := user.Append("INBOX", lit("From: ivan@company.ru\nMessage-ID: <"+id+"@company.ru>\nSubject: "+id+"\n\nbody "+id+"\n"), &imap.AppendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	srv := imapserver.New(&imapserver.Options{
		NewSession: func(*imapserver.Conn) (imapserver.Session, *imapserver.GreetingData, error) {
			return mem.NewSession(), nil, nil
		},
		Caps:   imap.CapSet{imap.CapIMAP4rev1: {}, imap.CapMove: {}, imap.CapIdle: {}},
		Logger: quiet{},
	})
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()

	pool := x509.NewCertPool()
	leaf, _ := x509.ParseCertificate(cert.Certificate[0])
	pool.AddCert(leaf)
	host, port, _ := net.SplitHostPort(ln.Addr().String())
	p, _ := new(big.Int).SetString(port, 10)
	c := Credentials{Settings: Settings{Provider: Yandex360, Mailbox: "nabu@company.ru", IMAP: Endpoint{Host: host, Port: int(p.Int64())}},
		AppPassword: "app-pass", tlsConfig: &tls.Config{RootCAs: pool, ServerName: "imap.test"}}

	var mu sync.Mutex
	var got []string
	m := &Mail{reset: make(chan struct{}, 1)}
	m.handle = func(_ context.Context, _ Credentials, raw []byte, key string) string {
		l, err := Parse(raw, 1<<20)
		mu.Lock()
		defer mu.Unlock()
		if err != nil {
			got = append(got, "unreadable "+key)
		} else {
			got = append(got, l.MessageID)
		}
		return Accepted
	}
	count := func() int { mu.Lock(); defer mu.Unlock(); return len(got) }
	wait := func(n int) {
		t.Helper()
		for i := 0; i < 100; i++ {
			if count() >= n {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("%d letters handled, want %d: %v", count(), n, got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.session(ctx, c) }()
	wait(2)
	// a letter that comes while the receiver idles is handled without waiting for the reconciliation
	if _, err := user.Append("INBOX", lit("From: ivan@company.ru\nMessage-ID: <c@company.ru>\nSubject: c\n\nbody c\n"), &imap.AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	wait(3)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("session: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the session does not stop")
	}
	if strings.Join(got, ",") != "a@company.ru,b@company.ru,c@company.ru" {
		t.Fatal(got)
	}
	// handled letters left the inbox for Nabu/Processed with the flag
	cl, err := c.DialIMAP(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	inbox, err := cl.Select("INBOX", nil).Wait()
	if err != nil || inbox.NumMessages != 0 {
		t.Fatalf("INBOX: %+v %v", inbox, err)
	}
	processed, err := cl.Select(ProcessedFolder, nil).Wait()
	if err != nil || processed.NumMessages != 3 {
		t.Fatalf("%s: %+v %v", ProcessedFolder, processed, err)
	}
	flagged, err := cl.UIDSearch(&imap.SearchCriteria{Flag: []imap.Flag{ProcessedFlag}}, nil).Wait()
	if err != nil || len(flagged.AllUIDs()) != 3 {
		t.Fatalf("flags: %v %v", flagged.AllUIDs(), err)
	}
	// a wrong password is a sign-in error, not a hang
	bad := c
	bad.AppPassword = "wrong"
	if _, err := bad.DialIMAP(context.Background(), &imapclient.UnilateralDataHandler{}); err == nil || !strings.Contains(err.Error(), "sign-in") {
		t.Fatalf("wrong password: %v", err)
	}
}

type quiet struct{}

func (quiet) Printf(string, ...any) {}
