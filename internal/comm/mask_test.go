package comm

import (
	"strings"
	"testing"
)

// NOTE-07: passwords, keys, tokens and card numbers do not reach the store.
func TestMask(t *testing.T) {
	secrets := []string{
		"Пароль от стенда: Qwerty!2345",
		"password = hunter2secret",
		"token: abcdef123456",
		"api key is ZXCVBN-0987",
		"ключ AKIAIOSFODNN7EXAMPLE лежит в сейфе",
		"Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c",
		"ghp_abcdefghijklmnopqrstuvwxyz0123456789",
		"-----BEGIN RSA PRIVATE KEY-----\nMIIEow\n-----END RSA PRIVATE KEY-----",
		"карта 4111 1111 1111 1111 до 12/27",
		"hash 9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
	}
	leaks := []string{"Qwerty!2345", "hunter2secret", "abcdef123456", "ZXCVBN-0987", "AKIAIOSFODNN7EXAMPLE", "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c",
		"ghp_abcdefghijklmnopqrstuvwxyz0123456789", "MIIEow", "4111 1111 1111 1111", "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"}
	for i, s := range secrets {
		out, n := Mask(s)
		if n == 0 || strings.Contains(out, leaks[i]) || !strings.Contains(out, Masked) {
			t.Errorf("not masked: %q -> %q", s, out)
		}
	}
	plain := []string{
		"Выпуск 2.3 переносится с 17 на 24 октября: не готова миграция справочников.",
		"Иванов пришлёт обновлённый план до среды, договор № 4500123456 от 01.02.2026.",
		"Пароль передан отдельным письмом.",
		"Созвон по телефону +7 916 123-45-67, заказ 1234567890123456.",
		"https://mail.google.com/mail/#all/18c2f0a1b2c3d4e5",
	}
	for _, s := range plain {
		if out, n := Mask(s); n != 0 {
			t.Errorf("masked a plain text: %q -> %q", s, out)
		}
	}
}
