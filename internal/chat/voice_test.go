package chat

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// wav builds a WAV recording of the given length: 16 kHz mono 16-bit.
func wav(seconds int) []byte {
	const rate = 32000 // bytes a second
	var b bytes.Buffer
	b.WriteString("RIFF")
	_ = binary.Write(&b, binary.LittleEndian, uint32(36+seconds*rate))
	b.WriteString("WAVEfmt ")
	for _, v := range []any{uint32(16), uint16(1), uint16(1), uint32(16000), uint32(rate), uint16(2), uint16(16)} {
		_ = binary.Write(&b, binary.LittleEndian, v)
	}
	b.WriteString("data")
	_ = binary.Write(&b, binary.LittleEndian, uint32(seconds*rate))
	b.Write(make([]byte, seconds*rate))
	return b.Bytes()
}

// VOC-05: the limit is the length of the recording, not a guess by its size.
func TestVoiceLimit(t *testing.T) {
	a := &API{MaxVoiceSeconds: 10}
	if sec, ok := wavSeconds(wav(3)); !ok || sec != 3 {
		t.Fatalf("wavSeconds = %v %v", sec, ok)
	}
	for _, c := range []struct {
		name string
		data []byte
		long bool
	}{
		{"WAV of 9 s, larger than compressed audio of 10 s", wav(9), false},
		{"WAV of 11 s", wav(11), true},
		{"opus of 480 KiB", make([]byte, 480<<10), false},
		{"opus of 481 KiB", make([]byte, 481<<10), true},
	} {
		if got := a.tooLong(c.data); got != c.long {
			t.Errorf("%s: tooLong = %v", c.name, got)
		}
	}
	if (&API{}).maxVoiceSeconds() != 300 {
		t.Error("the default limit is five minutes")
	}
}
