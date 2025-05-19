package util

import (
	"github.com/jxskiss/base62"
	"github.com/zeebo/xxh3"
)

func HashString(s *string) string {
	if s == nil {
		return ""
	}

	h := xxh3.Hash128([]byte(*s))
	var b []byte
	hBytes := h.Bytes()
	for i := 0; i < 16; i++ {
		b = append(b, hBytes[i])
	}
	return base62.EncodeToString(b)
}
