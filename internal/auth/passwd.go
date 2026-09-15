package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

const hashIterations = 100000

func HashPassword(pw string) (string, error) {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	h := hmac.New(sha256.New, salt)
	h.Write([]byte(pw))
	cur := h.Sum(nil)
	for i := 1; i < hashIterations; i++ {
		h = hmac.New(sha256.New, salt)
		h.Write(cur)
		cur = h.Sum(nil)
	}
	return fmt.Sprintf("%d:%s:%s", hashIterations, hex.EncodeToString(salt), hex.EncodeToString(cur)), nil
}

func VerifyPassword(stored, pw string) bool {
	parts := strings.Split(stored, ":")
	if len(parts) != 3 {
		return false
	}
	iters, err := strconv.Atoi(parts[0])
	if err != nil || iters < 1 {
		return false
	}
	salt, err := hex.DecodeString(parts[1])
	if err != nil || len(salt) == 0 {
		return false
	}
	want, err := hex.DecodeString(parts[2])
	if err != nil {
		return false
	}
	h := hmac.New(sha256.New, salt)
	h.Write([]byte(pw))
	cur := h.Sum(nil)
	for i := 1; i < iters; i++ {
		h = hmac.New(sha256.New, salt)
		h.Write(cur)
		cur = h.Sum(nil)
	}
	return subtle.ConstantTimeCompare(cur, want) == 1
}
