package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

var (
	ErrUnauthorized = errors.New("token invalido")
	ErrExpired      = errors.New("token expirado")
)

type Claims struct {
	Username string `json:"username"`
	Role     string `json:"role"`
	Exp      int64  `json:"exp"`
}

func b64e(b []byte) string {
	return base64.RawURLEncoding.EncodeToString(b)
}

func b64d(s string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(s)
}

func Sign(secret []byte, c Claims) (string, error) {
	header := b64e([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payload, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	body := header + "." + b64e(payload)
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(body))
	return body + "." + b64e(mac.Sum(nil)), nil
}

func Verify(secret []byte, token string) (Claims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return Claims{}, ErrUnauthorized
	}
	sig, err := b64d(parts[2])
	if err != nil {
		return Claims{}, ErrUnauthorized
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(parts[0] + "." + parts[1]))
	if subtle.ConstantTimeCompare(sig, mac.Sum(nil)) != 1 {
		return Claims{}, ErrUnauthorized
	}
	payload, err := b64d(parts[1])
	if err != nil {
		return Claims{}, ErrUnauthorized
	}
	var c Claims
	if err := json.Unmarshal(payload, &c); err != nil {
		return Claims{}, ErrUnauthorized
	}
	if c.Exp != 0 && time.Now().Unix() >= c.Exp {
		return Claims{}, ErrExpired
	}
	return c, nil
}

var RoleRank = map[string]int{"viewer": 1, "admin": 2, "superadmin": 3}

func RequireRole(role, min string) bool {
	return RoleRank[role] >= RoleRank[min]
}
