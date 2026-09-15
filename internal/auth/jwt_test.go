package auth

import (
	"testing"
	"time"
)

func TestSignVerify(t *testing.T) {
	secret := []byte("secret-para-tests")
	tok, err := Sign(secret, Claims{Username: "admin", Role: "superadmin", Exp: time.Now().Add(time.Hour).Unix()})
	if err != nil || tok == "" {
		t.Fatalf("tok=%q err=%v", tok, err)
	}
	c, err := Verify(secret, tok)
	if err != nil || c.Username != "admin" || c.Role != "superadmin" {
		t.Fatalf("c=%+v err=%v", c, err)
	}
}

func TestVerifyErrors(t *testing.T) {
	secret := []byte("s")
	tok, _ := Sign(secret, Claims{Username: "a", Role: "viewer", Exp: time.Now().Add(time.Hour).Unix()})
	if _, err := Verify([]byte("otro-secret"), tok); err == nil {
		t.Fatal("esperaba error por firma invalida")
	}
	bad, _ := Sign(secret, Claims{Username: "a", Role: "viewer", Exp: time.Now().Add(-time.Hour).Unix()})
	if _, err := Verify(secret, bad); err != ErrExpired {
		t.Fatalf("esperaba ErrExpired, got %v", err)
	}
	if _, err := Verify(secret, "no-es-un-jwt"); err != ErrUnauthorized {
		t.Fatalf("esperaba ErrUnauthorized, got %v", err)
	}
	if _, err := Verify(secret, tok+".extra"); err != ErrUnauthorized {
		t.Fatalf("esperaba ErrUnauthorized (3 partes), got %v", err)
	}
}

func TestRoleRank(t *testing.T) {
	if !RequireRole("superadmin", "admin") {
		t.Fatal("superadmin deberia pasar admin")
	}
	if RequireRole("viewer", "admin") {
		t.Fatal("viewer no deberia pasar admin")
	}
	if !RequireRole("admin", "admin") {
		t.Fatal("admin deberia pasar admin")
	}
	if !RequireRole("admin", "viewer") {
		t.Fatal("admin deberia pasar viewer")
	}
	if RequireRole("viewer", "superadmin") {
		t.Fatal("viewer no deberia pasar superadmin")
	}
}
