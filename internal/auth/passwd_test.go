package auth

import "testing"

func TestHashAndVerify(t *testing.T) {
	h, err := HashPassword("s3cret")
	if err != nil {
		t.Fatal(err)
	}
	if !VerifyPassword(h, "s3cret") {
		t.Fatal("password correcta deberia verificar")
	}
	if VerifyPassword(h, "incorrecta") {
		t.Fatal("password incorrecta no deberia verificar")
	}
	if VerifyPassword("no-valido", "x") {
		t.Fatal("hash corrupto no deberia verificar")
	}
	h1, _ := HashPassword("s3cret")
	h2, _ := HashPassword("s3cret")
	if h1 == h2 {
		t.Fatal("dos hashes de la misma password no deberian coincidir (salt aleatorio)")
	}
}
