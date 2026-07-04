package githubwebhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestVerifySignature(t *testing.T) {
	body := []byte(`{"zen":"keep it logically awesome"}`)
	mac := hmac.New(sha256.New, []byte("secret"))
	_, _ = mac.Write(body)
	signature := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	if !VerifySignature("secret", body, signature) {
		t.Fatal("expected signature to verify")
	}
	if VerifySignature("secret", body, "sha256=deadbeef") {
		t.Fatal("expected invalid signature to fail")
	}
}
