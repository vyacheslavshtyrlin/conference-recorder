package worker

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

type controlJWTConfig struct {
	issuer        string
	audience      string
	secret        []byte
	maxTTLSeconds int64
}

func (config controlJWTConfig) verify(header string, now time.Time) error {
	if !strings.HasPrefix(header, "Bearer ") {
		return errors.New("invalid authorization")
	}
	parts := strings.Split(strings.TrimPrefix(header, "Bearer "), ".")
	if len(parts) != 3 {
		return errors.New("invalid authorization")
	}
	signed := parts[0] + "." + parts[1]
	expected := hmac.New(sha256.New, config.secret)
	_, _ = expected.Write([]byte(signed))
	actual, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || !hmac.Equal(actual, expected.Sum(nil)) {
		return errors.New("invalid authorization")
	}

	var tokenHeader struct {
		Algorithm string `json:"alg"`
		Type      string `json:"typ"`
	}
	var claims struct {
		Issuer   string `json:"iss"`
		Audience string `json:"aud"`
		Subject  string `json:"sub"`
		IssuedAt int64  `json:"iat"`
		Expires  int64  `json:"exp"`
	}
	if decodeTokenPart(parts[0], &tokenHeader) != nil ||
		decodeTokenPart(parts[1], &claims) != nil {
		return errors.New("invalid authorization")
	}
	if tokenHeader.Algorithm != "HS256" || tokenHeader.Type != "recording-bot-control+jwt" {
		return errors.New("invalid authorization")
	}
	identityMatches := claims.Issuer == config.issuer &&
		claims.Audience == config.audience &&
		claims.Subject == "conference-signaling"
	current := now.Unix()
	timeValid := claims.IssuedAt <= current+5 &&
		claims.Expires > current &&
		claims.Expires > claims.IssuedAt &&
		claims.Expires-claims.IssuedAt <= config.maxTTLSeconds
	if !identityMatches || !timeValid {
		return errors.New("invalid authorization")
	}
	return nil
}

func decodeTokenPart(encoded string, destination any) error {
	data, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, destination)
}
