package utils

import (
	"math/rand"
)

var alphabet = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")

func RandomString(characters int) string {
	b := make([]rune, characters)
	for i := range b {
		b[i] = alphabet[rand.Intn(len(alphabet))]
	}
	return string(b)
}

func RandomInt(from int64, to int64) int64 {
	return rand.Int63n(to-from) + from
}
