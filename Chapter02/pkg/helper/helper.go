package helper

import (
	"crypto"
	"encoding/hex"
)

func HashRequest(request string) string {
	hash := crypto.MD5.New()
	hash.Write([]byte(request))
	res := hash.Sum(nil)
	return hex.EncodeToString(res)
}

func ExtractItemId(request string) {

}
