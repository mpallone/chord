// Package chord contains the functions and data structures to implement the protocol
package chord

import (
	"crypto/sha1"
)

func GetNodeID(ipAndPort string) [20]byte {
	data := []byte(ipAndPort)
	return sha1.Sum(data)
}
