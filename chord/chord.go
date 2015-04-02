// Package chord contains the functions and data structures to implement the protocol
package chord

import (
	"crypto/sha1"
	"fmt"
	"math/big"
)

type ChordNode struct {
	ChordID   *big.Int
	IpAddress string
	Port      string
}

const mBits int = 8

var FingerTable [mBits]ChordNode

func GetChordID(str string) *big.Int {
	data := []byte(str)

	// convert to SHA-1 hash, a byte array of size 20
	sha1hash := sha1.Sum(data)
	//fmt.Printf("SHA-1 hash: %x\n", sha1hash)

	// use only last 1 byte (8 bits)
	var b = sha1hash[len(sha1hash)-1 : len(sha1hash)]
	//fmt.Printf("Chord ID (hex): 0x%x\n", b)

	// https://groups.google.com/forum/#!topic/golang-nuts/se5SRGw3kqQ
	// for converting byte array into integer
	var chordID = big.NewInt(0).SetBytes(b)
	//fmt.Printf("Chord ID (dec): %d\n", chordID)

	return chordID

}

func Create(ip string, port string) {
	fmt.Println("Creating chord ring and initializing finger table...")

	// first entry in finger table is set to itself
	// first node is its own successor since no other nodes yet in the ring
	FingerTable[1].IpAddress = ip
	FingerTable[1].Port = port
	FingerTable[1].ChordID = GetChordID(ip + ":" + port)
}

func Join() {
	fmt.Println("Joining chord ring...")
}
