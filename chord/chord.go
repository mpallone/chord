// Package chord contains the functions and data structures to implement the protocol
package chord

import (
	"crypto/sha1"
	"fmt"
	"math/big"
)

type ChordNodePtr struct {
	ChordID   *big.Int
	IpAddress string
	Port      string
}

const mBits int = 8

var Predecessor ChordNodePtr

var FingerTable [mBits]ChordNodePtr

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

// parameters ip and port passed in is the existing node's ip address and port
func Join(existingNodeIP string, existingNodePort string, joiningNodeID *big.Int) {
	fmt.Println("Joining chord ring...")
	fmt.Println("Making RPC call to: ", existingNodeIP, ":", existingNodePort)

	// make RPC call to existing node already in chord ring (use Client.Call)

	// joining node updates its fingerTable[1] to have FingerTable[1] = successor
}

func FindSuccessor(id *big.Int) ChordNodePtr {

	fmt.Println("finding successor of: ", id)

	// TODO add logic for finger table lookup

	// placeholder for the successor once it is found
	var temp ChordNodePtr
	return temp
}
