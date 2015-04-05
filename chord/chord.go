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


// Implements the set membership test used by 
// 
//     find_successor() 
//     closest_preceding_node() 
//     stabilize() 
//     notify() 
// 
// Returns true if searchKey is in the set [startKey, stopKey] (note the 
// use of square brackets, because this is includes the boundaries), and
// false otherwise. 
// 
// The boundaries startKey and stopKey are considered part of the set. 
// stopKey may be less than startKey, in which case this function will 
// assume that we've "looped" over the top of the ring. 
// 
// todo - I made this capitalized for testing, but it should probably just 
// be an internal method 
func Inclusive_in(searchKey *big.Int, startKey *big.Int, stopKey *big.Int) bool {

    greaterThanOrEqualToStartKey := searchKey.Cmp(startKey) >= 0 
    lessThanOrEqualToStopKey := searchKey.Cmp(stopKey) <= 0 

    if startKey.Cmp(stopKey) <= 0 {
        // startKey is <= stopKey 
        return greaterThanOrEqualToStartKey && lessThanOrEqualToStopKey
    }

    // we've looped over the "top" of the ring, where we go from 
    // 2^(m-1) to 0. 
    max_key := ComputeMaxKey() 
    
    lessThanOrEqualToMaxKey := searchKey.Cmp(max_key) <= 0 
    greaterThanOrEqualToZero := searchKey.Cmp(big.NewInt(0)) >= 0 

    return (greaterThanOrEqualToStartKey && lessThanOrEqualToMaxKey) || (greaterThanOrEqualToZero && lessThanOrEqualToStopKey) 
}

// Uses mBits to compute and return 2**mbits - 1 
func ComputeMaxKey() *big.Int {
    base := big.NewInt(2)
    m := big.NewInt(int64(mBits))

    max_key := big.NewInt(0) 
    max_key.Exp(base, m, nil)
    max_key.Sub(max_key, big.NewInt(1))

    return max_key 
}

func GetChordID(str string) *big.Int {
    data := []byte(str)

    // convert to SHA-1 hash, a byte array of size 20
    sha1hash := sha1.Sum(data)
    //fmt.Printf("SHA-1 hash: %x\n", sha1hash)

    // use only last 1 byte (8 bits) (todo, don't forget to put this back to 160 or whatever)
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

    // FingerTable[1].IpAddress = ip
    // FingerTable[1].Port = port
    // FingerTable[1].ChordID = GetChordID(ip + ":" + port)

    for i := mBits - 1; i >= 1; i-- {
        FingerTable[i].IpAddress = ip 
        FingerTable[i].Port = port 
        FingerTable[i].ChordID = GetChordID(ip + ":" + port)
    }
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

func closestPrecedingNode(id *big.Int) ChordNodePtr {
    for i := mBits - 1; i >= 1; i-- {
        myId := FingerTable[1].ChordID 
        currentFingerId := FingerTable[i].ChordID 

        if Inclusive_in(currentFingerId, myId, id) {
            return FingerTable[i]
        }
    }
    return FingerTable[1]
}