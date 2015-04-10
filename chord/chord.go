// Package chord contains the functions and data structures to implement the protocol
package chord

import (
	"crypto/sha1"
	"fmt"
	"math/big"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"time"
)

type ChordNodePtr struct {
	ChordID   *big.Int
	IpAddress string
	Port      string
}

const mBits int = 8
const SELF int = 0 

var Predecessor ChordNodePtr

var FingerTable [mBits+1]ChordNodePtr


// todo - this is a duplicate definition as what's in node.go! 
type FindSuccessorReply struct {
	ChordNodePtr ChordNodePtr
}
// todo - this is a duplicate definition as what's in node.go! 
type ChordIDArgs struct {
	Id *big.Int
}
// todo - this is a duplicate definition as what's in node.go! 
type NotifyArgs struct {
	ChordNodePtr ChordNodePtr
}
// todo - this is a duplicate definition as what's in node.go! 
type GetPredecessorReply struct {
	Predecessor ChordNodePtr
}

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

// Add one to n, and wrap around if need be. 
// This is mostly to avoid the ugly big.Int syntax.
func addOne(n *big.Int) *big.Int {
	result := big.NewInt(0)
	result = result.Add(n, big.NewInt(1))
	max_val := ComputeMaxKey()
	if result.Cmp(max_val) > 0 {
		return big.NewInt(0)
	}
	return result 
}

// Subtract one from n, and wrap around if need be. 
// This is mostly to avoid the ugly big.Int syntax.
func subOne(n *big.Int) *big.Int {
	result := big.NewInt(0)
 	result = result.Sub(n, big.NewInt(1))
 	if result.Cmp(big.NewInt(0)) < 0 {
 		return ComputeMaxKey() 
 	}
 	return result 
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

	// Include 0 - let the 0th element point to ourself. 
	for i := mBits; i >= 0; i-- {
		FingerTable[i].IpAddress = ip 
		FingerTable[i].Port = port 
		FingerTable[i].ChordID = GetChordID(ip + ":" + port)
	}
}

// parameters ip and port passed in is the existing node's ip address and port
func Join(existingNodeIP string, existingNodePort string, myIp string, myPort string) {
	fmt.Println("Joining chord ring...")
	fmt.Println("Making RPC call to: ", existingNodeIP, ":", existingNodePort)

	// Init the pointer to ourself 
	FingerTable[SELF].IpAddress = myIp
	FingerTable[SELF].Port = myPort
	FingerTable[SELF].ChordID = GetChordID(myIp + ":" + myPort)


	// make RPC call to existing node already in chord ring (use Client.Call) 
	service := existingNodeIP + ":" + existingNodePort
	var client *rpc.Client 
	client, err := jsonrpc.Dial("tcp", service)
	if err != nil {
		fmt.Println("ERROR: Join() could not connect to: ", existingNodeIP, ":", existingNodePort, "; error:", err)
		// todo - not sure what to do if a node fails.
		// Exiting is nice if the node fails, to avoid subtle bugs, 
		// but the system should be more resilient than that. 
        os.Exit(1)
	} 

    var findSuccessorReply FindSuccessorReply
    var args ChordIDArgs
    args.Id = FingerTable[SELF].ChordID 
    err = client.Call("Node.FindSuccessor", &args, &findSuccessorReply) // Should I be closing this? todo 
   	if err != nil {
		fmt.Println("ERROR: Join() received an error when calling the Node.FindSuccessor RPC: ", err)
		fmt.Println("address: ", existingNodeIP, ":", existingNodePort)
		// todo - Again, probably shouldn't be exiting here. 
		os.Exit(1)
	}
	client.Close() 

	// Set our fingers to point to the successor. 
	// todo - I think it's actually better to just copy the successors finger table, 
	//        but I don't feel like implementing that right now, and stabilize() and 
	//        fix_fingers() should result in correct finger tables eventually. 
	for i := mBits; i >= 1; i-- {
		FingerTable[i].IpAddress = findSuccessorReply.ChordNodePtr.IpAddress
		FingerTable[i].Port = findSuccessorReply.ChordNodePtr.Port 
		FingerTable[i].ChordID = findSuccessorReply.ChordNodePtr.ChordID 
	}

	fmt.Println("Finger table at the end of Join():", FingerTable)

}

func FindSuccessor(id *big.Int) ChordNodePtr {

	fmt.Println("finding successor of: ", id)

	if Inclusive_in(id, addOne(FingerTable[SELF].ChordID), FingerTable[1].ChordID) {
		return FingerTable[1]
	}

	closestPrecedingFinger := closestPrecedingNode(id)


	// If *I* am the closest preceding node at this point, that means the initial Inclusive_in check
	// at the top of this function didn't work, and also that our finger table isn't yet correct. So, 
	// ask our successor to find the node for us in this case. 
	if closestPrecedingFinger.ChordID == FingerTable[0].ChordID {
		closestPrecedingFinger = FingerTable[1]
	}

	service := closestPrecedingFinger.IpAddress + ":" + closestPrecedingFinger.Port
	var client *rpc.Client

	client, err := jsonrpc.Dial("tcp", service)
	if err != nil {
		fmt.Println("ERROR: FindSuccessor() could not connect to closest preceding node: ", err)
		// todo - not sure what to do if a node fails.
		// Exiting is nice if the node fails, to avoid subtle bugs, 
		// but the system should be more resilient than that. 
        os.Exit(1)
	} 




    var findSuccessorReply FindSuccessorReply
    var args ChordIDArgs
    args.Id = id 
	err = client.Call("Node.FindSuccessor", &args, &findSuccessorReply) // Should I be closing this? todo 
	if err != nil {
		fmt.Println("ERROR: FindSuccessor() received an error when calling the Node.FindSuccessor RPC: ", err)
		// todo - Again, probably shouldn't be exiting here. 
		os.Exit(1)
	}
	client.Close()

	return findSuccessorReply.ChordNodePtr
}

func closestPrecedingNode(id *big.Int) ChordNodePtr {

	for i := mBits; i >= 1; i-- {

		myId := FingerTable[1].ChordID 
		currentFingerId := FingerTable[i].ChordID 

		if Inclusive_in(currentFingerId, addOne(myId), subOne(id)) {
			return FingerTable[i]
		}
	}
	return FingerTable[SELF]
}

// nodePtr thinks it might be our successor
func Notify(nodePtr ChordNodePtr) {
	// Need to be careful not to dereference Predecessor, if it's a null pointer. 
	if Predecessor.ChordID == nil {
		Predecessor = nodePtr 
	} else if Inclusive_in(nodePtr.ChordID, addOne(Predecessor.ChordID), subOne(FingerTable[SELF].ChordID)) {
		Predecessor = nodePtr 
	}
}

// Called periodically. Verifies immediate successor, and tells 
// (potentially new) successor about ourself. 
func Stabilize() {

	service := FingerTable[1].IpAddress + ":" + FingerTable[1].Port
	var client *rpc.Client

	client, err := jsonrpc.Dial("tcp", service)
	if err != nil {
		fmt.Println("ERROR: Stabilize() could not connect to successor node: ", err)
	} 

    var getPredecessorReply GetPredecessorReply
    var args interface{}
	err = client.Call("Node.GetPredecessor", &args, &getPredecessorReply) // Should I be closing this? todo 
	if err != nil {
		fmt.Println("ERROR: Stabilize() received an error when calling the Node.GetPredecessor RPC: ", err)
		return 
	}
	client.Close() 

	successorsPredecessor := getPredecessorReply.Predecessor 

	if successorsPredecessor.ChordID != nil {
    	if Inclusive_in(successorsPredecessor.ChordID, addOne(FingerTable[SELF].ChordID), subOne(FingerTable[1].ChordID)) {
		    FingerTable[1] = successorsPredecessor
	    }
	}

	service = FingerTable[1].IpAddress + ":" + FingerTable[1].Port
	client, err = jsonrpc.Dial("tcp", service)
	if err != nil {
		fmt.Println("ERROR: Stabilize() could not connect to successor node: ", err)
		return 
	} 

	var notifyArgs NotifyArgs
	notifyArgs.ChordNodePtr = FingerTable[0]
	var reply interface{}
	err = client.Call("Node.Notify", &notifyArgs, &reply) // should I be closing this? todo 
	defer client.Close() 

	if err != nil {
		fmt.Println("ERROR: Stabilize() received an error when calling the Node.Notify RPC: ", err)
		return 
	}
	
}

// todo - should FixFingers() and Stablize() be called consistently? I'm doing them kind of wonky here 
func FixFingers() {
	// todo - this, and other methods, should probably be using RWLock. 
	duration, _ := time.ParseDuration("2s")
	next := 0
	for {
		time.Sleep(duration)
		next += 1 
		if next > mBits {
			next = 1 
		}

		base := big.NewInt(2)
		exponent := big.NewInt(int64(next-1))
		lookupKey := new(big.Int).Add(FingerTable[SELF].ChordID, new(big.Int).Exp(base, exponent, nil))
		lookupKey = new(big.Int).Mod(lookupKey, new(big.Int).Exp(base, big.NewInt(int64(mBits)), nil))

		fmt.Println("\nFixFingers() is looking up:", lookupKey, "for next =", next)
		successor := FindSuccessor(lookupKey)
		fmt.Println("result:", successor)

		FingerTable[next] = successor 

		fmt.Println("\nFixFingers():", FingerTable)
	}
}
