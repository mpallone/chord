// Package chord contains the functions and data structures to implement the protocol
package chord

import (
	"crypto/sha1"
	"fmt"
	"math/big"
	"net/rpc"
	"net/rpc/jsonrpc"
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

var FingerTable [mBits + 1]ChordNodePtr

type FindSuccessorReply struct {
	ChordNodePtr ChordNodePtr
}

type ChordIDArgs struct {
	Id *big.Int
}

type NotifyArgs struct {
	ChordNodePtr ChordNodePtr
}

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
func AddOne(n *big.Int) *big.Int {
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
	FingerTable[SELF].IpAddress = ip
	FingerTable[SELF].Port = port
	FingerTable[SELF].ChordID = GetChordID(ip + ":" + port)

	FingerTable[1].IpAddress = ip
	FingerTable[1].Port = port
	FingerTable[1].ChordID = GetChordID(ip + ":" + port)
}

// parameters ip and port passed in is the existing node's ip address and port
func Join(existingNodeIP string, existingNodePort string, myIp string, myPort string) error {
	fmt.Println("Joining chord ring...")
	fmt.Println("Making RPC call to: ", existingNodeIP, ":", existingNodePort)

	// Init the pointer to ourself
	FingerTable[SELF].IpAddress = myIp
	FingerTable[SELF].Port = myPort
	FingerTable[SELF].ChordID = GetChordID(myIp + ":" + myPort)

	// Dial the node
	client, err := DialNode(existingNodeIP,existingNodePort)
	defer client.Close()
	if err != nil {
		fmt.Println("ERROR: Join() could not connect to: ", existingNodeIP, ":", existingNodePort, "; error:", err)
		return err
	}

	var findSuccessorReply FindSuccessorReply
	var args ChordIDArgs
	args.Id = FingerTable[SELF].ChordID
	err = client.Call("Node.FindSuccessor", &args, &findSuccessorReply)
	if err != nil {
		fmt.Println("ERROR: Join() received an error when calling the Node.FindSuccessor RPC: ", err)
		fmt.Println("address: ", existingNodeIP, ":", existingNodePort)
		return err
	}

	// Set our fingers to point to the successor.
	// todo - I think it's actually better to just copy the successors finger table,
	//        but I don't feel like implementing that right now, and stabilize() and
	//        fix_fingers() should result in correct finger tables eventually.
	FingerTable[1].IpAddress = findSuccessorReply.ChordNodePtr.IpAddress
	FingerTable[1].Port = findSuccessorReply.ChordNodePtr.Port
	FingerTable[1].ChordID = findSuccessorReply.ChordNodePtr.ChordID

	fmt.Println("Finger table at the end of Join():", FingerTable)

	return nil
}

func FindSuccessor(id *big.Int) (ChordNodePtr, error) {

	fmt.Println("finding successor of: ", id)

	if Inclusive_in(id, AddOne(FingerTable[SELF].ChordID), FingerTable[1].ChordID) {
		return FingerTable[1], nil
	}

	closestPrecedingFinger := closestPrecedingNode(id)

	// TODO If *I* am the closest preceding node at this point, that means the initial Inclusive_in check
	// at the top of this function didn't work, and also that our finger table isn't yet correct. So,
	// ask our successor to find the node for us in this case.
	if closestPrecedingFinger.ChordID == FingerTable[0].ChordID {
		fmt.Println("FINDSUCCESSOR!! THIS SHOULD NEVER HAPPEN!!! DELETE ME??")
		//closestPrecedingFinger = FingerTable[1]
	}

	// Dial the node
	client, err := DialNode(closestPrecedingFinger.IpAddress,closestPrecedingFinger.Port)
	defer client.Close()
	if err != nil {
		fmt.Println("ERROR: FindSuccessor() could not connect to closest preceding node: ", err)
		return ChordNodePtr{}, err
	}

	var findSuccessorReply FindSuccessorReply
	var args ChordIDArgs
	args.Id = id
	err = client.Call("Node.FindSuccessor", &args, &findSuccessorReply)
	if err != nil {
		fmt.Println("ERROR: FindSuccessor() received an error when calling the Node.FindSuccessor RPC: ", err)
		return ChordNodePtr{}, err
	}

	return findSuccessorReply.ChordNodePtr, nil
}

func closestPrecedingNode(id *big.Int) ChordNodePtr {

	for i := mBits; i >= 1; i-- {

		if FingerTable[i].ChordID != nil {
			myId := FingerTable[SELF].ChordID
			currentFingerId := FingerTable[i].ChordID

			if Inclusive_in(currentFingerId, AddOne(myId), subOne(id)) {
				return FingerTable[i]
			}
		}
	}
	return FingerTable[SELF]
}

// nodePtr thinks it might be our successor
func Notify(nodePtr ChordNodePtr) {
	// Need to be careful not to dereference Predecessor, if it's a null pointer.
	if Predecessor.ChordID == nil {
		Predecessor = nodePtr
	} else if Inclusive_in(nodePtr.ChordID, AddOne(Predecessor.ChordID), subOne(FingerTable[SELF].ChordID)) {
		Predecessor = nodePtr
	}
}

// Called periodically. Verifies immediate successor, and tells
// (potentially new) successor about ourself.
func Stabilize() {

	// todo - this, and other methods, should probably be using RWLock.
	duration, _ := time.ParseDuration("3s")
	for {
		time.Sleep(duration)

		fmt.Println("top of Stabilize() loop")

    	service := FingerTable[1].IpAddress + ":" + FingerTable[1].Port
    
    	client1, err := jsonrpc.Dial("tcp", service)
    	defer client1.Close()
    	if err != nil {
    		fmt.Println("ERROR: Stabilize() could not connect to successor node: ", err)
    	}
    
    	var getPredecessorReply GetPredecessorReply
    	var args interface{}
    	err = client1.Call("Node.GetPredecessor", &args, &getPredecessorReply)
    	if err != nil {
    		fmt.Println("ERROR: Stabilize() received an error when calling the Node.GetPredecessor RPC: ", err)
    		return
    	}
    
    	successorsPredecessor := getPredecessorReply.Predecessor
    
    	if successorsPredecessor.ChordID != nil {
    		if Inclusive_in(successorsPredecessor.ChordID, AddOne(FingerTable[SELF].ChordID), subOne(FingerTable[1].ChordID)) {
    			FingerTable[1] = successorsPredecessor
    		}
    	}
    
    	service = FingerTable[1].IpAddress + ":" + FingerTable[1].Port
    	client2, err := jsonrpc.Dial("tcp", service)
    	defer client2.Close()
    	if err != nil {
    		fmt.Println("ERROR: Stabilize() could not connect to successor node: ", err)
    		return
    	}
    
    	var notifyArgs NotifyArgs
    	notifyArgs.ChordNodePtr = FingerTable[SELF]
    	var reply interface{}
    	err = client2.Call("Node.Notify", &notifyArgs, &reply)
    
    	if err != nil {
    		fmt.Println("ERROR: Stabilize() received an error when calling the Node.Notify RPC: ", err)
    		// return
    	}

		fmt.Println("Stabilize(), predecess:", Predecessor)
		fmt.Println("Stabilize(), myself   :", FingerTable[0])
		fmt.Println("Stabilize(), successor:", FingerTable[1])
	}
}

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
		exponent := big.NewInt(int64(next - 1))
		lookupKey := new(big.Int).Add(FingerTable[SELF].ChordID, new(big.Int).Exp(base, exponent, nil))
		lookupKey = new(big.Int).Mod(lookupKey, new(big.Int).Exp(base, big.NewInt(int64(mBits)), nil))

		fmt.Println("\nFixFingers() is looking up:", lookupKey, "for next =", next)
		successor, err := FindSuccessor(lookupKey)
		if err != nil {
			return
		}
		fmt.Println("result:", successor)

		FingerTable[next] = successor

		fmt.Println("\nFixFingers():", FingerTable)
	}
}

// Dial a node and create a new client
func DialNode(NodeIP string, NodePort string) (*rpc.Client, error){
	
	service := NodeIP + ":" + NodePort          //create service
	client := new(rpc.Client)                   //get a pointer to an instance of "rpc.Client"
	client, err := jsonrpc.Dial("tcp", service) //dial the node
	
	if err != nil {
		return client, err
	}
	return client, nil
}
