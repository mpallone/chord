// Package chord contains the functions and data structures to implement the protocol
package chord

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"time"
)

type ChordNodePtr struct {
	ChordID   *big.Int
	IpAddress string
	Port      string
}

const MBits int = 160 // This *must* be an even number for key/rel hashing purposes
const SELF int = 0

var Predecessor ChordNodePtr

var FingerTable [MBits + 1]ChordNodePtr

var connections = make(map[string]*rpc.Client)

var RunStabilizeAndFixFingers = true
var FFDone = false

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

// Notify doesn't send a reply, but I think the Go library
// chokes if an RPC has a nil reply (hence those invalid errors)
type NotifyReply struct {
	Dummy string
}

type TransferKeysArgs struct {
	ChordNodePtr ChordNodePtr
}
type DeleteTransferredKeysArgs struct {
	ChordNodePtr ChordNodePtr
}
type TransferKeysReply struct {
	TransferKeysCompleted bool
}
type DeleteTransferredKeysReply struct {
	TransferKeysDeleted bool
}
type SetPredecessorArgs struct {
	ChordNodePtr ChordNodePtr
}
type SetPredecessorReply struct {
	PredecessorSet bool
}
type SetSucessorArgs struct {
	ChordNodePtr ChordNodePtr
}
type SetSuccessorReply struct {
	SuccessorSet bool
}

func ClosePersistentConnections() {
	for key, _ := range connections {
		connections[key].Close()
	}
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

// Uses MBits to compute and return 2**Mbits - 1
func ComputeMaxKey() *big.Int {
	base := big.NewInt(2)
	m := big.NewInt(int64(MBits))

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
func SubOne(n *big.Int) *big.Int {
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

	// use only last 1 byte (8 bits)
	//var b = sha1hash[len(sha1hash)-2 : len(sha1hash)]
	//fmt.Printf("Chord ID (hex): 0x%x\n", b)

	// https://groups.google.com/forum/#!topic/golang-nuts/se5SRGw3kqQ
	// for converting byte array into integer
	var chordID = big.NewInt(0).SetBytes(sha1hash[:])
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

	var findSuccessorReply FindSuccessorReply
	var args ChordIDArgs
	args.Id = FingerTable[SELF].ChordID

	var chordNodePtrToExistingNode ChordNodePtr
	chordNodePtrToExistingNode.IpAddress = existingNodeIP
	chordNodePtrToExistingNode.Port = existingNodePort
	chordNodePtrToExistingNode.ChordID = GetChordID(existingNodeIP + ":" + existingNodePort)

	for CallRPC("Requested.FindSuccessor", &args, &findSuccessorReply, &chordNodePtrToExistingNode) != nil {
		Delay("3s")
	}

	// Set our fingers to point to the successor.
	FingerTable[1].IpAddress = findSuccessorReply.ChordNodePtr.IpAddress
	FingerTable[1].Port = findSuccessorReply.ChordNodePtr.Port
	FingerTable[1].ChordID = findSuccessorReply.ChordNodePtr.ChordID

	// call TransferKeys on the node that we just discovered is our new successor
	//   we need to tell the new successor about ourself (IP, Port, and ChordID) so it
	//   knows where/what are the approrpiate keys to insert on us
	fmt.Println("Calling TransferKeys on node (my newly discovered successor): ", FingerTable[1].ChordID)
	var transferKeysReply TransferKeysReply
	var argsXfer TransferKeysArgs
	argsXfer.ChordNodePtr.IpAddress = FingerTable[SELF].IpAddress
	argsXfer.ChordNodePtr.Port = FingerTable[SELF].Port
	argsXfer.ChordNodePtr.ChordID = FingerTable[SELF].ChordID
	err := CallRPC("Requested.TransferKeys", &argsXfer, &transferKeysReply, &FingerTable[1])
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

// This is a generic way to call an RPC that hides the details of maintaining
// the persistent connections (or dealing with old connections to failed nodes.)
//
// Similar to calling an RPC, this function returns an error, and populates the
// reply pointer with whatever the RPC returns.
//
// rpcString: something like "Requested.FindSuccessor"
// args: the argument struct pointer, just as would be passed to the RPC
// reply: the reply struct pointer, just as would be passed to the RPC. This
//        function will populate this value with whatever the RPC returns.
// chordNodePtr: the node to contact
//
func CallRPC(rpcString string, args interface{}, reply interface{}, chordNodePtr *ChordNodePtr) error {

	// Just to test that my function signature syntax is correct:
	service := chordNodePtr.IpAddress + ":" + chordNodePtr.Port
	var client *rpc.Client
	var err error
	callFailed := false

	client = connections[service]
	if client != nil {
		err = client.Call(rpcString, args, reply)
		if err != nil {
			callFailed = true
		} else {
			return nil
		}
	}

	if client == nil || callFailed {

		client, err = jsonrpc.Dial("tcp", service)
		if err != nil {
			return err
		}

		// Only maintain a persistent connection if the node we're contacting is
		// in our finger table, or if it's our predecessor.

		if isFingerOrPredecessor(chordNodePtr) || aFingerOrPredecessorIsNil() {
			connections[service] = client
		} else {
			defer client.Close()
		}
	}

	err = client.Call(rpcString, args, reply)
	if err != nil {
		return err
	}

	return nil
}

// Helper method for CallRPC(), so we can easily tell if a given
// ChordNodePtr is one we should maintain a persistent connection
// with.
func isFingerOrPredecessor(chordNodePtr *ChordNodePtr) bool {
	if ChordNodePtrsAreEqual(&Predecessor, chordNodePtr) {
		return true
	}

	for i := MBits; i >= 1; i-- {

		if FingerTable[i].ChordID != nil {
			if ChordNodePtrsAreEqual(&FingerTable[i], chordNodePtr) {
				return true
			}
		}
	}
	return false
}

// Returns True if a finger or predecessor is nil. The CallRPC()
// function uses this to hold off on closing a connection.
// FixFingers() will be responsible for cleaning out the connections
// variable.
func aFingerOrPredecessorIsNil() bool {
	if &Predecessor == nil {
		return true
	}

	for i := MBits; i >= 1; i-- {

		if FingerTable[i].ChordID != nil {
			if &FingerTable[i] == nil {
				return true
			}
		}
	}
	return false
}

// Returns True if two *ChordNodePtr's are the same. This could probably
// just use the Chord ID, but screw big.Int
func ChordNodePtrsAreEqual(ptr1 *ChordNodePtr, ptr2 *ChordNodePtr) bool {
	if ptr1.IpAddress != ptr2.IpAddress {
		return false
	}
	if ptr1.Port != ptr2.Port {
		return false
	}
	return true
}

func FindSuccessor(id *big.Int) (ChordNodePtr, error) {

	if id == nil {
		Delay("10s")
		return ChordNodePtr{}, errors.New("FindSuccessor was called with a <nil> id.")
	}

	if Inclusive_in(id, AddOne(FingerTable[SELF].ChordID), FingerTable[1].ChordID) {
		return FingerTable[1], nil
	}

	closestPrecedingFinger := closestPrecedingNode(id)

	var findSuccessorReply FindSuccessorReply
	var args ChordIDArgs
	args.Id = id

	err := CallRPC("Requested.FindSuccessor", &args, &findSuccessorReply, &closestPrecedingFinger)
	if err != nil {
		fmt.Println("CallRPC() returned the following error:", err)
	}

	return findSuccessorReply.ChordNodePtr, nil
}

func closestPrecedingNode(id *big.Int) ChordNodePtr {

	for i := MBits; i >= 1; i-- {

		if FingerTable[i].ChordID != nil {
			myId := FingerTable[SELF].ChordID
			currentFingerId := FingerTable[i].ChordID

			if Inclusive_in(currentFingerId, AddOne(myId), SubOne(id)) {
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
	} else if Inclusive_in(nodePtr.ChordID, AddOne(Predecessor.ChordID), SubOne(FingerTable[SELF].ChordID)) {
		Predecessor = nodePtr
	}
}

// Called periodically. Verifies immediate successor, and tells
// (potentially new) successor about ourself.
func Stabilize() {
	var getPredecessorReply GetPredecessorReply
	var args interface{}

	CallRPC("Requested.GetPredecessor", &args, &getPredecessorReply, &FingerTable[1])

	successorsPredecessor := getPredecessorReply.Predecessor

	if successorsPredecessor.ChordID != nil {
		if Inclusive_in(successorsPredecessor.ChordID, AddOne(FingerTable[SELF].ChordID), SubOne(FingerTable[1].ChordID)) {
			FingerTable[1] = successorsPredecessor
		}
	}

	var notifyArgs NotifyArgs
	notifyArgs.ChordNodePtr = FingerTable[SELF]
	var reply NotifyReply

	CallRPC("Requested.Notify", &notifyArgs, &reply, &FingerTable[1])
}

func FixFingers() {
	duration, _ := time.ParseDuration("0.2s")
	next := 0
	for RunStabilizeAndFixFingers {
		time.Sleep(duration)
		next += 1
		if next > MBits {
			next = 1
		}

		base := big.NewInt(2)
		exponent := big.NewInt(int64(next - 1))
		lookupKey := new(big.Int).Add(FingerTable[SELF].ChordID, new(big.Int).Exp(base, exponent, nil))
		lookupKey = new(big.Int).Mod(lookupKey, new(big.Int).Exp(base, big.NewInt(int64(MBits)), nil))

		successor, err := FindSuccessor(lookupKey)
		if err != nil {
			return
		}

		FingerTable[next] = successor
	}
	FFDone = true
}

// Mostly to slow things down for debugging.
func Delay(delayString string) {
	duration, _ := time.ParseDuration(delayString)
	time.Sleep(duration)

}

func CheckPredecessor() {
	duration, _ := time.ParseDuration("300s")
	for {
		time.Sleep(duration)

		if Predecessor.IpAddress != "" {
			service := Predecessor.IpAddress + ":" + Predecessor.Port
			seconds := 30

			//Check if predecessor has failed
			_, err := net.DialTimeout("tcp", service, time.Duration(seconds)*time.Second)
			if err != nil {
				//Set predecessor to nil
				fmt.Println("Failed to connect to predecessor", err)
				Predecessor.ChordID = big.NewInt(0)
				Predecessor.IpAddress = ""
				Predecessor.Port = ""
			}
		}
	}
}
