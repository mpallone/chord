// Package chord contains the functions and data structures to implement the protocol
package chord

import (
	"crypto/sha1"
	"errors"
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

const mBits int = 8
const SELF int = 0

type Chord struct {
	Predecessor       ChordNodePtr
	FingerTable       [mBits + 1]ChordNodePtr
	StabilizeDuration time.Duration
	connections       map[string]*rpc.Client
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

func gimmeAChord() *Chord {
	t := new(Chord)
	t.connections = make(map[string]*rpc.Client)
	return t
}
func Create(ip string, port string) *Chord {
	t := gimmeAChord()
	fmt.Println("Creating chord ring and initializing finger table...")

	// first entry in finger table is set to itself
	// first node is its own successor since no other nodes yet in the ring
	// Init the pointer to ourself. Assume we own all pointers until we find otherwise. (fixes infinite loop between nodes)
	for i := 0; i < len(t.FingerTable); i++ {
		t.FingerTable[i].IpAddress = ip
		t.FingerTable[i].Port = port
		t.FingerTable[i].ChordID = GetChordID(ip + ":" + port)
	}
	return t
}

// parameters ip and port passed in is the existing node's ip address and port
func Join(existingNodeIP string, existingNodePort string, myIp string, myPort string) *Chord {
	t := gimmeAChord()

	fmt.Println("Joining chord ring...")
	fmt.Println("Making RPC call to: ", existingNodeIP, ":", existingNodePort)

	// Init the pointer to ourself. Assume we own all pointers until we find otherwise. (fixes infinite loop between nodes)
	for i := 0; i < len(t.FingerTable); i++ {
		t.FingerTable[i].IpAddress = myIp
		t.FingerTable[i].Port = myPort
		t.FingerTable[i].ChordID = GetChordID(myIp + ":" + myPort)
	}

	var findSuccessorReply FindSuccessorReply
	var args ChordIDArgs
	args.Id = t.FingerTable[SELF].ChordID

	var chordNodePtrToExistingNode ChordNodePtr
	chordNodePtrToExistingNode.IpAddress = existingNodeIP
	chordNodePtrToExistingNode.Port = existingNodePort
	chordNodePtrToExistingNode.ChordID = GetChordID(existingNodeIP + ":" + existingNodePort)

	for t.CallRPC("Node.FindSuccessor", &args, &findSuccessorReply, &chordNodePtrToExistingNode) != nil {
		fmt.Println("FindSuccessor() call in Join failed, trying again after a short Delay...")
		time.Sleep(3 * t.StabilizeDuration)
	}

	// Set our fingers to point to the successor.
	// todo - I think it's actually better to just copy the successors finger table,
	//        but I don't feel like implementing that right now, and stabilize() and
	//        fix_fingers() should result in correct finger tables eventually.
	t.FingerTable[1].IpAddress = findSuccessorReply.ChordNodePtr.IpAddress
	t.FingerTable[1].Port = findSuccessorReply.ChordNodePtr.Port
	t.FingerTable[1].ChordID = findSuccessorReply.ChordNodePtr.ChordID

	fmt.Println("Finger table at the end of Join():", t.FingerTable)
	return t
}

// This is a generic way to call an RPC that hides the details of maintaining
// the persistent connections (or dealing with old connections to failed nodes.)
//
// Similar to calling an RPC, this function returns an error, and populates the
// reply pointer with whatever the RPC returns.
//
// rpcString: something like "node.FindSuccessor"
// args: the argument struct pointer, just as would be passed to the RPC
// reply: the reply struct pointer, just as would be passed to the RPC. This
//        function will populate this value with whatever the RPC returns.
// chordNodePtr: the node to contact
//
func (t *Chord) CallRPC(rpcString string, args interface{}, reply interface{}, chordNodePtr *ChordNodePtr) error {

	fmt.Println("-------------------------------------------------------")
	fmt.Println("CallRPC() has been called with the following arguments:")
	fmt.Println("rpcString:", rpcString)
	fmt.Println("args:", args)
	fmt.Println("reply:", reply)
	fmt.Println("chordNodePtr:", chordNodePtr)

	// Just to test that my function signature syntax is correct:
	service := chordNodePtr.IpAddress + ":" + chordNodePtr.Port
	var client *rpc.Client
	var err error
	callFailed := false

	client = t.connections[service]
	if client != nil {
		fmt.Println("client isn't nil, attempting to Call it")
		err = client.Call(rpcString, args, reply)
		if err != nil {
			fmt.Println("CallRPC() tried to call an existing client, but failed. Attempting to reestablish connection in order to call:", rpcString)
			fmt.Println("error received was:", err)
			callFailed = true
		} else {
			return nil
		}
	}

	if client == nil || callFailed {

		fmt.Println("client is nil or the original call failed, attempting to establish a new connection")

		client, err = jsonrpc.Dial("tcp", service)
		if err != nil {
			fmt.Println("CallRPC ERROR;", rpcString, "failed to connect to", chordNodePtr, "with error", err)
			return err
		}

		// Only maintain a persistent connection if the node we're contacting is
		// in our finger table, or if it's our predecessor.

		// todo - if we implement 'r' predecessors and successors, this code might need
		// to be updated to maintain persistent connections to them, too.
		if t.isFingerOrPredecessor(chordNodePtr) || t.aFingerOrPredecessorIsNil() {
			t.connections[service] = client
		} else {
			defer client.Close()
		}
	}

	err = client.Call(rpcString, args, reply)
	if err != nil {
		fmt.Println("CallRPC ERROR;", rpcString, "received an error when calling the", rpcString, "RPC:", err)
		return err
	}

	fmt.Println("CallRPC() has populated the reply with:", reply)
	fmt.Println("------------------------------------------------------")

	return nil
}

// Helper method for CallRPC(), so we can easily tell if a given
// ChordNodePtr is one we should maintain a persistent connection
// with.
func (t *Chord) isFingerOrPredecessor(chordNodePtr *ChordNodePtr) bool {
	if ChordNodePtrsAreEqual(&t.Predecessor, chordNodePtr) {
		return true
	}

	for i := mBits; i >= 1; i-- {

		if t.FingerTable[i].ChordID != nil {
			if ChordNodePtrsAreEqual(&t.FingerTable[i], chordNodePtr) {
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
func (t *Chord) aFingerOrPredecessorIsNil() bool {
	if &t.Predecessor == nil {
		return true
	}

	for i := mBits; i >= 1; i-- {

		if t.FingerTable[i].ChordID != nil {
			if &t.FingerTable[i] == nil {
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

func (t *Chord) FindSuccessor(id *big.Int) (ChordNodePtr, error) {

	fmt.Println("finding successor of: ", id)

	if id == nil {
		fmt.Println("ERROR: FindSuccessor was called with a <nil> id.") // todo remove duplicate string
		return ChordNodePtr{}, errors.New("FindSuccessor was called with a <nil> id.")
	}

	if Inclusive_in(id, addOne(t.FingerTable[SELF].ChordID), t.FingerTable[1].ChordID) {
		return t.FingerTable[1], nil
	}

	closestPrecedingFinger := t.closestPrecedingNode(id)

	// TODO If *I* am the closest preceding node at this point, that means the initial Inclusive_in check
	// at the top of this function didn't work, and also that our finger table isn't yet correct. So,
	// ask our successor to find the node for us in this case.
	if closestPrecedingFinger.ChordID == t.FingerTable[0].ChordID {
		fmt.Println("FINDSUCCESSOR!! THIS SHOULD NEVER HAPPEN!!! DELETE ME??")
		//closestPrecedingFinger = t.FingerTable[1]
	}

	fmt.Println("FindSuccessor() chose the following for closestPrecedingFinger:", closestPrecedingFinger)

	var findSuccessorReply FindSuccessorReply
	var args ChordIDArgs
	args.Id = id

	err := t.CallRPC("Node.FindSuccessor", &args, &findSuccessorReply, &closestPrecedingFinger)
	if err != nil {
		fmt.Println("CallRPC() returned the following error:", err)
	}

	return findSuccessorReply.ChordNodePtr, nil
}

func (t *Chord) closestPrecedingNode(id *big.Int) ChordNodePtr {

	for i := mBits; i >= 1; i-- {

		if t.FingerTable[i].ChordID != nil {
			myId := t.FingerTable[SELF].ChordID
			currentFingerId := t.FingerTable[i].ChordID

			if Inclusive_in(currentFingerId, addOne(myId), subOne(id)) {
				return t.FingerTable[i]
			}
		}
	}
	return t.FingerTable[SELF]
}

// nodePtr thinks it might be our successor
func (t *Chord) Notify(nodePtr ChordNodePtr) {
	// Need to be careful not to dereference Predecessor, if it's a null pointer.
	if t.Predecessor.ChordID == nil {
		t.Predecessor = nodePtr
	} else if Inclusive_in(nodePtr.ChordID, addOne(t.Predecessor.ChordID), subOne(t.FingerTable[SELF].ChordID)) {
		t.Predecessor = nodePtr
	}
}

// Called periodically. Verifies immediate successor, and tells
// (potentially new) successor about ourself.
func (t *Chord) Stabilize() {

	var getPredecessorReply GetPredecessorReply
	var args interface{}

	t.CallRPC("Node.GetPredecessor", &args, &getPredecessorReply, &t.FingerTable[1])

	successorsPredecessor := getPredecessorReply.Predecessor

	if successorsPredecessor.ChordID != nil {
		if Inclusive_in(successorsPredecessor.ChordID, addOne(t.FingerTable[SELF].ChordID), subOne(t.FingerTable[1].ChordID)) {
			t.FingerTable[1] = successorsPredecessor
		}
	}

	var notifyArgs NotifyArgs
	notifyArgs.ChordNodePtr = t.FingerTable[SELF]
	var reply NotifyReply

	t.CallRPC("Node.Notify", &notifyArgs, &reply, &t.FingerTable[1])
}

// todo - should FixFingers() and Stablize() be called consistently? I'm doing them kind of wonky here
func (t *Chord) FixFingers() {
	// todo - this, and other methods, should probably be using RWLock.
	next := 0
	for {
		time.Sleep(t.StabilizeDuration)
		next += 1
		if next > mBits {
			next = 1
		}

		base := big.NewInt(2)
		exponent := big.NewInt(int64(next - 1))
		lookupKey := new(big.Int).Add(t.FingerTable[SELF].ChordID, new(big.Int).Exp(base, exponent, nil))
		lookupKey = new(big.Int).Mod(lookupKey, new(big.Int).Exp(base, big.NewInt(int64(mBits)), nil))

		fmt.Println("\nFixFingers() is looking up:", lookupKey, "for next =", next)
		successor, err := t.FindSuccessor(lookupKey)
		if err != nil {
			return
		}
		fmt.Println("result:", successor)

		t.FingerTable[next] = successor

		fmt.Println("\nFixFingers():", t.FingerTable)
	}
}
