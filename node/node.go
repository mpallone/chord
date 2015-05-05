/* CMSC-621
Project 2
node.go
*/

package main

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/robcs621/proj2/chord"
	"io/ioutil"
	"log"
	"math/big"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

// server configuration object read in
var conf ServerConfiguration
var stabilizeDone = false
var runListener = true
var listenerDone = false
var shutdownDone = false

type ServerConfiguration struct {
	ServerID                   string
	Protocol                   string
	IpAddress                  string
	Port                       string
	PersistentStorageContainer struct {
		File string
	}
	Methods []string
}

type Node int

type TripKey string
type TripRel string
type TripVal map[string]interface{}
type KeyRelPair struct {
	Key TripKey
	Rel TripRel
}

// todo - just use the Triplets struct. Args is not a good name.
type Args struct {
	Key TripKey
	Rel TripRel
	Val TripVal // only used for insert and insertOrUpdate,
}

type Triplet struct {
	Key TripKey
	Rel TripRel
	Val TripVal
}

type GetTripletsByKeyArgs struct {
	Key TripKey
}
type GetTripletsByKeyReply struct {
	TripList []Triplet
}

type SearchRingForKeyArgs struct {
	Key             TripKey
	LastNodeInQuery chord.ChordNodePtr
}
type SearchRingForKeyReply struct {
	TripList []Triplet
}

type SearchRingForRelArgs struct {
	Rel        TripRel
	StartValue *big.Int
	StopValue  *big.Int
}
type SearchRingForRelReply struct {
	TripList []Triplet
}

// To keep things uniform between regular queries and partial
// match queries, have LookupReply always return a list of
// triplets.
type LookupReply struct {
	// Key TripKey
	// Rel TripRel
	// Val TripVal
	TripList []Triplet
}
type InsertReply struct {
	TripletInserted bool
}
type ListKeysReply struct {
	KeyList []TripKey
}
type ListIDsReply struct {
	IDList []KeyRelPair
}
type DeleteReply struct {
	TripletDeleted bool
}

// global variable
var dict = map[KeyRelPair]TripVal{}
var relOnlyPartialMatchQueryCount = 0

// Hash the key and rel, concatenate the lower order bits together,
// and return the result as a *big.Int.
//
// Leading and trailing whitespace on the key and rel are ignored.
func getDict3ChordKey(tripletKey string, tripletRel string) *big.Int {
	tripletKeyHash := chord.GetChordID(strings.TrimSpace(tripletKey))
	tripletRelHash := chord.GetChordID(strings.TrimSpace(tripletRel))
	// fmt.Println(" @@@   initial tripletKeyHash:", tripletKeyHash) // todo remove
	// fmt.Println(" @@@   initial tripletRelHash:", tripletRelHash) // todo remove

	lowOrderBitMask := new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(chord.MBits/2)), nil)
	lowOrderBitMask = new(big.Int).Sub(lowOrderBitMask, big.NewInt(1))

	// fmt.Println(" @@@   getDict3ChordKey, lowOrderBitMask:", lowOrderBitMask) // todo remove

	// Extract the low order bits from each hash
	tripletKeyHash = new(big.Int).And(tripletKeyHash, lowOrderBitMask)
	tripletRelHash = new(big.Int).And(tripletRelHash, lowOrderBitMask)

	// fmt.Println(" @@@   tripletKeyHash after applying mask:", tripletKeyHash) // todo remove
	// fmt.Println(" @@@   tripletRelHash after applying mask:", tripletRelHash) // todo remove

	chordKey := new(big.Int).Lsh(tripletKeyHash, uint(chord.MBits/2))
	chordKey = new(big.Int).Or(chordKey, tripletRelHash)

	// fmt.Println(" @@@   returning chordKey:", chordKey)

	return chordKey
}

// Returns a list of triplets that have the given key.
// Only looks in the local dictionary; does not send
// queries across the network.
// todo - not sure if I actually need this method. Delete it if this RPC is
//        never actually called.
func (t *Node) GetTripletsByKey(args *GetTripletsByKeyArgs, reply *GetTripletsByKeyReply) error {

	var listOfTriplets []Triplet

	for keyRelPair, _ := range dict {
		if keyRelPair.Key == args.Key {
			var currTriplet Triplet
			currTriplet.Key = keyRelPair.Key
			currTriplet.Rel = keyRelPair.Rel
			currTriplet.Val = dict[keyRelPair]
			listOfTriplets = append(listOfTriplets, currTriplet)
		}
	}

	reply.TripList = listOfTriplets

	return nil
}

// Used for key-only partial match queries. Returns the lowest
// possible ChordID that could have the specified key, and the
// highest possible ChordID that could have the specified key
// (in that order).
func getLowestAndHighestValuesFromKey(key string) (*big.Int, *big.Int) {
	dummyKey := getDict3ChordKey(key, "fakeRelValueThatWillBeMaskedAwayAnyway")

	lowOrderBitMask := new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(chord.MBits/2)), nil)
	lowOrderBitMask = new(big.Int).Sub(lowOrderBitMask, big.NewInt(1))

	// fmt.Println(" @@@ lowOrderBitMask:", lowOrderBitMask) // todo remove

	highestPossibleValue := new(big.Int).Or(lowOrderBitMask, dummyKey)

	// fmt.Println(" @@@ highestPossibleValue:", highestPossibleValue) // todo remove

	highOrderBitMask := new(big.Int).Lsh(lowOrderBitMask, uint(chord.MBits/2))

	// fmt.Println(" @@@ highOrderBitMask:", highOrderBitMask) // todo remove

	lowestPossibleValue := new(big.Int).And(highOrderBitMask, dummyKey)

	// fmt.Println(" @@@ lowestPossibleValue:", lowestPossibleValue) // todo remove

	return lowestPossibleValue, highestPossibleValue
}

// Looks in its own dictionary for the key, and asks
// successor to do the same. Stops recursing when
// successor can't have any keys of the same value.
func (t *Node) SearchRingForKey(args *SearchRingForKeyArgs, reply *SearchRingForKeyReply) error {

	fmt.Println("SearchRingForKey RPC called, args=", args)

	lowestPossibleKey, highestPossibleKey := getLowestAndHighestValuesFromKey(string(args.Key))

	var listOfTriplets []Triplet

	// If I can't possibly contain a key in range(lowestPossibleKey, highestPossibleKey),
	// then just return the empty list.
	if chord.Inclusive_in(chord.FingerTable[chord.SELF].ChordID, chord.AddOne(args.LastNodeInQuery.ChordID),
		chord.SubOne(lowestPossibleKey)) {
		fmt.Println(" @@@ termination case with no local search of dict") // todo remove
		reply.TripList = listOfTriplets
		return nil
	}

	// If I'm the "end of the line", then search my local dict, but don't ask
	// any other nodes to do the same.
	singleNodeNetwork := chord.ChordNodePtrsAreEqual(&chord.FingerTable[0], &chord.FingerTable[1])
	if singleNodeNetwork || chord.Inclusive_in(highestPossibleKey, chord.Predecessor.ChordID, chord.FingerTable[chord.SELF].ChordID) {
		fmt.Println(" @@@ termination case with local search of dict ") // todo remove
		for keyRelPair, _ := range dict {
			if keyRelPair.Key == args.Key {
				var currTriplet Triplet
				currTriplet.Key = keyRelPair.Key
				currTriplet.Rel = keyRelPair.Rel
				currTriplet.Val = dict[keyRelPair]
				listOfTriplets = append(listOfTriplets, currTriplet)
			}
		}
		reply.TripList = listOfTriplets
		return nil
	}

	fmt.Println(" @@@ continuation case")

	// Now, the tricky(ish) part: I need to check my local dict for matching
	// keys, *and* I need to ask my successor if it has matching keys.
	tripListChannel := make(chan []Triplet)
	go callSearchRingForKeyOnSuccessor(string(args.Key), chord.FingerTable[1], args.LastNodeInQuery, tripListChannel)
	// Search the local dict while that RPC is running:
	for keyRelPair, _ := range dict {
		if keyRelPair.Key == args.Key {
			var currTriplet Triplet
			currTriplet.Key = keyRelPair.Key
			currTriplet.Rel = keyRelPair.Rel
			currTriplet.Val = dict[keyRelPair]
			listOfTriplets = append(listOfTriplets, currTriplet)
		}
	}

	var tripletsFromSuccessor []Triplet
	tripletsFromSuccessor = <-tripListChannel // this blocks until the RPC returns

	for _, newTriplet := range tripletsFromSuccessor {
		listOfTriplets = append(listOfTriplets, newTriplet)
	}

	reply.TripList = listOfTriplets
	return nil
}

// This is intended to independently (and concurrently) invoke the SearchRingForKey
// RPC on our successor, so that we can do other stuff while we wait for the
// successor to reply.
func callSearchRingForKeyOnSuccessor(key string, successor chord.ChordNodePtr,
	lastNodeInQuery chord.ChordNodePtr,
	tripListChannel chan []Triplet) {
	var searchRingForKeyArgs SearchRingForKeyArgs
	var searchRingForKeyReply SearchRingForKeyReply
	searchRingForKeyArgs.Key = TripKey(key)
	searchRingForKeyArgs.LastNodeInQuery = lastNodeInQuery
	// todo - don't forget to catch the error
	chord.CallRPC("Node.SearchRingForKey", &searchRingForKeyArgs, &searchRingForKeyReply, &successor)
	tripListChannel <- searchRingForKeyReply.TripList
}

// Partial match query for the Rel-only case. Searches all nodes (we decided to keep
// m = 160 so that we didn't have to deal with collision) using the algorithm Kalpakis
// gave us so that latency is logarithmic. The logarithmic latency is due to how this
// routine farms out searching all nodes to fingers.
func (t *Node) SearchRingForRel(args *SearchRingForRelArgs, reply *SearchRingForRelReply) error {

	relOnlyPartialMatchQueryCount += 1

	fmt.Println(" @@@ SearchRingForRel RPC called on node", chord.FingerTable[0].ChordID)
	fmt.Println(" @@@ StartVal: ", args.StartValue)
	fmt.Println(" @@@ StopVal: ", args.StopValue)

	callCount := 0
	tripListChannel := make(chan []Triplet)

	for i := 1; i <= chord.MBits; i++ {
		currChordID := chord.FingerTable[i].ChordID

		if chord.Inclusive_in(chord.FingerTable[i].ChordID, args.StartValue, args.StopValue) {
			var nextChordID *big.Int
			if i < chord.MBits {
				nextChordID = chord.FingerTable[i+1].ChordID
			} else {
				nextChordID = chord.FingerTable[chord.SELF].ChordID
			}

			// Skip entries that search for nil ranges
			if currChordID.Cmp(nextChordID) == 0 {
				continue
			}

			newStartValue := chord.AddOne(currChordID)
			newStopValue := chord.SubOne(nextChordID)

			// Don't look beyond where we're responsible for looking.
			if chord.Inclusive_in(args.StopValue, newStartValue, chord.SubOne(newStopValue)) {
				newStopValue = args.StopValue
			}

			if newStartValue.Cmp(newStopValue) != 0 {

				fmt.Println(" @@@@@@ Calling SearchRingForRel RPC on ", chord.FingerTable[i].ChordID, " [", i, "]", "startValue=", newStartValue, "stopValue=", newStopValue)
				callCount += 1
				go callSearchRingForRelOnSpecifiedNode(string(args.Rel), newStartValue, newStopValue,
					chord.FingerTable[i], tripListChannel)
			}

		} else {
			break
		}
	}

	// Search locally for the rel's while we wait for the RPCs to respond
	var listOfTriplets []Triplet
	for keyRelPair, _ := range dict {
		if keyRelPair.Rel == args.Rel {
			var currTriplet Triplet
			currTriplet.Key = keyRelPair.Key
			currTriplet.Rel = keyRelPair.Rel
			currTriplet.Val = dict[keyRelPair]
			listOfTriplets = append(listOfTriplets, currTriplet)
		}
	}

	for i := 0; i < callCount; i++ {
		rpcTripletList := <-tripListChannel
		listOfTriplets = append(listOfTriplets, rpcTripletList...)
	}

	reply.TripList = listOfTriplets
	return nil
}

// This is intended to independently (and concurrently) invoke the SearchRingForRel
// RPC on the specified node, so that we can do other stuff while we wait for the
// successor to reply.
func callSearchRingForRelOnSpecifiedNode(rel string, startValue *big.Int, stopValue *big.Int, node chord.ChordNodePtr,
	tripListChannel chan []Triplet) {

	var args SearchRingForRelArgs
	var reply SearchRingForRelReply
	args.Rel = TripRel(rel)
	args.StartValue = startValue
	args.StopValue = stopValue

	// todo - don't forget to catch the error
	chord.CallRPC("Node.SearchRingForRel", &args, &reply, &node)
	tripListChannel <- reply.TripList
}

// LOOKUP(keyA, relationA)
func (t *Node) Lookup(args *Args, reply *LookupReply) error {

	fmt.Print("  Lookup:    ", args.Key, ", ", args.Rel)

	// Remove any leading or trailing whitespace:
	tripletKey := strings.TrimSpace(string(args.Key))
	tripletRel := strings.TrimSpace(string(args.Rel))

	if len(tripletKey) != 0 && len(tripletRel) != 0 {

		fmt.Println(" *** Performing a 'normal' lookup on ", tripletKey, tripletRel)

		chordKey := getDict3ChordKey(tripletKey, tripletRel)
		successor, err := chord.FindSuccessor(chordKey)
		if err != nil {
			fmt.Println("Lookup (normal case) received an error when calling FindSuccessor(): ", err)
			// todo - populate the reply with the error or whatever the standard thing to do is
			return err
		}

		// If I'm the successor, just look in my local key store.
		if chord.ChordNodePtrsAreEqual(&successor, &chord.FingerTable[chord.SELF]) {
			// construct temp KeyRelPair
			krp := KeyRelPair{args.Key, args.Rel}

			// return triplet Value if KeyRelPair exists
			if tempVal, exists := dict[krp]; exists {
				// reply.Key = args.Key
				// reply.Rel = args.Rel
				// reply.Val = tempVal
				var triplet Triplet
				triplet.Key = args.Key
				triplet.Rel = args.Rel
				triplet.Val = tempVal
				var listOfTriplets []Triplet
				listOfTriplets = append(listOfTriplets, triplet)
				reply.TripList = listOfTriplets
				fmt.Println(" ... Triplet found in DICT3.")
			} else {
				fmt.Println(" ... Triplet NOT found in DICT3.")
			}
		} else {
			// Otherwise, just forward the request to the successor.
			chord.CallRPC("Node.Lookup", &args, &reply, &successor)
		}

	} else if len(tripletKey) != 0 && len(tripletRel) == 0 {

		fmt.Println(" *** Performing a key-only partial match on ", tripletKey)

		lowestPossibleKey, highestPossibleKey := getLowestAndHighestValuesFromKey(tripletKey)
		fmt.Println(" @@@ lowestPossibleKey:", lowestPossibleKey)   // todo
		fmt.Println(" @@@ highestPossibleKey:", highestPossibleKey) // todo

		startNode, err := chord.FindSuccessor(lowestPossibleKey)
		if err != nil {
			fmt.Println(" *** key-only search failed to call FindSuccessor with err=", err)
			return err
		}
		stopNode, err := chord.FindSuccessor(highestPossibleKey)
		if err != nil {
			fmt.Println(" *** key-only search failed to call FindSuccessor with err=", err)
			return err
		}

		fmt.Println(" @@@ key-only search would start with node: ", startNode)
		fmt.Println(" @@@ key-only search would stop with node: ", stopNode)

		var searchRingForKeyArgs SearchRingForKeyArgs
		var searchRingForKeyReply SearchRingForKeyReply
		searchRingForKeyArgs.Key = TripKey(tripletKey)
		searchRingForKeyArgs.LastNodeInQuery = stopNode
		chord.CallRPC("Node.SearchRingForKey", &searchRingForKeyArgs, &searchRingForKeyReply, &startNode)
		reply.TripList = searchRingForKeyReply.TripList

	} else if len(tripletKey) == 0 && len(tripletRel) != 0 {

		fmt.Println(" *** Performing a rel-only partial match on ", tripletRel)
		var searchRingForRelArgs SearchRingForRelArgs
		var searchRingForRelReply SearchRingForRelReply
		searchRingForRelArgs.Rel = TripRel(tripletRel)
		searchRingForRelArgs.StartValue = chord.FingerTable[1].ChordID
		searchRingForRelArgs.StopValue = chord.SubOne(searchRingForRelArgs.StartValue)
		chord.CallRPC("Node.SearchRingForRel", &searchRingForRelArgs, &searchRingForRelReply, &chord.FingerTable[1])
		reply.TripList = searchRingForRelReply.TripList // todo - may have to check for duplicates if network is unstable
		fmt.Println(" *** Finished performing the rel-only partial match on ", tripletRel)

	} else {

		fmt.Println(" *** Lookup RPC called with invalid args:", args)
		// todo - return errors.New or whatever
	}

	// // construct temp KeyRelPair
	// krp := KeyRelPair{args.Key, args.Rel}

	// // return triplet Value if KeyRelPair exists
	// if tempVal, exists := dict[krp]; exists {
	//  reply.Key = args.Key
	//  reply.Rel = args.Rel
	//  reply.Val = tempVal
	//  fmt.Println(" ... Triplet found in DICT3.")
	// } else {
	//  fmt.Println(" ... Triplet NOT found in DICT3.")
	// }

	return nil
}

// INSERT(keyA, relationA, valA)
func (t *Node) Insert(args *Args, reply *InsertReply) error {

	//fmt.Println("Insert RPC called with args:", args, "  reply:", reply)

	//create the key and relationship concatenated ID
	// keyRelID := chord.GetChordID(string(args.Key) + string(args.Rel))
	keyRelID := getDict3ChordKey(string(args.Key), string(args.Rel))

	//Find the successor of the KeyRelID
	keyRelSuccessor, err := chord.FindSuccessor(keyRelID)
	if err != nil {
		fmt.Println("ERROR: Insert() received an error when calling the Node.FindSuccessor RPC: ", err)
		fmt.Println("address: ", chord.FingerTable[chord.SELF].IpAddress, ":", chord.FingerTable[chord.SELF].Port)
		reply.TripletInserted = false
		return err
	}

	//Check if the current node is the successor of keyRelID
	//if not then make a RPC Insert call on the keyRelID's successor
	//else insert the args here
	if keyRelSuccessor.ChordID.Cmp(chord.FingerTable[0].ChordID) != 0 {

		//Connect to the successor node of KeyRelID

		//Copy reply
		newReply := reply

		err = chord.CallRPC("Node.Insert", &args, &newReply, &keyRelSuccessor)
		if err != nil {
			fmt.Println("node.go's Insert RPC failed to call the remote node's Insert with error:", err)
			return err
		}

		//return the reply message
		reply.TripletInserted = newReply.TripletInserted

	} else {

		//Print insert message
		fmt.Print("  Inserting:   ", "chordID: ", "(", keyRelID, ") ", args.Key, ", ", args.Rel, ", ", args.Val)

		// construct temp KeyRelPair
		krp := KeyRelPair{args.Key, args.Rel}

		// add key-rel pair if does not exist in dict
		if _, exists := dict[krp]; !exists {
			dict[krp] = args.Val
			reply.TripletInserted = true // default is false
			fmt.Println(" ... Doesnt exist in DICT3. Writing triplet to disk.")
			writeDictToDisk()
		} else {
			fmt.Println(" ... Triplet already exists in DICT3.")
			reply.TripletInserted = false
		}

	}
	return nil
}

// INSERTORUPDATE(keyA, relA, valA)
func (t *Node) InsertOrUpdate(args *Args, reply *string) error {

	fmt.Print("  InsOrUpd: ", args.Key, ", ", args.Rel, ", ", args.Val)

	//construct temp KeyRelPair
	krp := KeyRelPair{args.Key, args.Rel}
	dict[krp] = args.Val

	fmt.Println(" ... Writing new (or updated) triplet to disk.")
	writeDictToDisk()

	return nil
}

// DELETE(keyA, relA)
func (t *Node) Delete(args *Args, reply *DeleteReply) error {

	fmt.Print("  Delete:     ", args.Key, ", ", args.Rel)

	//create the key and relationship concatenated ID
	// keyRelID := chord.GetChordID(string(args.Key) + string(args.Rel))
	keyRelID := getDict3ChordKey(string(args.Key), string(args.Rel))

	//Find the successor of the KeyRelID
	keyRelSuccessor, err := chord.FindSuccessor(keyRelID)
	if err != nil {
		fmt.Println("ERROR: Delete() received an error when calling the Node.FindSuccessor RPC: ", err)
		fmt.Println("address: ", chord.FingerTable[chord.SELF].IpAddress, ":", chord.FingerTable[chord.SELF].Port)
		reply.TripletDeleted = false
		return err
	}

	//Check if the current node is the successor of keyRelID
	//if not then make a RPC Delete call on the keyRelID's successor
	//else insert the args here
	if keyRelSuccessor.ChordID.Cmp(chord.FingerTable[0].ChordID) != 0 {

		//Connect to the successor node of KeyRelID

		//Copy reply
		newReply := reply

		err = chord.CallRPC("Node.Delete", &args, &newReply, &keyRelSuccessor)
		if err != nil {
			fmt.Println("node.go's Delete RPC failed to call the remote node's Delete with error:", err)
		}

		//return the reply message
		reply.TripletDeleted = newReply.TripletDeleted

	} else {

		//Print insert message
		fmt.Print("  Deleting:      ", args.Key, ", ", args.Rel, ", ", args.Val)

		// construct temp KeyRelPair
		krp := KeyRelPair{args.Key, args.Rel}
		delete(dict, krp)
		fmt.Println(" ... Removing triplet from DICT3 and writing to disk.")
		writeDictToDisk()
		reply.TripletDeleted = true
	}

	return nil
}

// LISTKEYS()
func (t *Node) ListKeys(args *Args, reply *ListKeysReply) error {

	fmt.Println("  ListKeys ")

	// use map as a set of unique keys
	var uniqueKeys = make(map[TripKey]bool)
	var result []TripKey

	// "Go Maps In Action" inspired: https://blog.golang.org/go-maps-in-action
	for krp, _ := range dict {
		if _, added := uniqueKeys[krp.Key]; !added {
			uniqueKeys[krp.Key] = true
			result = append(result, krp.Key)
		}
	}
	reply.KeyList = result
	return nil
}

// LISTIDs()
func (t *Node) ListIDs(args *Args, reply *ListIDsReply) error {

	fmt.Println("  ListIDs")

	var ids []KeyRelPair
	for krp, _ := range dict {
		ids = append(ids, krp)
	}
	reply.IDList = ids
	return nil
}

// SHUTDOWN()
func (t *Node) Shutdown(args *Args, reply *string) error {

	fmt.Println("***Preparing to shut down. Transferring my keys to my successor...")

	//Leave the network and update the others
	chord.RunStabilize = false
	runListener = false
	for !chord.FFDone || !stabilizeDone || !listenerDone {
		time.Sleep(time.Millisecond)
	}

	fmt.Printf("***Updating my successor's predecessor (currently me, Node %d) to now point to my predecessor (Node %d)\n", chord.FingerTable[0].ChordID, chord.Predecessor.ChordID)
	var argsSetPredecessor chord.SetPredecessorArgs
	var setPredecessorReply chord.SetPredecessorReply
	argsSetPredecessor.ChordNodePtr = chord.Predecessor

	err := chord.CallRPC("Node.SetPredecessor", &argsSetPredecessor, &setPredecessorReply, &chord.FingerTable[1])
	if err != nil {
		fmt.Println(err)
		return err
	}

	fmt.Printf("***Updating my predecessor (Node %d) to now point to my successor (Node %d) \n", chord.Predecessor.ChordID, chord.FingerTable[1].ChordID)
	var argsSetSuccessor chord.SetSucessorArgs
	var setSuccessorReply chord.SetSuccessorReply
	argsSetSuccessor.ChordNodePtr = chord.FingerTable[1]

	err = chord.CallRPC("Node.SetSuccessor", &argsSetSuccessor, &setSuccessorReply, &chord.Predecessor)
	if err != nil {
		fmt.Println(err)
		return err
	}

	var numKeysTransferred int = 0
	for kr, v := range dict {

		var argXferInsert Args
		var xferInsertreply InsertReply
		argXferInsert.Key = kr.Key
		argXferInsert.Rel = kr.Rel
		argXferInsert.Val = v

		err := chord.CallRPC("Node.TransferInsert", &argXferInsert, &xferInsertreply, &chord.FingerTable[1])
		if err != nil {
			fmt.Println(err)
			return err
		}
		numKeysTransferred++
	}
	fmt.Printf("***Triplets transferred: %d\n", numKeysTransferred)

	fmt.Println("***Closing persistent connections.")
	chord.ClosePersistentConnections()

	shutdownDone = true
	return nil
}

// invoked on an existing node in the chord ring by a joining node.  the joining node passes its IP, Port, NodeID (as a ChordNodePtr)
// to the existing node, which then uses that information to determine which keys need to be moved to the joining node.
func (t *Node) TransferKeys(args *chord.TransferKeysArgs, reply *chord.TransferKeysReply) error {
	fmt.Println("TransferKeys() called. Checking to see if I need to transfer some of my keys to: Node", args.ChordNodePtr.ChordID)

	var numKeysTransferred = 0

	// if joining_node.ID > my.ID (e.g., 99 joining ring with 41)
	if args.ChordNodePtr.ChordID.Cmp(chord.FingerTable[0].ChordID) > 0 {

		// loop thru local DICT3 and find all keys that need to be transferred
		for kr, v := range dict {
			// var chordKeyRelID = chord.GetChordID(string(kr.Key) + string(kr.Rel))
			var chordKeyRelID = getDict3ChordKey(string(kr.Key), string(kr.Rel))

			// transfer: key.ID > my.ID && keyID <= joining_node.ID
			if (chordKeyRelID.Cmp(chord.FingerTable[0].ChordID) > 0) && (chordKeyRelID.Cmp(args.ChordNodePtr.ChordID) <= 0) {

				// RPC call to insert this triplet
				var insertReply InsertReply
				var insertArgs Args
				insertArgs.Key = kr.Key
				insertArgs.Rel = kr.Rel
				insertArgs.Val = v

				// call to TransferInsert (just our original Insert method from project1) is needed because if we call our modified Insert method (which now
				// calls findsuccessor BEFORE inserting in its local DICT3), the node that is responsible for transferring the keys will attempt to insert
				// the same keys on itself - because it has no knowledge yet of the joining node as part of the chord ring at this point in time
				err := chord.CallRPC("Node.TransferInsert", &insertArgs, &insertReply, &args.ChordNodePtr)
				if err != nil {
					fmt.Println("node.go's TransferKeys RPC call failed to call the remote node's TransferInsert with error:", err)
					reply.TransferKeysCompleted = false
					return err
				}
				numKeysTransferred++
			}
		}
	}

	// if joining_node.ID < my.ID (e.g., 20 joining ring with 41)
	if args.ChordNodePtr.ChordID.Cmp(chord.FingerTable[0].ChordID) < 0 {

		// loop thru local DICT3 and find all keys that need to be transferred
		for kr, v := range dict {
			// var chordKeyRelID = chord.GetChordID(string(kr.Key) + string(kr.Rel))
			var chordKeyRelID = getDict3ChordKey(string(kr.Key), string(kr.Rel))

			// transfer: key.ID > my.ID || keyID <= joining_node.ID
			if (chordKeyRelID.Cmp(chord.FingerTable[0].ChordID) > 0) || (chordKeyRelID.Cmp(args.ChordNodePtr.ChordID) <= 0) {

				// RPC call to insert this triplet
				var insertReply InsertReply
				var insertArgs Args
				insertArgs.Key = kr.Key
				insertArgs.Rel = kr.Rel
				insertArgs.Val = v

				// call to TransferInsert (just our original Insert method from project1) is needed because if we call our modified Insert method (which now
				// calls findsuccessor BEFORE inserting in its local DICT3), the node that is responsible for transferring the keys will attempt to insert
				// the same keys on itself - because it has no knowledge yet of the joining node as part of the chord ring at this point in time
				err := chord.CallRPC("Node.TransferInsert", &insertArgs, &insertReply, &args.ChordNodePtr)
				if err != nil {
					fmt.Println("node.go's TransferKeys RPC call failed to call the remote node's TransferInsert with error:", err)
					reply.TransferKeysCompleted = false
					return err
				}
				numKeysTransferred++
			}
		}
	}

	fmt.Printf("Number of keys transferred: %d\n", numKeysTransferred)

	reply.TransferKeysCompleted = true
	return nil
}

// used to allow a node to directly insert key,rel,val on another node without
// looking up findsuccessor
func (t *Node) TransferInsert(args *Args, reply *InsertReply) error {

	// keyRelID := chord.GetChordID(string(args.Key) + string(args.Rel))
	keyRelID := getDict3ChordKey(string(args.Key), string(args.Rel))

	fmt.Print(" TransferInsert: ", "chordID: ", "(", keyRelID, ") ", args.Key, ", ", args.Rel, ", ", args.Val)

	// construct temp KeyRelPair
	krp := KeyRelPair{args.Key, args.Rel}

	// add key-rel pair if does not exist in dict
	if _, exists := dict[krp]; !exists {
		dict[krp] = args.Val
		reply.TripletInserted = true // default is false
		fmt.Println(" ... Doesnt exist in DICT3. Writing to disk.")
		writeDictToDisk()
	} else {
		fmt.Println(" ... Triplet already exists in DICT3.")
		reply.TripletInserted = false
	}
	return nil
}

//--------------CHORD WRAPPER METHODS-----------------------------
func (t *Node) FindSuccessor(args *chord.ChordIDArgs, reply *chord.FindSuccessorReply) error {
	//fmt.Println("FindSuccessor wrapper called with id: ", args.Id)

	var err error
	reply.ChordNodePtr, err = chord.FindSuccessor(args.Id)
	if err != nil {
		fmt.Println("FindSuccessor() RPC received an error when calling chord.FindSuccessor()")
	}

	return err
}

// "reply *interface{}" means that no reply is sent.
func (t *Node) Notify(args *chord.NotifyArgs, reply *chord.NotifyReply) error {
	//fmt.Println("Notify wrapper called.")
	chord.Notify(args.ChordNodePtr)
	reply.Dummy = "Dummy Notify Response"
	return nil
}

// Takes no arguments, but does send a reply.
func (t *Node) GetPredecessor(args *interface{}, reply *chord.GetPredecessorReply) error {
	//fmt.Println("GetPredecessor() RPC called.")
	reply.Predecessor = chord.Predecessor
	return nil
}

// argument is a ChordNodePtr (the new predecessor)
func (t *Node) SetPredecessor(args *chord.SetPredecessorArgs, reply *chord.SetPredecessorReply) error {
	fmt.Printf("SetPredecessor() called, setting predecessor to: %d\n", args.ChordNodePtr.ChordID)

	chord.Predecessor.IpAddress = args.ChordNodePtr.IpAddress
	chord.Predecessor.Port = args.ChordNodePtr.Port
	chord.Predecessor.ChordID = args.ChordNodePtr.ChordID

	fmt.Printf("My Predecessor is now: %v\n", chord.Predecessor)
	return nil
}

// argument is a ChordNodePtr (the new successor)
func (t *Node) SetSuccessor(args *chord.SetSucessorArgs, reply *chord.SetSuccessorReply) error {
	fmt.Printf("SetSuccessor() called, setting successor to: %d\n", args.ChordNodePtr.ChordID)

	chord.FingerTable[1].IpAddress = args.ChordNodePtr.IpAddress
	chord.FingerTable[1].Port = args.ChordNodePtr.Port
	chord.FingerTable[1].ChordID = args.ChordNodePtr.ChordID

	fmt.Printf("My Successor is now: %v\n", chord.FingerTable[1])
	return nil
}

//--------------CHORD WRAPPER METHODS-----------------------------

func main() {

	if len(os.Args) != 2 {
		fmt.Println("Usage: ", os.Args[0], "Path to server configuration file")
		os.Exit(1)
	}
	infile := os.Args[1]

	// read in configuration file
	fmt.Println("Loading server configuration...")
	parseConfigurationFile(infile)

	// open persistent storage container
	fmt.Println("Accessing DICT3 persistent storage...")
	openPersistentStorageContainer(conf.PersistentStorageContainer.File)

	// start server
	fmt.Println("Starting server ...")
	tcpAddr, err := net.ResolveTCPAddr(conf.Protocol, ":"+conf.Port)
	checkErrorCondition(err)

	// register procedure call
	n := new(Node)
	rpc.Register(n)

	listener, err := net.ListenTCP(conf.Protocol, tcpAddr)
	checkErrorCondition(err)
	defer listener.Close()

	// display this node's ID based on SHA-1 hash value
	fmt.Printf("Chord Node ID: %d\n", chord.GetChordID(conf.IpAddress+":"+conf.Port))

	// TODO put this in config file
	// bootstrap, first node with port number 7001 creates the ring, and the rest join
	if conf.Port == "7001" {
		chord.Create(conf.IpAddress, conf.Port)
		//fmt.Println("Finger Table: ", chord.FingerTable)
	} else {
		// contact existing node

		// introduce a little delay to allow the listener to get up first before attempting
		// to join, otherwise this node will not be able to have keys inserted via
		// the TransferKeys RPC
		duration, _ := time.ParseDuration("3s")
		time.Sleep(duration)

		// TODO - this hard-coded stuff should really be in the config file
		go join("127.0.0.1", "7001")
	}

	fmt.Printf("Listening on port " + conf.Port + " ...\n")

	go periodicallyStabilize()
	go chord.FixFingers()
	go n.sigHandler()

	for runListener {
		listener.SetDeadline(time.Now().Add(time.Second))
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go jsonrpc.ServeConn(conn)
	}
	listenerDone = true
	for !shutdownDone {
		time.Sleep(time.Millisecond)
	}
}
func join(existingNodeIpAddress string, existingNodePort string) {
	duration, _ := time.ParseDuration("3s")

	// continue to attempting to connect until success
	for chord.Join(existingNodeIpAddress, existingNodePort, conf.IpAddress, conf.Port) != nil {
		time.Sleep(duration)
	}
}

func periodicallyStabilize() {
	// todo - this, and other methods, should probably be using RWLock.
	duration, _ := time.ParseDuration("3s")
	for chord.RunStabilize {
		time.Sleep(duration)
		chord.Stabilize()
		deleteAnyTransferredKeys()

		fmt.Println("Finger Table:")
		for index, val := range chord.FingerTable {
			fmt.Println(index, val)
		}
		fmt.Println("relOnlyPartialMatchQueryCount", relOnlyPartialMatchQueryCount)

		//fmt.Println("periodicallyStabilize(), predecess:", chord.Predecessor)
		//fmt.Println("periodicallyStabilize(), myself   :", chord.FingerTable[0])

		//fmt.Println("periodicallyStabilize(), successor:", chord.FingerTable[1])
	}
	stabilizeDone = true
}

// this is called immediately after Stabilize() has completed and checks the local DICT3 for
// any Triplets that might need to be deleted because they were just copied as part of
// a call to TransferKeys().  Any keys found to belong to the predecessor are deleted
func deleteAnyTransferredKeys() error {
	fmt.Println("Stabilize completed - Now checking if I need to delete any duplicate keys that may have been transferred")

	// ensure stabilize has completed and a predecessor exists, otherwise null pointer dereference
	if chord.Predecessor.ChordID != nil {

		// if my predecessor has a NodeID < me
		if chord.Predecessor.ChordID.Cmp(chord.FingerTable[0].ChordID) < 0 {
			for kr, _ := range dict {
				// var chordKeyRelID = chord.GetChordID(string(kr.Key) + string(kr.Rel))
				var chordKeyRelID = getDict3ChordKey(string(kr.Key), string(kr.Rel))

				//delete key if keyID <= predecessor OR keyID > me
				if (chordKeyRelID.Cmp(chord.Predecessor.ChordID) <= 0) || (chordKeyRelID.Cmp(chord.FingerTable[0].ChordID) > 0) {
					fmt.Printf("     Duplicate key found - need to delete in my local DICT3 Chord Key-Rel ID: %d\n", chordKeyRelID)

					// delete the triplet and write to disk
					delete(dict, kr)
					writeDictToDisk()
				}
			}
		}

		// if my predecessor has a NodeID > me:
		if chord.Predecessor.ChordID.Cmp(chord.FingerTable[0].ChordID) > 0 {
			for kr, _ := range dict {
				// var chordKeyRelID = chord.GetChordID(string(kr.Key) + string(kr.Rel))
				var chordKeyRelID = getDict3ChordKey(string(kr.Key), string(kr.Rel))

				//delete key if KeyID <= predID AND keyID > me
				if (chordKeyRelID.Cmp(chord.Predecessor.ChordID) <= 0) && (chordKeyRelID.Cmp(chord.FingerTable[0].ChordID) > 0) {
					fmt.Printf("     Duplicate key found - need to delete in my local DICT3 Chord Key-Rel ID: %d\n", chordKeyRelID)

					// delete the triplet and write to disk
					delete(dict, kr)
					writeDictToDisk()
				}
			}
		}
	}

	// -- DEBUG REMOVE
	var numKeys int = 0
	fmt.Printf("Node ID: %d\n", chord.GetChordID(conf.IpAddress+":"+conf.Port))
	fmt.Println("DICT3 contents are now: ")
	for k, v := range dict {
		// var chordKey = string(k.Key) + string(k.Rel)
		// fmt.Printf(" (%d)", chord.GetChordID(chordKey))
		fmt.Printf(" (%d)", getDict3ChordKey(string(k.Key), string(k.Rel)))
		fmt.Println("  ", k, v)
		numKeys++
	}
	fmt.Println("Total triplets: ", numKeys)
	fmt.Println("------------------------")
	// -- DEBUG REMOVE

	return nil
}

func writeDictToDisk() {
	// zero out existing storage container contents by creating new file
	storageFile, err := os.Create(conf.PersistentStorageContainer.File)
	checkErrorCondition(err)

	// write map back to file
	enc := gob.NewEncoder(storageFile)
	enc.Encode(dict)
	storageFile.Close()
}

func parseConfigurationFile(infile string) {
	// read in server configuration
	configurationFile, err := ioutil.ReadFile(infile)
	checkErrorCondition(err)

	// parse JSON in server configuration
	err = json.Unmarshal(configurationFile, &conf)
	checkErrorCondition(err)

	fmt.Println("   ServerID: ", conf.ServerID)
	fmt.Println("   Protocol: ", conf.Protocol)
	fmt.Println("   IP Address: ", conf.IpAddress)
	fmt.Println("   Port: ", conf.Port)
	fmt.Println("   Persistent Storage Container: ", conf.PersistentStorageContainer.File)
	fmt.Println("   Methods: ", conf.Methods)
}

func openPersistentStorageContainer(pathToStorageContainer string) {

	// open file if it exists, create new file if not
	storageFile, err := os.OpenFile(pathToStorageContainer, os.O_RDWR|os.O_CREATE, 0666)
	checkErrorCondition(err)

	// read back in binary data stored on disk to DICT3
	dec := gob.NewDecoder(storageFile)
	dec.Decode(&dict)
	fmt.Println("   DICT3 contents stored on disk: ")
	for k, v := range dict {
		// var chordKey = string(k.Key) + string(k.Rel)
		fmt.Print("    ", k, v)
		// fmt.Printf("     Chord Key-Rel ID: %d\n", chord.GetChordID(chordKey))
		fmt.Printf("     Chord Key-Rel ID: %d\n", getDict3ChordKey(string(k.Key), string(k.Rel)))
	}
	storageFile.Close()
}

func checkErrorCondition(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func (t *Node) sigHandler() {
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT, syscall.SIGKILL)
	<-c
	t.Shutdown(nil, nil)
}
