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
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"time"
)

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

type Node struct {
	stabilizeDuration time.Duration
	conf              ServerConfiguration
	dict              map[KeyRelPair]TripVal
	listen            bool
	chord             *chord.Chord
}

type TripKey string
type TripRel string
type TripVal map[string]interface{}
type KeyRelPair struct {
	Key TripKey
	Rel TripRel
}

type Args struct {
	Key TripKey
	Rel TripRel
	Val TripVal // only used for insert and insertOrUpdate,
}

type LookupReply struct {
	Key TripKey
	Rel TripRel
	Val TripVal
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

// LOOKUP(keyA, relationA)
func (t *Node) Lookup(args *Args, reply *LookupReply) error {

	fmt.Print("  Lookup:    ", args.Key, ", ", args.Rel)

	// construct temp KeyRelPair
	krp := KeyRelPair{args.Key, args.Rel}

	// return triplet Value if KeyRelPair exists
	if tempVal, exists := t.dict[krp]; exists {
		reply.Key = args.Key
		reply.Rel = args.Rel
		reply.Val = tempVal
		fmt.Println(" ... Triplet found in DICT3.")
	} else {
		fmt.Println(" ... Triplet NOT found in DICT3.")
	}
	return nil
}

// INSERT(keyA, relationA, valA)
func (t *Node) Insert(args *Args, reply *InsertReply) error {

	fmt.Println("Insert RPC called with args:", args, "  reply:", reply)

	//create the key and relationship concatenated ID
	keyRelID := chord.GetChordID(string(args.Key) + string(args.Rel))

	//Find the successor of the KeyRelID
	keyRelSuccessor, err := t.chord.FindSuccessor(keyRelID)
	if err != nil {
		fmt.Println("ERROR: Insert() received an error when calling the Node.FindSuccessor RPC: ", err)
		fmt.Println("address: ", t.chord.FingerTable[chord.SELF].IpAddress, ":", t.chord.FingerTable[chord.SELF].Port)
		reply.TripletInserted = false
		return err
	}

	//Check if the current node is the successor of keyRelID
	//if not then make a RPC Insert call on the keyRelID's successor
	//else insert the args here
	if keyRelSuccessor.ChordID.Cmp(t.chord.FingerTable[0].ChordID) != 0 {

		//Connect to the successor node of KeyRelID

		//Copy reply
		newReply := reply

		err = t.chord.CallRPC("Node.Insert", &args, &newReply, keyRelSuccessor)
		if err != nil {
			fmt.Println("node.go's Insert RPC failed to call the remote node's Insert with error:", err)
			return err
		}

		//return the reply message
		reply.TripletInserted = newReply.TripletInserted

	} else {

		//Print insert message
		fmt.Print("  Inserting:      ", args.Key, ", ", args.Rel, ", ", args.Val)

		// construct temp KeyRelPair
		krp := KeyRelPair{args.Key, args.Rel}

		// add key-rel pair if does not exist in dict
		if _, exists := t.dict[krp]; !exists {
			t.dict[krp] = args.Val
			reply.TripletInserted = true // default is false
			fmt.Println(" ... Does not exist in DICT3. Writing new triplet to disk.")
			t.writeDictToDisk()
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
	t.dict[krp] = args.Val

	fmt.Println(" ... Writing new (or updated) triplet to disk.")
	t.writeDictToDisk()

	return nil
}

// DELETE(keyA, relA)
func (t *Node) Delete(args *Args, reply *DeleteReply) error {

	fmt.Print("  Delete:     ", args.Key, ", ", args.Rel)

	//create the key and relationship concatenated ID
	keyRelID := chord.GetChordID(string(args.Key) + string(args.Rel))

	//Find the successor of the KeyRelID
	keyRelSuccessor, err := t.chord.FindSuccessor(keyRelID)
	if err != nil {
		fmt.Println("ERROR: Delete() received an error when calling the Node.FindSuccessor RPC: ", err)
		fmt.Println("address: ", t.chord.FingerTable[chord.SELF].IpAddress, ":", t.chord.FingerTable[chord.SELF].Port)
		reply.TripletDeleted = false
		return err
	}

	//Check if the current node is the successor of keyRelID
	//if not then make a RPC Delete call on the keyRelID's successor
	//else insert the args here
	if keyRelSuccessor.ChordID.Cmp(t.chord.FingerTable[0].ChordID) != 0 {

		//Connect to the successor node of KeyRelID

		//Copy reply
		newReply := reply

		err = t.chord.CallRPC("Node.Delete", &args, &newReply, keyRelSuccessor)
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
		delete(t.dict, krp)
		fmt.Println(" ... Removing triplet from DICT3 and writing to disk.")
		t.writeDictToDisk()
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
	for krp, _ := range t.dict {
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
	for krp, _ := range t.dict {
		ids = append(ids, krp)
	}
	reply.IDList = ids
	return nil
}

// SHUTDOWN()
func (t *Node) Shutdown(args *Args, reply *string) error {

	fmt.Println("  Shutting down ... ")
	t.listen = false
	return nil
}

//--------------CHORD WRAPPER METHODS-----------------------------
func (t *Node) FindSuccessor(args *chord.ChordIDArgs, reply *chord.FindSuccessorReply) error {
	fmt.Println("FindSuccessor wrapper called with id: ", args.Id)

	var err error
	suc, err := t.chord.FindSuccessor(args.Id)
	reply.ChordNodePtr = suc
	if err != nil {
		fmt.Println("FindSuccessor() RPC received an error when calling chord.FindSuccessor()")
	}

	return err
}

// "reply *interface{}" means that no reply is sent.
func (t *Node) Notify(args *chord.NotifyArgs, reply *chord.NotifyReply) error {
	fmt.Println("Notify wrapper called.")
	t.chord.Notify(args.ChordNodePtr)
	reply.Dummy = "Dummy Notify Response"
	return nil
}

// Takes no arguments, but does send a reply.
func (t *Node) GetPredecessor(args *interface{}, reply *chord.GetPredecessorReply) error {
	fmt.Println("GetPredecessor() RPC called.")
	reply.Predecessor = t.chord.Predecessor
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
	conf := parseConfigurationFile(infile)

	// run node
	node := new(Node)
	node.stabilizeDuration = 2 * time.Second
	node.run(conf)
}

func (t *Node) run(conf ServerConfiguration) {
	t.conf = conf
	t.dict = map[KeyRelPair]TripVal{}
	t.listen = true

	// open persistent storage container
	fmt.Println("Accessing DICT3 persistent storage...")
	t.openPersistentStorageContainer(t.conf.PersistentStorageContainer.File)

	// start server
	fmt.Println("Starting server ...")
	tcpAddr, err := net.ResolveTCPAddr(t.conf.Protocol, ":"+t.conf.Port)
	checkErrorCondition(err)

	// register procedure call
	rpc.Register(t)

	listener, err := net.ListenTCP(t.conf.Protocol, tcpAddr)
	checkErrorCondition(err)
	defer listener.Close()

	// display this node's ID based on SHA-1 hash value
	fmt.Printf("Chord Node ID: %d\n", chord.GetChordID(t.conf.IpAddress+":"+t.conf.Port))

	// bootstrap, first node with port number 7001 creates the ring, and the rest join
	t.chord = chord.GimmeAChord(t.conf.IpAddress, t.conf.Port)
	t.chord.StabilizeDuration = t.stabilizeDuration
	if t.conf.Port == "7001" {
		t.chord.Create()
		fmt.Println("Finger Table: ", t.chord.FingerTable)
	} else {
		// contact CreatedNode and pass in my own chord ID
		t.chord.Join("127.0.0.1", "7001")
	}
	fmt.Printf("Listening on port " + t.conf.Port + " ...\n")

	go t.periodicallyStabilize()
	go t.chord.FixFingers()

	for t.listen {
		listener.SetDeadline(time.Now().Add(time.Second))
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go jsonrpc.ServeConn(conn)
	}
}

func (t *Node) periodicallyStabilize() {
	// todo - this, and other methods, should probably be using RWLock.
	for {
		time.Sleep(t.stabilizeDuration)
		t.chord.Stabilize()

		fmt.Println("periodicallyStabilize(), predecess:", t.chord.Predecessor)
		fmt.Println("periodicallyStabilize(), myself   :", t.chord.FingerTable[0])
		fmt.Println("periodicallyStabilize(), successor:", t.chord.FingerTable[1])
	}
}

func (t *Node) writeDictToDisk() {
	// zero out existing storage container contents by creating new file
	storageFile, err := os.Create(t.conf.PersistentStorageContainer.File)
	checkErrorCondition(err)

	// write map back to file
	enc := gob.NewEncoder(storageFile)
	enc.Encode(t.dict)
	storageFile.Close()
}

func parseConfigurationFile(infile string) ServerConfiguration {
	var conf ServerConfiguration

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
	return conf
}

func (t *Node) openPersistentStorageContainer(pathToStorageContainer string) {

	// open file if it exists, create new file if not
	storageFile, err := os.OpenFile(pathToStorageContainer, os.O_RDWR|os.O_CREATE, 0666)
	checkErrorCondition(err)

	// read back in binary data stored on disk to DICT3
	dec := gob.NewDecoder(storageFile)
	dec.Decode(&(t.dict))
	fmt.Println("   DICT3 contents stored on disk: ")
	for k, v := range t.dict {
		var chordKey = string(k.Key) + string(k.Rel)
		fmt.Print("    ", k, v)
		fmt.Printf("     Chord Key-Rel ID: %d\n", chord.GetChordID(chordKey))
	}
	storageFile.Close()
}

func checkErrorCondition(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
