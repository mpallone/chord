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
	"bytes"
	"strconv"
)

type keyRelValue struct {
	Content    interface{}
	Size       string
	Created    string
	Modified   string
	Accessed   string
	Permission string
}

// layout shows by example how the reference time should be represented.
const longForm = "1/_2/2006, 15:04:05"

// server configuration object read in
var conf ServerConfiguration

type ServerConfiguration struct {
	ServerID                   string
	Protocol                   string
	IpAddress                  string
	Port                       string
	PersistentStorageContainer struct {
		File string
	}
	Methods []string
	Purge string
}

type Node int

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
	Val keyRelValue // only used for insert and insertOrUpdate,
}

type LookupReply struct {
	Key TripKey
	Rel TripRel
	Val keyRelValue
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
var dict = map[KeyRelPair]keyRelValue{}

// LOOKUP(keyA, relationA)
func (t *Node) Lookup(args *Args, reply *LookupReply) error {

	fmt.Print("  Lookup:    ", args.Key, ", ", args.Rel)

	// construct temp KeyRelPair
	krp := KeyRelPair{args.Key, args.Rel}

	// return triplet Value if KeyRelPair exists
	if tempVal, exists := dict[krp]; exists {
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

		//Copy reply
		newReply := reply

		//Make an RPC Insert call on the keyRelID's successor
		err = chord.CallRPC("Node.Insert", &args, &newReply, &keyRelSuccessor)
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

		//Calculate content size:
		size, err := GetBytes(args.Val.Content)
		if err != nil {
			fmt.Println("Time format is wrong", err)
			return err
		}
		ContentSize := strconv.Itoa(len(size)) + "bytes"
		
		//Set the content size
		args.Val.Size = ContentSize
		
		//Get the current system date and time
		created := time.Now().Format(longForm)
		
		//Set the created date
		args.Val.Created = created

		// add key-rel pair if does not exist in dict
		if _, exists := dict[krp]; !exists {
			dict[krp] = args.Val
			reply.TripletInserted = true // default is false
			fmt.Println(" ... Does not exist in DICT3. Writing new triplet to disk.")
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

	//create the key and relationship concatenated ID
	keyRelID := chord.GetChordID(string(args.Key) + string(args.Rel))

	//Find the successor of the KeyRelID
	keyRelSuccessor, err := chord.FindSuccessor(keyRelID)
	if err != nil {
		fmt.Println("ERROR: Insert() received an error when calling the Node.FindSuccessor RPC: ", err)
		fmt.Println("address: ", chord.FingerTable[chord.SELF].IpAddress, ":", chord.FingerTable[chord.SELF].Port)
		return err
	}

	//Check if the current node is the successor of keyRelID
	//if not then make a RPC Insert call on the keyRelID's successor
	//else insert the args here
	if keyRelSuccessor.ChordID.Cmp(chord.FingerTable[0].ChordID) != 0 {

		//Copy reply
		newReply := reply

		//Make an RPC Insert call on the keyRelID's successor
		err = chord.CallRPC("Node.InsertOrUpdate", &args, &newReply, &keyRelSuccessor)
		if err != nil {
			fmt.Println("node.go's Insert RPC failed to call the remote node's InsertOrUpdate with error:", err)
			return err
		}
	} else {
		fmt.Print("  InsOrUpdate: ", args.Key, ", ", args.Rel, ", ", args.Val)
	
		// construct temp KeyRelPair
		krp := KeyRelPair{args.Key, args.Rel}

		//Calculate content size:
		size, err := GetBytes(args.Val.Content)
		if err != nil {
			fmt.Println("Time format is wrong", err)
			return err
		}
		ContentSize := strconv.Itoa(len(size)) + "bytes"
		
		//Set the content size
		args.Val.Size = ContentSize
		
		//Get the current system date and time
		created := time.Now().Format(longForm)
		modified := time.Now().Format(longForm)
		
		// add key-rel pair if does not exist in dict
		if _, exists := dict[krp]; !exists {
			//Set the created date
			args.Val.Created = created
			dict[krp] = args.Val
			fmt.Println(" ... Writing new (or updated) triplet to disk.")
			writeDictToDisk()
		}else{//update
			//Set the modified date
			args.Val.Modified = modified
			dict[krp] = args.Val
			fmt.Println(" ... Writing new (or updated) triplet to disk.")
			writeDictToDisk()
		}
		}
	return nil
}

// DELETE(keyA, relA)
func (t *Node) Delete(args *Args, reply *DeleteReply) error {

	fmt.Print("  Delete:     ", args.Key, ", ", args.Rel)

	//create the key and relationship concatenated ID
	keyRelID := chord.GetChordID(string(args.Key) + string(args.Rel))

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
	//else delete the args here
	if keyRelSuccessor.ChordID.Cmp(chord.FingerTable[0].ChordID) != 0 {

		//Copy reply
		newReply := reply

		//Make an RPC Delete call on the KeyRelID's successor
		err = chord.CallRPC("Node.Delete", &args, &newReply, &keyRelSuccessor)
		if err != nil {
			fmt.Println("node.go's Delete RPC failed to call the remote node's Delete with error:", err)
		}

		//return the reply message
		reply.TripletDeleted = newReply.TripletDeleted

	} else {

		// construct temp KeyRelPair
		krp := KeyRelPair{args.Key, args.Rel}

		//Print delete message
		fmt.Println("  Deleting:      ", krp, ", ", args.Val)

		//Check if the content is Read only
		fmt.Println(len(dict))
		for krp := range dict {
			val := dict[krp]			
			if p := val.Permission; p == "R" {
				//Read only: can not delete
			} else if p := val.Permission; p == "RW" {
				delete(dict, krp)
				fmt.Println(" ... Removing triplet from DICT3 and writing to disk.")
				writeDictToDisk()
				reply.TripletDeleted = true
			} else {
				//invalid permission
				//do nothing
				reply.TripletDeleted = false
			}
		}
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

	fmt.Println("  Shutting down ... ")
	os.Exit(0)
	return nil
}

//--------------CHORD WRAPPER METHODS-----------------------------
func (t *Node) FindSuccessor(args *chord.ChordIDArgs, reply *chord.FindSuccessorReply) error {
	fmt.Println("FindSuccessor wrapper called with id: ", args.Id)

	var err error
	reply.ChordNodePtr, err = chord.FindSuccessor(args.Id)
	if err != nil {
		fmt.Println("FindSuccessor() RPC received an error when calling chord.FindSuccessor()")
	}

	return err
}

// "reply *interface{}" means that no reply is sent.
func (t *Node) Notify(args *chord.NotifyArgs, reply *chord.NotifyReply) error {
	fmt.Println("Notify wrapper called.")
	chord.Notify(args.ChordNodePtr)
	reply.Dummy = "Dummy Notify Response"
	return nil
}

// Takes no arguments, but does send a reply.
func (t *Node) GetPredecessor(args *interface{}, reply *chord.GetPredecessorReply) error {
	fmt.Println("GetPredecessor() RPC called.")
	reply.Predecessor = chord.Predecessor
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
	rpc.Register(new(Node))

	listener, err := net.ListenTCP(conf.Protocol, tcpAddr)
	checkErrorCondition(err)
	defer listener.Close()

	// display this node's ID based on SHA-1 hash value
	fmt.Printf("Chord Node ID: %d\n", chord.GetChordID(conf.IpAddress+":"+conf.Port))

	// bootstrap, first node with port number 7001 creates the ring, and the rest join
	if conf.Port == "7001" {
		chord.Create(conf.IpAddress, conf.Port)
		fmt.Println("Finger Table: ", chord.FingerTable)
	} else {
		// contact CreatedNode and pass in my own chord ID
		// todo - this hard-coded stuff should really be in the config file
		duration, _ := time.ParseDuration("3s")
		for chord.Join("127.0.0.1", "7001", conf.IpAddress, conf.Port) != nil {
			time.Sleep(duration)
		}
	}

	fmt.Printf("Listening on port " + conf.Port + " ...\n")

	go periodicallyStabilize()
	go chord.FixFingers()
	go purge()

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go jsonrpc.ServeConn(conn)

	}
}

func purge() {
	duration, _ := time.ParseDuration("300s")
	for {
		time.Sleep(duration)
		
		
		
		//Get purge time from configuration file
		purge_time := conf.Purge
		
		fmt.Println("Purge called. Checking if entries have been accessed since: ", purge_time)
		
		for krp, val := range dict {
		accessed_time := val.Accessed
		accessTime, err := time.Parse(longForm, accessed_time)
		if err != nil {
			fmt.Println("Time format is wrong or access time is nil", err)
		}
		purgeTime, err := time.Parse(longForm, purge_time)
		if err != nil {
			fmt.Println("Time format is wrong", err)
		}
		
		//If krp has not been accessed sonce some user specified time aka purge time then delete
		if !accessTime.After(purgeTime){
			if p := val.Permission; p == "R" {
				delete(dict, krp)
				fmt.Println(" ... Removing triplet from DICT3 and writing to disk.")
				writeDictToDisk()
			}
			fmt.Println("Can't purge Read only")
		}
		}
		}//loop forever
}

func periodicallyStabilize() {
	// todo - this, and other methods, should probably be using RWLock.
	duration, _ := time.ParseDuration("2s")
	for {
		time.Sleep(duration)
		chord.Stabilize()

		fmt.Println("periodicallyStabilize(), predecess:", chord.Predecessor)
		fmt.Println("periodicallyStabilize(), myself   :", chord.FingerTable[0])
		fmt.Println("periodicallyStabilize(), successor:", chord.FingerTable[1])
	}
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
	fmt.Println("   Purge: ", conf.Purge)
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

func GetBytes(key interface{}) ([]byte, error) {
    var buf bytes.Buffer
    enc := gob.NewEncoder(&buf)
    err := enc.Encode(key)
    if err != nil {
        return nil, err
    }
    return buf.Bytes(), nil
}
