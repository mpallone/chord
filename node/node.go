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
)

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
	Val TripVal // only used for insert and insertOrUpdate,
}

type ChordIDArgs struct {
	Id *big.Int
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
type FindSuccessorReply struct {
	ChordNodePtr chord.ChordNodePtr
}

// global variable
var dict = map[KeyRelPair]TripVal{}

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

	fmt.Print("  Insert:      ", args.Key, ", ", args.Rel, ", ", args.Val)

	// construct temp KeyRelPair
	krp := KeyRelPair{args.Key, args.Rel}

	// add key-rel pair if does not exist in dict
	if _, exists := dict[krp]; !exists {
		dict[krp] = args.Val
		reply.TripletInserted = true // default is false
		fmt.Println(" ... Does not exist in DICT3. Writing new triplet to disk.")
		writeDictToDisk()
	} else {
		fmt.Println(" ... Triplet already exists in DICT3.")
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
func (t *Node) Delete(args *Args, reply *string) error {

	fmt.Print("  Delete:     ", args.Key, ", ", args.Rel)

	// construct temp KeyRelPair
	krp := KeyRelPair{args.Key, args.Rel}
	delete(dict, krp)
	fmt.Println(" ... Removing deleted triplet from DICT3 and writing to disk.")
	writeDictToDisk()
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
func (t *Node) FindSuccessor(args *ChordIDArgs, reply *FindSuccessorReply) error {
	fmt.Println("FindSuccessor wrapper called with id: ", args.Id)

	reply.ChordNodePtr = chord.FindSuccessor(args.Id)

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
		//chord.Join("127.0.0.1", "7001", chord.GetChordID(conf.IpAddress+":"+conf.Port))
		// todo - this hard-coded stuff should really be in the config file 
		chord.Join("127.0.0.1", "7001", conf.IpAddress, conf.Port)
	}

	fmt.Printf("Listening on port " + conf.Port + " ...\n")

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go jsonrpc.ServeConn(conn)
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
