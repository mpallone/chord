package main

import (
	"fmt"
	"github.com/robcs621/proj2/chord"
	"sort"
	"strconv"
	"testing"
	"time"
)

func TestKeyLocation(t *testing.T) {
	//const numNodes = 20 //TODO: 21 nodes = collision
	const numNodes = 3 //TODO: 21 nodes = collision
	const numEntries = 512
	const fingerTableSize = 9
	const duration = 100 * time.Millisecond

	//Make some test data
	entries := make([]Args, numEntries)
	for i := 0; i < numEntries; i++ {
		var a = new(Args)
		a.Key = "This is entry number"
		a.Rel = TripRel(strconv.Itoa(i))
		a.Val = TripVal{"blah": "blaher"}
		entries[i] = *a
	}

	//Make some keys
	keys := make([]int, numEntries)
	for i := 0; i < numEntries; i++ {
		id, _ := strconv.Atoi(chord.GetChordID(string(entries[i].Key) + string(entries[i].Rel)).String())
		keys[i] = id
	}

	//Make some nodes
	nodes := make([]*Node, numNodes)
	for i := numNodes - 1; i >= 0; i-- {
		var conf ServerConfiguration
		conf.Protocol = "tcp"
		conf.IpAddress = "127.0.0.1"
		conf.Port = strconv.Itoa(7001 + i)
		conf.PersistentStorageContainer.File = "/dev/null"
		conf.ServerID = conf.IpAddress + ":" + conf.Port
		conf.Methods = []string{"lookup", "insert", "insertOrUpdate", "delete", "listKeys", "listIDs", "shutdown"}
		fmt.Println("node_test.go: Config ", conf)

		n := new(Node)
		n.stabilizeDuration = duration
		nodes[i] = n
		go n.run(conf)
		for n.chord == nil {
			time.Sleep(duration)
		}
		time.Sleep(123 * time.Millisecond)
	}
	time.Sleep(10 * (fingerTableSize) * duration)
return

	//Wait for a stable network
	stable := false
	lastState := make([][]*chord.ChordNodePtr, numNodes)
	for i := 0; i < numNodes; i++ {
		lastState[i] = make([]*chord.ChordNodePtr, fingerTableSize)
		for j := 0; j < fingerTableSize; j++ {
			if nodes[i].chord.FingerTable[j] == nil{
				j--
				continue
			}
			lastState[i][j] = new(chord.ChordNodePtr)
			*lastState[i][j] = *nodes[i].chord.FingerTable[j]
		}
	}
	fmt.Println("node_test.go: Stabilizing...")
	for !stable {
		time.Sleep((fingerTableSize) * duration)
		stable = true
		for i := 0; i < numNodes; i++ {
			for j := 0; j < fingerTableSize; j++ {
				if *lastState[i][j] != *nodes[i].chord.FingerTable[j] {
					stable = false
					break
				}
			}
			for j := 0; j < fingerTableSize; j++ {
				*lastState[i][j] = *nodes[i].chord.FingerTable[j]
			}
		}
	}
	fmt.Println("node_test.go: Stable")

	//Identify nodes
	nodeIds := make([]int, numNodes)
	for i := 0; i < numNodes; i++ {
		id, _ := strconv.Atoi(nodes[i].chord.FingerTable[chord.SELF].ChordID.String())
		nodeIds[i] = id
	}
	sort.Ints(nodeIds)

	//Check for duplicate IDs
	lastId := 65535
	for i := 0; i < numNodes; i++ {
		if lastId == nodeIds[i] {
			t.Errorf("Duplicate Node ID detected: " + strconv.Itoa(lastId))
		}
		lastId = nodeIds[i]
	}

	//Do the mappings
	for e := 0; e < numEntries; e++ {
		fmt.Println("node_test.go: insert (" + string(entries[e].Key) + ", " + string(entries[e].Rel) + ")")

		//Fake map
		var fakeLoc int
		if keys[e] <= nodeIds[0] || keys[e] > nodeIds[numNodes-1] {
			fakeLoc = nodeIds[0]
		} else {
			for i := 1; i < numNodes-1; i++ {
				if keys[e] < nodeIds[i] {
					fakeLoc = nodeIds[i]
					break
				}
			}
		}

		//Real insert and location check
		reply := new(InsertReply)
		nodes[0].Insert(&entries[e], reply)
		for i := 0; i < numNodes; i++ {
			id, _ := strconv.Atoi(nodes[i].chord.FingerTable[chord.SELF].ChordID.String())
			if id == fakeLoc {
				krp := KeyRelPair{entries[e].Key, entries[e].Rel}
				if _, exists := nodes[i].dict[krp]; !exists {
					t.Errorf("KRP (", krp, ") not inserted in expected node: "+strconv.Itoa(id))
				}
				break
			}
		}
	}

	//Kill
	for i := 0; i < numNodes; i++ {
		nodes[i].Shutdown(nil, nil)
	}
}
