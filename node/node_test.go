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
	const numNodes = 2 //TODO: 21 nodes = collision
	const numEntries = 512
	const fingerTableSize = 9
	const duration = time.Millisecond

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
	for i := 0; i < numNodes; i++ {
		var conf ServerConfiguration
		conf.Protocol = "tcp"
		conf.IpAddress = "127.0.0.1"
		conf.Port = strconv.Itoa(7001 + i)
		conf.PersistentStorageContainer.File = "/dev/null"
		conf.ServerID = conf.IpAddress + ":" + conf.Port

		n := new(Node)
		n.stabilizeDuration = duration
		nodes[i] = n
		go n.run(conf)
	}

	//Wait for a stable network
	stable := false
	lastState := make([][]chord.ChordNodePtr, numNodes)
	for i := 0; i < numNodes; i++ {
		lastState[i] = make([]chord.ChordNodePtr, 9)
		for nodes[i].chord == nil {
			fmt.Print("")
		}
		for len(nodes[i].chord.FingerTable) == 0 {
			fmt.Print("")
		}
		copy(lastState[i], nodes[i].chord.FingerTable[:])
	}
	fmt.Println("node_test.go: Stabilizing...")
	for !stable {
		//fmt.Print("node_test.go: ")
		//fmt.Println(lastState)
		time.Sleep((fingerTableSize + 1) * duration)
		stable = true
		for i := 0; i < numNodes; i++ {
			for itemno, el := range nodes[i].chord.FingerTable {
				if lastState[i][itemno] != el {
					stable = false
				}
			}
			copy(lastState[i], nodes[i].chord.FingerTable[:])
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
				krp := KeyRelPair{entries[i].Key, entries[i].Rel}
				if _, exists := nodes[i].dict[krp]; !exists {
					t.Errorf("KRP not inserted in expected node: " + strconv.Itoa(lastId))
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
