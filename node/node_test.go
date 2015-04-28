package main

import (
	"fmt"
	"github.com/robcs621/proj2/chord"
	"math/big"
	"strconv"
	"testing"
	"time"
)

func TestKeyLocation(t *testing.T) {
	const numNodes = 4
	const numEntries = 512
	const fingerTableSize = 9

	//Make some test data
	entries := make([]string, numEntries)
	for i := 0; i < numEntries; i++ {
		entries[i] = "This is entry number " + strconv.Itoa(i)
	}

	//Make some keys
	keys := make([]*big.Int, numEntries)
	for i := 0; i < numEntries; i++ {
		keys[i] = chord.GetChordID(entries[i])
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
		fmt.Println(conf.Port)

		n := new(Node)
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
	fmt.Println("Stabilizing...")
	for !stable {
		time.Sleep(8 * time.Second)
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
	fmt.Println("Stable")

	//Test

	//Kill
	for i := 0; i < numNodes; i++ {
		nodes[i].Shutdown(nil, nil)
	}
}
