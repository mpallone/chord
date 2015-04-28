package main

import (
	"github.com/robcs621/proj2/chord"
	"math/big"
	"testing"
	"time"
	"fmt"
	"strconv"
)

func TestKeyLocation(t *testing.T) {
	numNodes := 1
	numEntries := 512

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
	stable := true
	for !stable {
		time.Sleep(time.Second)
	}

	//Test

	//Kill
	for i := 0; i < numNodes; i++ {
		nodes[i].Shutdown(nil, nil)
	}
}
