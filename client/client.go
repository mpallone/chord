/* CMSC-621
Project 2
client.go
*/

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
)

// client configuration object read in
var conf ClientConfiguration

type ClientConfiguration struct {
	ServerID  string
	Protocol  string
	IpAddress string
	Port      string
	Methods   []string
}
type RPCCall struct {
	Method string
	Params []interface{}
	Id     int64
}

func main() {

	if len(os.Args) != 2 {
		fmt.Println("Usage: ", os.Args[0], "Path to client configuration file")
		os.Exit(1)
	}
	infile := os.Args[1]

	// read in configuration file
	fmt.Println("Loading client configuration...")
	parseConfigurationFile(infile)

	// connect to remote server
	fmt.Print("Establishing connection to ", conf.IpAddress, ":", conf.Port, " ... ")
	client, err := jsonrpc.Dial(conf.Protocol, (conf.IpAddress + ":" + conf.Port))
	checkErrorCondition(err)
	fmt.Println("Success!")

	// prompt for user input (assume properly formatted JSON message, with params "by-name")
	var inputStr string
	fmt.Println("===============================================================================================================")
	fmt.Println("JSON messages entered must:")
	fmt.Println(` -specify Node.[method name] in the "method" field`)
	fmt.Println(` -pass arguments in the "params" field "by-name"`)
	fmt.Println(`Example: {"method":"Node.Insert","params":[{ "Key":"keyA", "Rel":"relA", "Val":{"a":5, "b":6} }], "id":85}`)
	fmt.Println(`{"method":"Node.Shutdown","params":[], "id":95}`) // todo remove, this is just for convenience
	fmt.Println(`{"method":"Node.Lookup","params":[{ "Key":"key0", "Rel":"relA"}], "id":85}`)
	fmt.Println(`{"method":"Node.Lookup","params":[{ "Key":"key0", "Rel":" "}], "id":85}`)
	fmt.Println(`{"method":"Node.Lookup","params":[{ "Key":" ", "Rel":"relA"}], "id":85}`)
	fmt.Println(`{"method":"Node.DetermineNetworkStructure","params":[{}], "id":85}`)
	fmt.Println("===============================================================================================================")
	rdr := bufio.NewReader(os.Stdin)

	//Spawn the response reader
	replies := make(chan *rpc.Call, 1024)
	go readResponses(replies)
	for {
		fmt.Println("Please enter a properly formatted JSON message: ")

		inputStr, err = rdr.ReadString('\n')
		checkErrorCondition(err)

		// encode message as a json object
		var query RPCCall
		err = json.Unmarshal([]byte(inputStr), &query)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		if len(query.Params) < 1 {
			fmt.Println("Params must be a single-element containing a JSON construct")
			continue
		}

		// send JSON message to node
		client.Go(query.Method, query.Params[0], new(interface{}), replies)
		checkErrorCondition(err)
	}
}

func readResponses(replies chan *rpc.Call) {
	for {
		reply := <-replies
		bytes, _ := json.Marshal(reply.Reply)
		fmt.Println("JSON message received:\n",
			"Method >", reply.ServiceMethod, "\n",
			"Args   >", reply.Args, "\n",
			"Reply  >", string(bytes), "\n",
			"Error  >", reply.Error, "\n")
	}
}

func parseConfigurationFile(infile string) {
	// read in client configuration
	configurationFile, err := ioutil.ReadFile(infile)
	checkErrorCondition(err)

	// parse JSON in client configuration file
	err = json.Unmarshal(configurationFile, &conf)
	checkErrorCondition(err)

	fmt.Println("   Protocol: ", conf.Protocol)
	fmt.Println("   IP Address: ", conf.IpAddress)
	fmt.Println("   Port: ", conf.Port)
}

func checkErrorCondition(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
