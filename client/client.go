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
	fmt.Println(` -specify Requested.[method name] in the "method" field`)
	fmt.Println(` -pass arguments in the "params" field "by-name"`)
	fmt.Println(`Example: {"method":"Requested.Insert", "params":[{ "Key":"key1", "Rel":"relA", "Val":{"content":{"someJSONobject1":1}, "permission":"RW"} }]}`)
	fmt.Println(`{"method":"Requested.Delete", "params":[{ "Key":"key1", "Rel":"relA"}]}`)
	fmt.Println(`{"method":"Requested.InsertOrUpdate", "params":[{ "Key":"key1", "Rel":"relA", "Val":{"content":{"someJSONobject1":1}, "permission":"RW"} }]}`)
	fmt.Println(`{"method":"Requested.ListKeys","params":[{}]}`)
	fmt.Println(`{"method":"Requested.ListIDs","params":[{}]}`)
	fmt.Println(`{"method":"Requested.Shutdown","params":[{}]}`)
	fmt.Println(`{"method":"Requested.Lookup","params":[{ "Key":"key1", "Rel":"relA"}]}`)
	fmt.Println(`{"method":"Requested.Lookup","params":[{ "Key":"key1", "Rel":" "}]}`)
	fmt.Println(`{"method":"Requested.Lookup","params":[{ "Key":" ", "Rel":"relA"}]}`)
	// For Mark's convenience when copying and pasting
	// fmt.Println(`{"method":"Requested.NaiveLookup","params":[{ "Key":"key1", "Rel":"relA"}]}`)
	// fmt.Println(`{"method":"Requested.NaiveLookup","params":[{ "Key":"key1", "Rel":" "}]}`)
	// fmt.Println(`{"method":"Requested.NaiveLookup","params":[{ "Key":" ", "Rel":"relA"}]}`)
	fmt.Println(`{"method":"Requested.DetermineIfNetworkIsStable","params":[{}]}`)
	fmt.Println(`{"method":"Requested.DetermineNetworkStructure","params":[{}]}`)
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
