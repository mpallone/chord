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
	"net"
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
	conn, err := net.Dial(conf.Protocol, (conf.IpAddress + ":" + conf.Port))
	checkErrorCondition(err)
	fmt.Println("Success!")

	// prompt for user input (assume properly formatted JSON message, with params "by-name")
	var inputStr string
	fmt.Println("===============================================================================================================")
	fmt.Println("JSON messages entered must:")
	fmt.Println(` -specify Node.[method name] in the "method" field`)
	fmt.Println(` -pass arguments in the "params" field "by-name"`)
	fmt.Println(`Example: {"method":"Node.Insert","params":[{ "Key":"keyA", "Rel":"relA", "Val":{"a":5, "b":6} }], "id":85}`)
	fmt.Println("===============================================================================================================")
	rdr := bufio.NewReader(os.Stdin)
	for {
		fmt.Println("Please enter a properly formatted JSON message: ")

		inputStr, err = rdr.ReadString('\n')
		checkErrorCondition(err)

		// send JSON message to node
		fmt.Fprintf(conn, inputStr)
		responseFromServer, err := bufio.NewReader(conn).ReadString('\n')
		checkErrorCondition(err)

		// display JSON response message from server
		fmt.Println("\n")
		fmt.Println("JSON message sent: ")
		fmt.Println(inputStr)
		fmt.Println("JSON message received:")
		fmt.Println(responseFromServer)
		fmt.Println("--------------------------------------------------------------------------------------------------------------")
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
