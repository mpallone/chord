#!/bin/bash

START=1
NUM_TEST_NODES_TO_CREATE=9
REAL_NODE_FILE=node/node.go
TEST_NODE_DIR=test_node

echo "Deleting old test nodes..."
rm -rf "${TEST_NODE_DIR}"*

echo "Creating ${NUM_TEST_NODES_TO_CREATE} new test nodes..."

for ((node_num=$START; node_num<=$NUM_TEST_NODES_TO_CREATE; node_num++))
do
	echo "Building new ${TEST_NODE_DIR}${node_num}"
	
	mkdir ${TEST_NODE_DIR}${node_num}

	# relative path for .go and .config
	test_node_file_path=${TEST_NODE_DIR}${node_num}/${TEST_NODE_DIR}${node_num}

	# copy real node.go file from Git into test directory and rename it
	cp ${REAL_NODE_FILE} ${test_node_file_path}.go

	# write out config file to test directory
	echo '{
	"serverID"  : "'${TEST_NODE_DIR}${node_num}'",
	"protocol"  : "tcp",
	"ipAddress" : "127.0.0.1",
	"port"	    : "700'${node_num}'",
	"persistentStorageContainer" : {"file" : "dict3.gob"},
	"methods"   : ["lookup", "insert", "insertOrUpdate", "delete", "listKeys", "listIDs", "shutdown"] 
}'  >> ${test_node_file_path}.config

	# write run script
	test_node_run_script_file_path=${TEST_NODE_DIR}${node_num}/run_${TEST_NODE_DIR}${node_num}.sh

	echo '#!/bin/bash 
go run '${TEST_NODE_DIR}${node_num}.go' '${TEST_NODE_DIR}${node_num}.config'' >> ${test_node_run_script_file_path}

	# set permissions to execute script
	chmod u+x ${test_node_run_script_file_path}

done



