#!/bin/bash

REPO_DIR=$GOPATH/src/github.com/robcs621/proj2
TEST_NODE_DIR=test_node

START=1
NUM_TEST_NODES_TO_RUN=9

echo "Starting all ${NUM_TEST_NODES_TO_RUN} test nodes..."

if [[ $TMUX == 1 ]]; then tmux new-session -d 'bash'; fi

for ((node_num=$START; node_num<=$NUM_TEST_NODES_TO_RUN; node_num++))
do
	test_node_run_script_file_name=run_${TEST_NODE_DIR}${node_num}.sh

    if [[ $TMUX == 1 ]]; then
        pushd ${REPO_DIR}/${TEST_NODE_DIR}${node_num} &>/dev/null
        tmux new-window -n "${TEST_NODE_DIR}${node_num}" "./${test_node_run_script_file_name}"
        popd &>/dev/null
    else
	    gnome-terminal --working-directory=${REPO_DIR}/${TEST_NODE_DIR}${node_num} -e ./${test_node_run_script_file_name} --window-with-profile=HOLD_OPEN --title="${TEST_NODE_DIR}${node_num}"
    fi

done

# pause for a few seconds until all servers are up and listening
sleep 4

echo "Starting client..."
if [[ $TMUX == 1 ]]; then
    cd ${REPO_DIR}/client
    tmux new-window -n "CLIENT" ./run_client.sh
    tmux -2 attach-session -d
else
    gnome-terminal --working-directory=${REPO_DIR}/client -e ./run_client.sh --window-with-profile=HOLD_OPEN --title="CLIENT"
fi


