#!/bin/bash
PROJPATH=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)

if [[ $# != 5 ]]; then
    echo Usage: $0 "<out_mode> <test_dir> <client_msgs> <num_instances> <purge_seconds>"
    echo out_mode must be \"gnome-terminal\", \"tmux\", or \"null\"
    exit 1
fi

out_mode=$1
test_dir=$2
client_msgs=$3
num_instances=$4
purge_seconds=$5

cd $PROJPATH
./build.sh && \
./config_gen.sh $test_dir $test_dir $num_instances $purge_seconds && \
./spawn.sh $out_mode $test_dir $client_msgs
