#!/bin/bash
PROJPATH=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)

if [[ $# != 3 ]]; then
    echo Usage: $0 "<out_mode> <test_dir> <num_instances>"
    echo out_mode must be \"gnome-terminal\", \"tmux\", or \"null\"
    exit 1
fi

out_mode=$1
test_dir=$2
num_instances=$3

cd $PROJPATH
./build.sh && \
./config_gen.sh $test_dir $test_dir $num_instances && \
./spawn.sh $out_mode $test_dir
