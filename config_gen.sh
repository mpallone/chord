#!/bin/bash

if [[ $# != 3 ]]; then
    echo Usage: $0 "<config_dir> <store_dir> <num_instances>"
    exit 1
fi

config_dir=$1
store_dir=$2
num_instances=$3

mkdir -p $1 2>/dev/null
for ((i=1; i<=$num_instances; i++)); do
	echo '{
	"serverID"  : "'127.0.0.1:700$i'",
	"protocol"  : "tcp",
	"ipAddress" : "127.0.0.1",
	"port"	    : "700'$i'",
	"persistentStorageContainer" : {"file" : "'$store_dir/dict3_${i}.gob'"},
	"methods"   : ["lookup", "insert", "insertOrUpdate", "delete", "listKeys", "listIDs", "shutdown"] 
}'  > $config_dir/700${i}.config
done
