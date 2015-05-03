#!/bin/bash
PROJPATH=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)
BIND=127.0.0.1
BASE_PORT=7000
PURGE_TIME="3/20/2015, 18:09:54"

if [[ $# != 3 ]]; then
    echo Usage: $0 "<config_dir> <store_dir> <num_instances>"
    exit 1
fi

config_dir=$1
store_dir=$2
num_instances=$3

rm -r $config_dir
mkdir -p $config_dir $stor_dir 2>/dev/null
pushd $store_dir; store_dir=`pwd`; popd
for ((i=1; i<=$num_instances; i++)); do
    let PORT=$BASE_PORT+$i
    echo Configuring $BIND:$PORT
	echo '{
	"serverID"  : "'${BIND}:$PORT'",
	"protocol"  : "tcp",
	"ipAddress" : "'$BIND'",
	"port"	    : "'$PORT'",
	"persistentStorageContainer" : {"file" : "'$store_dir/dict3_${i}.gob'"},
	"methods"   : ["lookup", "insert", "insertOrUpdate", "delete", "listKeys", "listIDs", "shutdown"], 
	"purge"     : "'$PURGE_TIME'"
}'  > $config_dir/${i}.config
done
cp $PROJPATH/client/client.config $config_dir/client.cfg
