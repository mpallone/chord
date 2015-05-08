#!/bin/bash
PROJPATH=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)
BIND=127.0.0.1
BASE_PORT=7000

EXISTING_NODE_PORT=7001

if [[ $# != 4 ]]; then
    echo Usage: $0 "<config_dir> <store_dir> <num_instances>"
    exit 1
fi

config_dir=$1
store_dir=$2
num_instances=$3
purge_seconds=$4

rm -r $config_dir
mkdir -p $config_dir $stor_dir 2>/dev/null
pushd $store_dir; store_dir=`pwd`; popd
for ((i=1; i<=$num_instances; i++)); do
    let PORT=$BASE_PORT+$i
    echo Configuring $BIND:$PORT
	str='{\n
	\t"serverID"  : "'${BIND}:$PORT'",\n
	\t"protocol"  : "tcp",\n
	\t"ipAddress" : "'$BIND'",\n
	\t"port"	    : "'$PORT'",\n
	\t"persistentStorageContainer" : {"file" : "'$store_dir/dict3_${i}.gob'"},\n
	\t"methods"   : ["lookup", "insert", "insertOrUpdate", "delete", "listKeys", "listIDs", "shutdown"],\n
	\t"PurgeSeconds" : "'$purge_seconds'"' 

	# node on port 7001 creates the ring if 'join' field is not set
	if [[ $i == 1 ]]; then
		str+='\n}'

	# all others join the ring through the node on port 7001
	else
		str+=',\n\t"join" : {"ipAddress" : "'${BIND}'", "port" : "'${EXISTING_NODE_PORT}'"}\n'
		str+='}'
	fi

	echo -e ${str} > $config_dir/${i}.config
done
cp $PROJPATH/client/client.config $config_dir/client.cfg

