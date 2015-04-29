#!/bin/bash

if [[ $# != 2 ]]; then
	echo Usage: $0 "<test_data_filename> <num_test_messages>"
	exit 1
fi

TESTDATA_FILE=$1
NUM_TRIPLETS=$2

BEFORE_KEY='{"method":"Node.Insert", "params":[{ "Key":"'
MIDDLE_KEY='", "Rel":"relA", "Val":{"a":5, "b":6} }], "id":'
AFTER_KEY='}'

msgs=""

rm ${TESTDATA_FILE} 2>/dev/null

for ((i=1; i<=${NUM_TRIPLETS}; i++)); do
	msgs=${BEFORE_KEY}
	msgs+=key$i
	msgs+=${MIDDLE_KEY}
	msgs+=$i
	msgs+=$AFTER_KEY
	echo "${msgs}" >> ${TESTDATA_FILE} 
done
echo -e "\n" >> ${TESTDATA_FILE}
