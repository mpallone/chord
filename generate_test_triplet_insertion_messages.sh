#!/bin/bash

if [[ $# != 3 ]]; then
	echo Usage: $0 "<test_data_filename> <num_test_messages> <R or RW>"
	exit 1
fi

TESTDATA_FILE=$1
NUM_TRIPLETS=$2
READ_OR_READWRITE=$3

BEFORE_KEY='{"method":"Requested.Insert", "params":[{ "Key":"'
MIDDLE_KEY1='", "Rel":"relA", "Val":{"content":{"someJSONobject'
MIDDLE_KEY2='":'
MIDDLE_KEY3='}, "permission":"'
AFTER_KEY='"} }] }'

msgs=""

rm ${TESTDATA_FILE} 2>/dev/null

for ((i=1; i<=${NUM_TRIPLETS}; i++)); do
	msgs=${BEFORE_KEY}
	msgs+=key$i
	msgs+=${MIDDLE_KEY1}
	msgs+=$i
	msgs+=${MIDDLE_KEY2}
	msgs+=$i
	msgs+=${MIDDLE_KEY3}
	msgs+=$READ_OR_READWRITE
	msgs+=$AFTER_KEY
	echo "${msgs}" >> ${TESTDATA_FILE} 
done
echo -e "\n" >> ${TESTDATA_FILE}
