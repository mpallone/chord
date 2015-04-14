#!/bin/bash

PROJPATH=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)

cd $PROJPATH
./build.sh && \
./config_gen.sh $PROJPATH/test_files 4 && \
./spawn.sh $PROJPATH/test_files
