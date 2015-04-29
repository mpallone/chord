#!/bin/bash

rm dict3_15.gob

gnome-terminal -e "${GOPATH}/bin/node 15.config" --window-with-profile=HOLD_OPEN --title=JOINING_NODE
