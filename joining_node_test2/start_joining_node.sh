#!/bin/bash

rm dict3_14.gob

gnome-terminal -e "${GOPATH}/bin/node 14.config" --window-with-profile=HOLD_OPEN --title=JOINING_NODE
