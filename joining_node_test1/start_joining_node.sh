#!/bin/bash

rm dict3_13.gob

gnome-terminal -e "${GOPATH}/bin/node 13.config" --window-with-profile=HOLD_OPEN --title=JOINING_NODE
