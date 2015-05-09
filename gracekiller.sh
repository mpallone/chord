#!/bin/bash
for i in `ps -fA | grep node | awk '{print $2}'`; do ps -f --pid $i | grep node; kill -INT $i 2>/dev/null; sleep 1; done
