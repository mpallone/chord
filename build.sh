#!/bin/bash
mkdir $GOPATH/{bin,lib} 2>/dev/null

pkgs="
    github.com/robcs621/proj2/chord
    github.com/robcs621/proj2/client
    github.com/robcs621/proj2/node
"
for p in $pkgs; do
    echo $p
    go fmt $p && \
    go test $p && \
    go install $p || exit
done
