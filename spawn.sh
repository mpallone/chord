#!/bin/bash

if [[ $# < 3 ]]; then
    echo Usage: $0 "<out_mode> <config_dir> <client_messages> [instancenum.config instancenum.config...]"
    echo out_mode must be \"gnome-terminal\", \"tmux\", \"log\", or \"null\"
    exit 1
fi

out_mode=$1
config_dir=$2
client_msgs=`readlink -f $3`
shift
shift
shift

log=/tmp/bestchordever.log
echo > $log

#Figure out which configs to use
cd $config_dir
if [[ $# == 0 ]]; then
    configs=`ls *.config`
else
    configs="$@"
fi

set -m
if [[ $out_mode == tmux ]]; then tmux new-session -d 'bash'; fi
for c in $configs; do
    echo Loading $c
    case $out_mode in
        null)
            ${GOPATH}/bin/node $c &>/dev/null&
            ;;
    	log)
            echo Logging to $log
	    ${GOPATH}/bin/node $c 2>&1 | sed "s,^,$c: ,g" --unbuffered >> $log&
            ;;
        tmux)
            tmux new-window -n $c "${GOPATH}/bin/node $c; cat -"
            ;;
        gnome-terminal)
	    gnome-terminal  -e "${GOPATH}/bin/node $c" --window-with-profile=HOLD_OPEN --title="$c"
            ;;
    esac
done
set +m

# pause for a few seconds until all servers are up and listening
sleep 3

echo "Starting client"
case $out_mode in
    log)
        cat - $client_msgs - | ${GOPATH}/bin/client client.cfg
        ;;
    null)
        cat - $client_msgs - | ${GOPATH}/bin/client client.cfg
        ;;
    tmux)
        tmux new-window -n "CLIENT" "echo Press CTL-D to send; cat - $client_msgs - | ${GOPATH}/bin/client client.cfg;"
        tmux -2 attach-session -d
        ;;
    gnome-terminal)
	    gnome-terminal -x bash -c "echo Press CTL-D to send; cat - $client_msgs - | ${GOPATH}/bin/client client.cfg" --window-with-profile=HOLD_OPEN --title="CLIENT"
        ;;
esac
