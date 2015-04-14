#!/bin/bash

if [[ $# < 2 ]]; then
    echo Usage: $0 "<out_mode> <config_dir> [instancenum.config instancenum.config...]"
    echo out_mode must be \"gnome-terminal\", \"tmux\", or \"null\"
    exit 1
fi

out_mode=$1
config_dir=$2
shift
shift

#Figure out which configs to use
cd $config_dir
if [[ $# == 0 ]]; then
    configs=`ls *.config`
else
    configs="$@"
fi

if [[ $out_mode == tmux ]]; then tmux new-session -d 'bash'; fi
for c in $configs; do
    echo Loading $c
    case $out_mode in
        null)
            node $c &>/dev/null&
            ;;
        tmux)
            tmux new-window -n $c "node $c; cat -"
            ;;
        gnome)
	        gnome-terminal -e "node $c" -window-with-profile=HOLD_OPEN --title="$c"
            ;;
    esac
done

# pause for a few seconds until all servers are up and listening
sleep 4

echo "Starting client"
case $out_mode in
    null)
        client client.cfg
        ;;
    tmux)
        tmux new-window -n "CLIENT" "client client.cfg; cat -"
        tmux -2 attach-session -d
        ;;
    gnome)
	    gnome-terminal -e "client client.cfg" -window-with-profile=HOLD_OPEN --title="CLIENT"
        ;;
esac
