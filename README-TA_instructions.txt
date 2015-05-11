Go installation and environment setup 
[same as https://golang.org/doc/install]

tar -C /usr/local -xzf go1.4.2.linux-amd64.tar.gz

add the following lines at the end of .bashrc to set environment variables:
	export GOPATH=$HOME/go
	export PATH=$PATH:/usr/local/go/bin
	export PATH=$PATH:$GOPATH/bin

mkdir -p $HOME/go/src/github.com/robcs621/

# $PROJ2 = name of submitted project file
tar -C $HOME/go/src/github.com/robcs621 -xzvf $PROJ2.tar.gz

cd $HOME/go/src/github.com/robcs621/proj2/

==============================================================================

I. LOG MODE (for large networks with output to log file)
-100 nodes w/ 1000 triplets inserted; all nodes then gracefully exit and transfer keys

# STEP 1)
# Compile, generate configuration files, and execute CHORD ring with 100 nodes, 1000 keys, and purge time of 600 seconds
CHORD@/home/vm2/go/src/github.com/robcs621/proj2 ~] ./tester.sh log testdir insert_1000_triplet_messages.txt 100 600

# STEP 2)
# open new terminal window to observe the log file
CHORD@/home/vm2/go/src/github.com/robcs621/proj2 ~] tail -f /tmp/bestchordever.log

# STEP 3)
# Wait at least a minute for the network to stabilize
# In the client window, send the following JSON message to verify the network has stabilized:
{"method":"Requested.DetermineIfNetworkIsStable","params":[{}]}

# This RPC call may take 15-20 seconds to complete
# Expected result is as follows (if "NetworkIsStable" is 'false', resend JSON message until 'true'):
JSON message received:
 Method > Requested.DetermineIfNetworkIsStable 
 Args   > map[] 
 Reply  > {"NetworkIsStable":true} 
 Error  > <nil> 

# STEP 4)
# In the client window, enter Ctrl-D to redirect the auto-generated 1000 triplets into the CHORD ring
# (do NOT close the client window)

# STEP 5) 
# Open a new terminal window and gracefully shutdown all of the nodes
CHORD@/home/vm2/go/src/github.com/robcs621/proj2 ~] ./gracekiller.sh

# STEP 6)
# Observe in log file, all 1000 triplets have been transferred to the last remaining node

------------------------------------------------------------------------------

II. INTERACTIVE GNOME-TERMINAL MODE (for small networks with node output to individual terminal windows)
-5 nodes w/ 100 triplets inserted, followed by 3 nodes joining; all nodes then gracefully exit and transfer keys

# STEP 1)
Create the following Profile in gnome-terminal:
Terminal -> Edit -> Profiles...
Click "New"
Profile name: HOLD_OPEN (leave 'Base on:' set as 'Default')
In the "Title and Command" tab: Set the 'When command exits:' to "Hold the terminal open"
Save this profile

# STEP 2)
# Compile, generate configuration files, and execute CHORD ring with 5 nodes, 100 keys, and purge time of 600 seconds
CHORD@/home/vm2/go/src/github.com/robcs621/proj2 ~] ./tester.sh gnome-terminal testdir insert_100_triplet_messages.txt 5 600

# STEP 3)
# Wait several seconds for the network to stabilize
# In the client window, send the following JSON message to determine if the network has stabilized:
{"method":"Requested.DetermineIfNetworkIsStable","params":[{}]}

# Expected result is as follows (if "NetworkIsStable" is 'false', resend JSON message until 'true'):
JSON message received:
 Method > Requested.DetermineIfNetworkIsStable 
 Args   > map[] 
 Reply  > {"NetworkIsStable":true} 
 Error  > <nil> 

# STEP 4)
# In the client window, enter Ctrl-D to redirect the auto-generated 100 triplets into the CHORD ring
# The total number of keys displayed each of the 5 windows should sum to 100

# STEP 5)
# In a new terminal window, join 3 nodes to the network and observe transfer of keys
CHORD@/home/vm2/go/src/github.com/robcs621/proj2 ~] cd joining_node_test1/
CHORD@/home/vm2/go/src/github.com/robcs621/proj2/joining_node_test1 ~] ./start_joining_node.sh 

CHORD@/home/vm2/go/src/github.com/robcs621/proj2/joining_node_test1 ~] cd ../joining_node_test2
CHORD@/home/vm2/go/src/github.com/robcs621/proj2/joining_node_test2 ~] ./start_joining_node.sh 

CHORD@/home/vm2/go/src/github.com/robcs621/proj2/joining_node_test2 ~] cd ../joining_node_test3/
CHORD@/home/vm2/go/src/github.com/robcs621/proj2/joining_node_test3 ~] ./start_joining_node.sh 

# STEP 6)
# Randomly select a node terminal window and enter Ctrl-C to send the shutdown signal (or send the Shutdown() RPC)
# Continue to shutdown all of the nodes, observe the last remaining node has had all 100 keys transferred
