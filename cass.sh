#!/bin/bash
iface="ib0" # still unused, it will be received as argument and change the seeds and interface used in conf/cassandra.yaml

JOBNAME=${1}
DC=${2}
N_NODES=${3}

CASS_HOME=$HOME/cassandra-dc
SEEDS_FILE_DC1=seeds/$JOBNAME-1.txt
SEEDS_FILE_DC2=seeds/$JOBNAME-2.txt

DATA_HOME=/tmp/cassandra-data-tmp-e
COMM_HOME=/tmp/cassandra-commitlog
HOST_LIST=/tmp/cassandra-host-list.txt
N_NODES_FILE=cassandra-num-nodes.txt
SNAPSHOT_FILE=cassandra-snapshot-file.txt
RECOVER_FILE=cassandra-recover-file.txt
RETRY_MAX=20

function exit_killjob () {
    # Traditional harakiri
    bkill $(bjobs | grep cassandra | awk '{ print $1 }')
}

function exit_bad_node_status () {
    # Exit after getting a bad node status. 
    echo "Cassandra Cluster Status: ERROR"
    echo "It was expected to find ""$(cat $N_NODES_FILE)"" UP nodes, found "$NODE_COUNTER"."
    echo "Exiting..."
    exit_killjob
}

function get_nodes_up () {
    NODE_STATE_LIST=`$CASS_HOME/bin/nodetool status | sed 1,5d | sed '$ d' | awk '{ print $1 }'`
    if [ "$NODE_STATE_LIST" != "" ]
    then
        NODE_COUNTER=0
        for state in $NODE_STATE_LIST
        do  
            if [ $state == "UN" ]
            then
                NODE_COUNTER=$(($NODE_COUNTER+1))
            fi  
        done
    fi 
}

if [ ! -f $CASS_HOME/bin/cassandra ]; then
    echo "ERROR: Cassandra executable is not placed where it was expected. ($CASS_HOME/bin/cassandra)"
    echo "Exiting..."
    exit
fi

echo "STARTING UP CASSANDRA..."
echo "I am $(hostname)."
module load java/openjdk/1.8-rhel7
#export REPLICA_FACTOR=2

echo $LSB_HOSTS
echo "Non repeated list..."
prev=""
for host in $LSB_HOSTS
do
    if [ "$host" != "$prev" ]
    then
        prev=$host
        hostlist=$hostlist" "$host
    fi  
done
echo $hostlist > seeds/$JOBNAME-$DC.txt
# Wait 20 seconds to get the seeds, else, timeout
sleep 20

if [ ! -f $SEEDS_FILE_DC1 ] || [ ! -f $SEEDS_FILE_DC2 ]; then
    echo "ERROR: A seeds file is missing. Aborting."
    exit
else
    echo "Seeds files OK."
fi
HOST_LIST="$(cat $SEEDS_FILE_DC1) $(cat $SEEDS_FILE_DC2)"

seeds=`echo $HOST_LIST | sed "s/ /-ib0,/g"`
seeds=$seeds-$iface #using only infiniband atm, will change later
cat $CASS_HOME/conf/cassandra-cfg.yaml | sed "s/.*seeds:.*/          - seeds: \"$seeds\"/" > $CASS_HOME/conf/cassandra-aux.yaml
cat $CASS_HOME/conf/cassandra-aux.yaml | sed "s/.*initial_token:.*/#initial_token:/" > $CASS_HOME/conf/cassandra-aux2.yaml
mv $CASS_HOME/conf/cassandra-aux2.yaml $CASS_HOME/conf/cassandra-cfg.yaml


echo "Launching Datacenter$DC in the following hosts: $hostlist"

NODE_COUNT=0
for u_host in $hostlist
do
    ((NODE_COUNT++))
    # Clearing data from previous executions and checking symlink coherence
    blaunch $u_host "bash $HOME/cassandraDC4juron/dc-set.sh $DC $JOBNAME $CASS_HOME $NODE_COUNT"

    #if [ "$(cat $RECOVER_FILE)" != "" ]
    #then
    #    # Moving data to each datapath
    #    blaunch $u_host "$HOME/cassandra4juron/recover.sh $NODE_COUNT"
    #fi       

    # Launching Cassandra in every node
    echo "Launching in: $u_host"
    blaunch $u_host "$CASS_HOME/bin/cassandra -f" &
    sleep 1
done

# Checking cluster status until all nodes are UP (or timeout)
echo "Waiting 20 seconds until all Cassandra nodes are launched..."
sleep 20
echo "Checking..."
RETRY_COUNTER=0
get_nodes_up
while [ "$NODE_COUNTER" != "$N_NODES" ] && [ $RETRY_COUNTER -lt $RETRY_MAX ]; do
    echo "Retry #$RETRY_COUNTER"
    echo "Checking..."
    sleep 10
    get_nodes_up
    ((RETRY_COUNTER++))
done
if [ "$NODE_COUNTER" == "$N_NODES" ]
then
    echo "Cassandra Cluster with "$N_NODES" nodes started successfully."
else
    echo "ERROR: Cassandra Cluster RUN timeout. Check STATUS."
    exit_bad_node_status
fi

# THIS IS THE APPLICATION CODE EXECUTING SOME TASKS USING CASSANDRA DATA, ETC
echo "CHECKING CASSANDRA STATUS: "
$CASS_HOME/bin/nodetool status

#sleep 12
#firstnode=$(echo $hostlist | awk '{ print $1 }')
#echo "INSERTING DATA FROM: "$firstnode
#blaunch $firstnode "$CASS_HOME/bin/cqlsh -f $HOME/1dot25GB.cql 127.0.0.1 9042"
# END OF THE APPLICATION EXECUTION CODE

# Wait for a couple of minutes to assure that the data is stored
sleep 60

# Don't continue until the status is stable
#while [ "$NDT_STATUS" != "$($CASS_HOME/bin/nodetool status)" ]
#do
#    NDT_STATUS=$($CASS_HOME/bin/nodetool status)
#    sleep 60
#done

# If an snapshot was ordered, it is done
#if [ "$(cat $SNAPSHOT_FILE)" == "1" ]
#then
#    #BUCLE SOBRE LOS HOSTS
#    for u_host in $hostlist
#    do
#        blaunch $u_host "bash snapshot.sh $$"
#    done
#fi

sleep 300

# Kills the job to shutdown every cassandra service
#exit_killjob
