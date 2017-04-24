#!/bin/bash
iface="ib0" # still unused, it will be received as argument and change the seeds and interface used in conf/cassandra.yaml
ulimit -c 0 
JOBNAME=${1}
DC=${2}
N_NODES=${3}
DC2_N_NODES=${4} # If DC=2 this is ignored
CASS_HOME=$HOME/cassandra-dc
SEEDS_FILE_DC1=seeds/$JOBNAME-1.txt
SEEDS_FILE_DC2=seeds/$JOBNAME-2.txt
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

function get_dc_status () {
    NODE_STATE_LIST=`$CASS_HOME/bin/nodetool status | grep ack | awk '{ print $1 }'`
    SWITCH_STATUS="ON"
    if [ "$NODE_STATE_LIST" == "" ]
    then
        echo "ERROR: No status found. The Cassandra Cluster may be still bootstrapping. Try again later."
        exit
    fi  
    NODE_COUNTER_1=0
    NODE_COUNTER_2=0
    for state in $NODE_STATE_LIST
    do  
        if [ "$state" == "--" ]
        then
            if [ "$SWITCH_STATUS" == "ON" ]; then
                SWITCH_STATUS="OFF"
            elif [ "$SWITCH_STATUS" == "OFF" ]; then
                SWITCH_STATUS="ON"
            fi  
        elif [ "$state" != "UN" ]
        then
            echo "E1"
            exit_bad_node_status
        elif [ "$SWITCH_STATUS" == "OFF" ]
        then
            NODE_COUNTER_1=$(($NODE_COUNTER_1+1))
        elif [ "$SWITCH_STATUS" == "ON" ]
        then
            NODE_COUNTER_2=$(($NODE_COUNTER_2+1))
        fi  
    done
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
NODE_COUNTER=0
while [ "$NODE_COUNTER" != "$N_NODES" ] && [ $RETRY_COUNTER -lt $RETRY_MAX ]; do
    echo "Retry #$RETRY_COUNTER"
    echo "Checking DC$DC status..."
    sleep 10
    get_dc_status
    if [ "$DC" == 1 ]; then
        NODE_COUNTER=$NODE_COUNTER_1
    else
        NODE_COUNTER=$NODE_COUNTER_2
    fi
    ((RETRY_COUNTER++))
done
if [ "$NODE_COUNTER" == "$N_NODES" ]
then
    echo "Cassandra Cluster DC$DC with "$N_NODES" nodes started successfully."
else
    echo "ERROR: Cassandra Cluster RUN timeout. Check STATUS."
    exit_bad_node_status
fi

# If the DC=1 checks the status of the whole system and executes the code of the application
if [ "$DC" == "1" ]
then
    RETRY_COUNTER=0
    CLUSTER_READY=""
    while [ "$CLUSTER_READY" != "OK" ] && [ "$RETRY_COUNTER" -lt "$RETRY_MAX" ] 
    do
        echo "Checking status, please wait..."
        sleep 20
        RETRY_COUNTER=$(($RETRY_COUNTER+1))
        CLUSTER_READY=$(bash launcher.sh STATUS $JOBNAME | head -n 1 | awk '{ print $NF }')
    done
    if [ "$CLUSTER_READY" != "OK" ]
    then
        echo "ERROR: Cassandra MultiDC cluster RUN timeout. Check STATUS."
        exit
    else
        echo "Cassandra cluster DC1 with "$N_NODES" nodes and cluster DC2 with "$DC2_N_NODES" nodes started successfully."
    fi

    # THIS IS THE APPLICATION CODE EXECUTING SOME TASKS USING CASSANDRA DATA, ETC
#    echo "CHECKING CASSANDRA STATUS: "
#    $CASS_HOME/bin/nodetool status
#    sleep 12
#    firstnode=$(echo $hostlist | awk '{ print $1 }')
#    echo "INSERTING DATA FROM: "$firstnode
#    blaunch $firstnode "$CASS_HOME/bin/cqlsh -f $HOME/1dot25GB.cql $firstnode-$iface 9042"

echo "CHECKING CASSANDRA STATUS: "
$CASS_HOME/bin/nodetool status

sleep 12
firstnode=$(echo $hostlist | awk '{ print $1 }')
#echo "INSERTING DATA FROM: "$firstnode
#blaunch $firstnode "$CASS_HOME/bin/cqlsh -f $HOME/1dot25GB.cql 127.0.0.1 9042"
#N_TESTS=10
N_TESTS=5
IT_COUNTER=0
#N_OP=10000000
N_OP=1000000
while [ "$IT_COUNTER" -lt "$N_TESTS" ]; do
    if [ "$IT_COUNTER" == "0" ] || [ "$(tail -n 1 stress/$TEST_FILENAME)" == "END" ]; then
        ((IT_COUNTER++))
        TEST_FILENAME=MD_NN6-3_RL3N-1_1M_WR_SN1_"$IT_COUNTER".log
        $CASS_HOME/tools/bin/cassandra-stress write n=$N_OP -schema replication\(strategy=NetworkTopologyStrategy, dc1=3, dc2=1\) -node $firstnode-$iface -log file=stress/$TEST_FILENAME
    fi
    sleep 10
done






    # END OF THE APPLICATION EXECUTION CODE
fi

# Wait for a couple of minutes to assure that the data is stored
#sleep 60

# If it is the 2nd datacenter, does not continue until all the nodes are in a stable status
if [ "$DC" == "2" ]
then 
    TIME1=`date +"%T.%3N"`

    # Looping over the assigned hosts until the stable status is confirmed
    for u_host in $hostlist
    do
        blaunch $u_host "bash sync-check.sh $$"
    done

    SYNC_CONT=0
    while [ "$SYNC_CONT" != "$DC2_N_NODES" ]
    do
        SYNC_CONT=0
        for u_host in $hostlist
        do
            if [ -f sync-status-$$-$u_host-file.txt ]
            then
                SYNC_CONT=$(($SYNC_CONT+1))
            else
               break
            fi
        done
    done
    
    TIME2=`date +"%T.%3N"`
    MILL1=$(echo $TIME1 | cut -c 10-12)
    MILL2=$(echo $TIME2 | cut -c 10-12)
    TIMESEC1=$(date -d "$TIME1" +%s)
    TIMESEC2=$(date -d "$TIME2" +%s)
    TIMESEC=$(( TIMESEC2 - TIMESEC1 ))
    MILL=$(( MILL2 - MILL1 ))

    # Adjusting seconds if necessary
    if [ $MILL -lt 0 ] 
    then
        MILL=$(( 1000 + MILL ))
        TIMESEC=$(( TIMESEC - 1 ))
    fi  

    # Getting minutes if over 56 seconds
    if [ $TIMESEC -gt 59 ]; then
        TIMEMIN=$( TIMESEC / 60 )
        TIMESEC=$(echo $TIMESEC | rev | cut -c -2 | rev)
        echo "Sync process took: "$TIMEMIN"m. "$TIMESEC"s. "$MILL"ms."
    else
        echo "Sync process took: "$TIMESEC"s. "$MILL"ms."
    fi 

    # Cleaning sync status files
    rm sync-status-$$-*-file.txt
fi


# Don't continue until the status is stable
#while [ "$NDT_STATUS" != "$($CASS_HOME/bin/nodetool status)" ]
#do
#    NDT_STATUS=$($CASS_HOME/bin/nodetool status)
#    sleep 60
#done

sleep 300

# Kills the job to shutdown every cassandra service
#exit_killjob
