#!/bin/bash
###############################################################################################################
#                                   Cassandra Multiple DC Launcher for Juron                                  #
#                                          Eloy Gil - eloy.gil@bsc.es                                         #
#                                     Barcelona Supercomputing Center 2017                                    #
###############################################################################################################
ACTION=${1}
OPTION=${2}
JOBNAME=${3}
N_NODES=${4}
CASS_HOME=$HOME/cassandra-dc
HOST_LIST=/tmp/cassandradc-host-list.txt
N_NODES_FILE=cassandradc-num-nodes.txt
RECOVER_FILE=cassandradc-recover-file.txt
JOB_DB_FILE=jobs.db
RETRY_MAX=12
#DC2_N_HOSTS=4 #Hardcoded
DC2_N_HOSTS=2 #while testing

function usage () {
    # Prints a help message
    echo "Usage: bash launcher.sh [ -h | RUN [ -j jobname ] [ N ] | STATUS | KILL ]"
    echo "       -h:"
    echo "       Prints this usage help."
    echo " "
    echo "       RUN [ jobname ] [ N ]:"
    echo "       Starts two new Cassandra Clusters. For the DC1 it starts N nodes, if given. Default value is 4."
    echo "       If a job name is specified with the -j parameter it will have that name, otherwise a random job name is generated."
    echo " "
    echo "       STATUS:"
    echo "       Gets the status of the Cassandra Cluster(s)."
    echo " "
    echo "       KILL [ jobname ]:"
    echo "       Kills the clusters associated with that job name, if given, else shows a list of running jobs."
}

function get_running_clusters () {
    RUNNING_JOBS=""
    # Finds LSF jobs related to Cassandra clusters.
    BJOBS=$(bjobs -noheader | awk '{ print $3 $7 }' | sed '/^\s*$/d')     # List of STATEJOBID information like: RUN11234567
    for line in $BJOBS
    do
        if [ "$(echo $line | cut -c -3)" == "RUN" ] && [ "$(echo $line | cut -c 4)" == "1" ]  # Filtering to get RUNNING and DC1
        then
            RUN_JOBID=$(echo $line | cut -c 5-)
            if [ "$(echo $BJOBS | grep "RUN2"$RUN_JOBID)" != "" ] && [ "$(cat $JOB_DB_FILE | grep $RUN_JOBID | sed 's/ //g')" == "$RUN_JOBID" ]      # Its DC2 is RUN & in jobs.db
            then
                RUNNING_JOBS="$RUNNING_JOBS $RUN_JOBID"                     # Adding JOB ID to the Cassandra running jobs list
            fi
        fi
    done
    if [ "$RUNNING_JOBS" != "" ]
    then
        for job in $RUNNING_JOBS
        do
            echo " "
            echo "CLUSTER JOB NAME: "$job                                                
            echo "###########################################################################################################"
            echo "Datacenter dc1 job details:"
            bjobs -J 1$job
            echo "Datacenter dc2 job details:"
            bjobs -J 2$job
            $CASS_HOME/bin/nodetool -h $(bjobs -noheader | grep 1$job | awk '{ print $6 }' | cut -c 4-) status  
            echo "###########################################################################################################"
            echo " "
        done
    else
        echo "No running jobs related with DC1/DC2 Cassandra clusters found"
        bjobs
    fi
}

function get_job_info () {
    # Gets the ID of the job that runs the Cassandra Cluster
    JOB_INFO=$(bjobs | grep $JOBNAME) 
    JOB_ID=$(echo $JOB_INFO | awk '{ print $1 }')
    JOB_STATUS=$(echo $JOB_INFO | awk '{ print $3 }')   
}

function get_cluster_node () {
    # Gets the ID of the first node
    NODE_ID=$(bjobs | grep $JOBNAME | awk '{ print $6 }' | tail -c 9)
}

function get_cluster_ips () {
    # Gets the IP of every node in the cluster
    NODE_IPS=$($CASS_HOME/bin/nodetool -h $NODE_ID status | awk '/Address/{p=1;next}{if(p){print $2}}')
}

function exit_no_cluster () {
    # Any Cassandra cluster is running. Exit.
    echo "There is not a Cassandra cluster running. Exiting..."
    exit
}

function exit_bad_node_status () {
    # Exit after getting a bad node status. 
    echo "Cassandra Cluster Status: ERROR"
    echo "One or more nodes are not up (yet?) - It was expected to find ""$(cat $N_NODES_FILE)"" UP nodes."
    #$CASS_HOME/bin/nodetool -h $NODE_ID status
    echo "Exiting..."
    exit
}

function test_if_cluster_up () {
    # Checks if other Cassandra Cluster is running, aborting if it is happening
    if [ "$(bjobs | grep cassandra)" != "" ] && [ "$(bjobs | grep cassandra)" != "No unfinished job found" ] 
    then
        echo "Another Cassandra Cluster is running and could collide with a new execution. Aborting..."
        bjobs
        exit
    fi
}

function get_nodes_up () {
    get_job_info
    if [ "$JOB_ID" != "" ]
    then
        if [ "$JOB_STATUS" == "RUN" ]
        then    
            get_cluster_node 
            NODE_STATE_LIST=`$CASS_HOME/bin/nodetool -h $NODE_ID status | sed 1,5d | sed '$ d' | awk '{ print $1 }'`
            if [ "$NODE_STATE_LIST" != "" ]
            then
                NODE_COUNTER=0
                for state in $NODE_STATE_LIST
                do  
                    if [ $state != "UN" ]
                    then
                        RETRY_COUNTER=$(($RETRY_COUNTER+1))
                        break
                    else
                        NODE_COUNTER=$(($NODE_COUNTER+1))
                    fi
                done
            fi
        fi
    fi
}

function set_run_parameters() {
    # If the jobname DB does not exist, it is created
    if [ ! -f $JOB_DB_FILE ]
    then
        echo "" > $JOB_DB_FILE
    fi
    # Sets parameters depeding of the input
    if [ "$JOBNAME" == "-j" ]
    then
        N_NODES=${2}
        JOBNAME=${4}
    elif [ "${2}" != "-j" ] && [ "${3}" != "-j" ]
    then
        # Test and set of a new job name
        N=1
        JOBNAME=$(date "+%y%m%d")$N
        while [ "$(cat $JOB_DB_FILE | grep $JOBNAME)" != "" ]
        do
            JOBNAME=$(date "+%y%m%d")$N
            N=$(($N+1))
        done
    fi
    if [ "$(echo $JOBNAME | cut -c -9)" != "$JOBNAME" ]
    then
        echo "The selected job name is too long (max. 9 char)"
        echo "Exiting..."
        exit
    fi
    # The number of nodes is 4 by default
    if [ "$OPTION" != "-j" ] && [ "$OPTION" != "" ]
    then
        N_NODES=$OPTION
    elif [ "$N_NODES" == "" ]
    then
        N_NODES=4
    fi
}

if [ "$ACTION" == "TEST" ] || [ "$ACTION" == "test" ]
then
    get_running_clusters
    exit
fi


if [ "$ACTION" == "RUN" ] || [ "$ACTION" == "run" ]
then
    set_run_parameters
    #test_if_cluster_up
    # Starts a Cassandra Clusters
    echo "Starting Cassandra Clusters (Job ID: "$JOBNAME")"

    echo "#DC1 nodes: "$N_NODES
    echo "#DC2 nodes: "$DC2_N_HOSTS
    # Since this is a fresh launch, it assures that the recover file is empty
    #echo "" > $RECOVER_FILE

    # Creating logs directory, if not exists
    mkdir -p $HOME/cassandraDC4juron/logs

    # Adding job to job database
    echo " $JOBNAME" >> $JOB_DB_FILE

    # Launching job for Datacenter1 (over SSD)
    bsub -J "1$JOBNAME" -n $((19 * $N_NODES)) -W 20 -R span[ptile=19] -oo logs/$JOBNAME-DC1-%J.out -eo logs/$JOBNAME-DC1-%J.err "bash cass.sh $JOBNAME 1 $N_NODES"
    # Launching job for Datacenter2 (over GPFS)
    bsub -J "2$JOBNAME" -n $((19 * $DC2_N_HOSTS)) -W 20 -R span[ptile=19] -oo logs/$JOBNAME-DC2-%J.out -eo logs/$JOBNAME-DC2-%J.err "bash cass.sh $JOBNAME 2 $DC2_N_HOSTS"

    #echo "Please, be patient. It may take a while until it shows a correct STATUS (and it may show some harmless errors during this process)."
    #RETRY_COUNTER=0
    #sleep 10
    #while [ "$NODE_COUNTER" != "$N_NODES" ] && [ $RETRY_COUNTER -lt $RETRY_MAX ]; do
    #    echo "Checking..."
    #    sleep 3
    #	get_nodes_up
    #done
    #if [ "$NODE_COUNTER" == "$N_NODES" ]
    #then
    #    echo "Cassandra Cluster with "$N_NODES" nodes started successfully."
    #else
    #    echo "ERROR: Cassandra Cluster RUN timeout. Check STATUS."
    #fi 
elif [ "$ACTION" == "STATUS" ] || [ "$ACTION" == "status" ]
then
    # If there is a running Cassandra Cluster it prints the information of the nodes
    get_job_info
    if [ "$JOB_ID" != "" ]
    then
    	if [ "$JOB_STATUS" == "PEND" ]
        then
            echo "The job is still pending. Wait for a while and try again."
            exit
        fi 
        get_cluster_node 
        NODE_STATE_LIST=`$CASS_HOME/bin/nodetool -h $NODE_ID status | sed 1,5d | sed '$ d' | awk '{ print $1 }'`
	if [ "$NODE_STATE_LIST" == "" ]
	then
            echo "ERROR: No status found. The Cassandra Cluster may be still bootstrapping. Try again later."
            exit
        fi
        NODE_COUNTER=0
        for state in $NODE_STATE_LIST
        do
            if [ $state != "UN" ]
            then
                echo "E1"
                exit_bad_node_status
            else
                NODE_COUNTER=$(($NODE_COUNTER+1))
            fi
       	done
        if [ "$(cat $N_NODES_FILE)" == "$NODE_COUNTER" ]
        then
            echo "Cassandra Cluster Status: OK"
       	    $CASS_HOME/bin/nodetool -h $NODE_ID status
        else
            echo "E2"
            echo "N_NODES_FILE: "$(cat $N_NODES_FILE)
            echo "NODE_COUNTER: "$NODE_COUNTER
            exit_bad_node_status
        fi
    else
        exit_no_cluster
    fi
elif [ "$ACTION" == "KILL" ] || [ "$ACTION" == "kill" ]
then
    # If there is a running Cassandra Cluster it kills it
    get_job_info
    if [ "$JOB_ID" != "" ]
    then
        bkill $JOB_ID
        echo "It will take a while to complete the shutdown..." 
        sleep 5
        echo "Done."
    else
        exit_no_cluster
    fi
elif [ "$ACTION" == "-H" ] || [ "$ACTION" == "-h" ]
then
    # Shows the help information
    usage
    exit
else
    # There may be an error with the arguments used, also prints the help
    echo "Input argument error. Only an ACTION must be specified."
    usage
    echo "Exiting..."
    exit
fi
