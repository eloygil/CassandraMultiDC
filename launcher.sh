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
RETRY_MAX=10
#DC2_N_HOSTS=4 #Hardcoded
DC2_N_HOSTS=2 #while testing

function usage () {
    # Prints a help message
    echo "Usage: bash launcher.sh [ -h | RUN [ -j jobname ] [ N ] | STATUS [ jobname ] | KILL [ jobname ] ]"
    echo "       -h:"
    echo "       Prints this usage help."
    echo " "
    echo "       RUN [ -j jobname ] [ N ]:"
    echo "       Starts two new Cassandra Clusters. For the DC1 it starts N nodes, if given. Default value is 4."
    echo "       If a job name is specified with the -j parameter it will have that name (not implemented), otherwise a random job name is generated."
    echo " "
    echo "       STATUS [ jobname ]"
    echo "       Gets the status of the Cassandra Cluster with that job name, if given, otherwise it shows a list of running jobs."
    echo " "
    echo "       KILL [ jobname ]:"
    echo "       Kills the clusters associated with that job name, if given, else shows a list of running jobs."
}

function get_running_clusters () {
    # Finds LSF jobs related to Cassandra clusters and prints details about jobs and datacenter status.
    RUNNING_JOBS=""
    BJOBS=$(bjobs -noheader | awk '{ print $3 $7 }' | sed '/^\s*$/d')     # List of STATEJOBID information like: RUN11234567
    for line in $BJOBS
    do
        if [ "$(echo $line | cut -c -3)" == "RUN" ] && [ "$(echo $line | cut -c 4)" == "1" ]  # Filtering to get RUNNING and DC1
        then
            RUN_JOBID=$(echo $line | cut -c 5-)
            if [ "$(echo $BJOBS | grep "RUN2"$RUN_JOBID)" != "" ] && [ "$(cat $JOB_DB_FILE | grep $RUN_JOBID | awk '{ print $1 }' | sed 's/ //g')" == "$RUN_JOBID" ]      # Its DC2 is RUN & in jobs.db
            then
                RUNNING_JOBS="$RUNNING_JOBS $RUN_JOBID"                     # Adding JOB ID to the Cassandra running jobs list
            fi
        fi
    done
    if [ "$RUNNING_JOBS" != "" ]
    then
        echo "Showing all the running Cassandra Multi-Datacenter clusters:"
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
        echo "No running jobs related with DC1/DC2 Cassandra clusters found."
        bjobs
    fi
}

function get_job_info () {
    # Gets the status of the jobs that are responsible of the given Cassandra cluster Job ID
    JOB_ID_1=$(bjobs | grep $OPTION | head -n 1 | awk '{ print $1 }') 
    JOB_ID_2=$(bjobs | grep $OPTION | tail -n 1 | awk '{ print $1 }') 
    JOB_STATUS_1=$(bjobs | grep $OPTION | head -n 1 | awk '{ print $3 }') 
    JOB_STATUS_2=$(bjobs | grep $OPTION | tail -n 1 | awk '{ print $3 }') 
}

function get_cluster_node () {
    # Gets the ID of the first node
    NODE_ID=$(bjobs | grep $OPTION | head -n 1 | awk '{ print $6 }' | tail -c 9)
}

function exit_no_cluster () {
    # Any Cassandra cluster is running. Exit.
    echo "There is not a Cassandra cluster named <"$OPTION"> running. Exiting..."
    exit
}

function exit_bad_node_status () {
    # Exit after getting a bad node status. 
    echo "Cassandra Cluster Status: ERROR"
    echo "One or more nodes are not up (yet?)"
    #$CASS_HOME/bin/nodetool -h $NODE_ID status
    echo "Exiting..."
    exit
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

function get_dc_status () {
    NODE_STATE_LIST=`$CASS_HOME/bin/nodetool -h $NODE_ID status | grep ack | awk '{ print $1 }'`
    SWITCH_STATUS="ON"
    if [ "$NODE_STATE_LIST" == "" ] && [ "$ACTION" != "RUN" ]
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
            if [ "$ACTION" != "RUN" ]; then
                echo "E1"
                exit_bad_node_status
            fi
        elif [ "$SWITCH_STATUS" == "OFF" ]
        then
            NODE_COUNTER_1=$(($NODE_COUNTER_1+1))
        elif [ "$SWITCH_STATUS" == "ON" ]
        then
            NODE_COUNTER_2=$(($NODE_COUNTER_2+1))
        fi
    done
}

if [ "$ACTION" == "RUN" ] || [ "$ACTION" == "run" ]
then
    set_run_parameters
    # Starts a Cassandra Clusters
    echo "Starting Cassandra Clusters (Job ID: "$JOBNAME")"
    echo "#DC1 nodes: "$N_NODES "| #DC2 nodes: "$DC2_N_HOSTS

    # Creating logs directory, if not exists
    mkdir -p $HOME/cassandraDC4juron/logs

    # Adding jobname and details to job database
    echo " $JOBNAME $N_NODES $DC2_N_HOSTS" >> $JOB_DB_FILE
 
    # Launching job for Datacenter1 (over SSD)
    bsub -J "1$JOBNAME" -n $((19 * $N_NODES)) -W 20 -R span[ptile=19] -oo logs/$JOBNAME-DC1-%J.out -eo logs/$JOBNAME-DC1-%J.err "bash cass.sh $JOBNAME 1 $N_NODES"
    # Launching job for Datacenter2 (over GPFS)
    bsub -J "2$JOBNAME" -n $((19 * $DC2_N_HOSTS)) -W 20 -R span[ptile=19] -oo logs/$JOBNAME-DC2-%J.out -eo logs/$JOBNAME-DC2-%J.err "bash cass.sh $JOBNAME 2 $DC2_N_HOSTS"

    echo "Please, be patient. It may take a while until it shows a correct STATUS (and it may show some harmless errors during this process)."
    RETRY_COUNTER=0
    sleep 15
    get_cluster_node
    get_dc_status
    while [ "$NODE_COUNTER_1" != "$N_NODES" ] || [ "$NODE_COUNTER_2" -lt "$DC2_N_HOSTS" ] &&  [[ "$RETRY_COUNTER" -lt "$RETRY_MAX" ]] 
    do
        echo "Checking..."
        sleep 20
        RETRY_COUNTER=$(($RETRY_COUNTER+1))
    	get_dc_status
    done
    if [ "$RETRY_COUNTER" == "$RETRY_MAX" ]
    then
        echo "ERROR: Cassandra MultiDC cluster RUN timeout. Check STATUS."
    else
        echo "Cassandra cluster DC1 with "$N_NODES" nodes and cluster DC2 with "$DC2_N_HOSTS" started successfully."
    fi
    exit 
elif [ "$ACTION" == "STATUS" ] || [ "$ACTION" == "status" ]
then
    if [ "$OPTION" != "" ]
    then
        # If a Cassandra Cluster jobname is given and alive, it prints the information of the nodes
        get_job_info
        if [ "$JOB_ID_1" != "" ]
        then
            if [ "$JOB_STATUS_1" == "PEND" ] || [ "$JOB_STATUS_2" == "PEND" ]
            then
                echo "There is at least a job that is still pending. Wait for a minute and try again."
                exit
            fi 
            get_cluster_node
            get_dc_status
            N_DC1_DB=$(cat $JOB_DB_FILE | grep $OPTION | awk '{ print $2 }')
            N_DC2_DB=$(cat $JOB_DB_FILE | grep $OPTION | awk '{ print $3 }')
            if [ "$N_DC1_DB" == "$NODE_COUNTER_1" ] && [ "$N_DC2_DB" == "$NODE_COUNTER_2" ]
            then
                echo "Cassandra Multi-Datacenter cluster <"$OPTION"> Status: OK"
       	        $CASS_HOME/bin/nodetool -h $NODE_ID status
            else
                echo "E2"
                echo "Cassandra Multi-Datacenter cluster <"$OPTION"> Status: ERROR"
                echo "N_DC1_DB: "$N_DC1_DB
                echo "NODE_COUNTER_1: "$NODE_COUNTER_1
                echo "N_DC2_DB: "$N_DC2_DB
                echo "NODE_COUNTER_2: "$NODE_COUNTER_2
                exit_bad_node_status
            fi
        else
            exit_no_cluster
        fi
    else
        # Otherwise, it shows all the alive Cassandra Clusters, if they exist.
        echo "INFO: No ID given, therefore the cluster consistency check is DISABLED."
        get_running_clusters
        exit
    fi
elif [ "$ACTION" == "KILL" ] || [ "$ACTION" == "kill" ]
then
    # If there is a running Cassandra Cluster with the given Job ID it kills it, otherwise it shows a list of running clusters
    if [ "$OPTION" != "" ]
    then
        get_job_info
        if [ "$JOB_ID_1" != "" ]
        then
            bkill $JOB_ID_1
            bkill $JOB_ID_2
            echo "It will take a while to complete the shutdown..." 
            sleep 5
            echo "Done."
        else
            exit_no_cluster
        fi
    else
        # No ID given, showing running clusters...
        echo "INFO: No ID given, showing running clusters: "
        get_running_clusters
        exit
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
