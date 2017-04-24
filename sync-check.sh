#!/bin/bash
###############################################################################################################
#                               Cassandra Node Synchronization Checker for Juron                              #
#                                          Eloy Gil - eloy.gil@bsc.es                                         #
#                                     Barcelona Supercomputing Center 2017                                    #
###############################################################################################################
CASS_HOME=$HOME/cassandra-dc
EXEC_ID=${1}
SYNC_STATUS_FILE=sync-status-$EXEC_ID-$(hostname)-file.txt
HST_IFACE="ib0" #interface configured in the cassandra.yaml file
N_TIMES=5
REPEATED=0
NETSTATS_OLD_INFO=$($CASS_HOME/bin/nodetool -h $(hostname) netstats | grep "Small")
while [ $REPEATED -lt $N_TIMES ]
do    
    NETSTATS_NEW_INFO=$(../cassandra-dc/bin/nodetool -h $(hostname) netstats | grep "Small")
    if [ "$NETSTATS_OLD_INFO" == "$NETSTATS_NEW_INFO" ]
    then
        ((REPEATED++))
    else
        REPEATED=0 
    fi
    NETSTATS_OLD_INFO=$NETSTATS_NEW_INFO
    sleep 2
done

# When it finishes creates the DONE status file for this host
echo "DONE" > $SYNC_STATUS_FILE
