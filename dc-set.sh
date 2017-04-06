#!/bin/bash
DC=${1}
JOBNAME=${2}
SSD_MOUNT_POINT=${3}
CASS_HOME=${4}
NODE_ID=${5}

# Cleaning symlink from previous executions
if [ -f /tmp/rack-properties-softlink ]; then
    rm /tmp/rack-properties-softlink
fi

# Symlinking (if necessary)
if [ ! -f $CASS_HOME/conf/cassandra-rackdc.properties ]; then
    ln -s /tmp/rack-properties-softlink $CASS_HOME/conf/cassandra-rackdc.properties
fi

if [ "$DC" == "1" ]	
then
    # Creating the necessary directories to place the Cassandra Data for Datacenter 1
    mkdir -p $SSD_MOUNT_POINT/cassandra-DC1
    mkdir -p $SSD_MOUNT_POINT/cassandra-DC1/$JOBNAME
    mkdir -p $SSD_MOUNT_POINT/cassandra-DC1/$JOBNAME/data
    mkdir -p $SSD_MOUNT_POINT/cassandra-DC1/$JOBNAME/commit
    mkdir -p $SSD_MOUNT_POINT/cassandra-DC1/$JOBNAME/data/node$NODE_ID
    # Setting the symlink to reach DC1 rackdc config file
    ln -s $CASS_HOME/conf/cassandra-rackdc.properties-dc1 /tmp/rack-properties-softlink
    #Setting the data path in the config file (safely)
    sed "/data_file_directories:/!b;n;c     - $SSD_MOUNT_POINT/cassandra-DC1/$JOBNAME/data/node$NODE_ID" $CASS_HOME/conf/cassandra-cfg.yaml > $CASS_HOME/conf/aux.yaml
    cat $CASS_HOME/conf/aux.yaml | sed "s/.*commitlog_directory:.*/          - commitlog_directory: $SSD_MOUNT_POINT\/cassandra-DC1\/$JOBNAME\/commit/" > $CASS_HOME/conf/cassandra-aux.yaml
    mv $CASS_HOME/conf/cassandra-aux.yaml $CASS_HOME/conf/yaml/cassandra.yaml-$(hostname)
elif [ "$DC" == "2" ]
then
    # Creating the necessary directories to place the Cassandra Data for Datacenter 1
    mkdir -p $CASS_HOME/cassandra-DC2
    mkdir -p $CASS_HOME/cassandra-DC2/$JOBNAME
    mkdir -p $CASS_HOME/cassandra-DC2/$JOBNAME/data
    mkdir -p $CASS_HOME/cassandra-DC2/$JOBNAME/commit
    mkdir -p $CASS_HOME/cassandra-DC2/$JOBNAME/data/node$NODE_ID
    # Setting the symlink to reach DC2 rackdc config file
    ln -s $CASS_HOME/conf/cassandra-rackdc.properties-dc2 /tmp/rack-properties-softlink
    #Setting the data path in the config file (safely)
    sed "/data_file_directories:/!b;n;c     - $CASS_HOME/cassandra-DC2/$JOBNAME/data/node$NODE_ID" $CASS_HOME/conf/cassandra-cfg.yaml > $CASS_HOME/conf/aux.yaml
    cat $CASS_HOME/conf/aux.yaml | sed "s/.*commitlog_directory:.*/          - commitlog_directory: $CASS_HOME\/cassandra-DC2\/$JOBNAME\/commit/" > $CASS_HOME/conf/cassandra-aux.yaml
    mv $CASS_HOME/conf/cassandra-aux.yaml $CASS_HOME/conf/yaml/cassandra.yaml-$(hostname)
fi

# Setting symlinks to reach the configuration file for this Cassandra node
if [ -f /tmp/cassandra.yaml ]; then
    rm /tmp/cassandra.yaml
fi
ln -s $HOME_PATH/conf/yaml/cassandra.yaml-$(hostname) /tmp/cassandra.yaml

if [ ! -f $HOME_PATH/conf/cassandra.yaml ]
then
    ln -s /tmp/cassandra.yaml $HOME_PATH/conf/cassandra.yaml
fi
