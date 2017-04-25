#!/bin/bash
DC="${1}"
JOBNAME="${2}"
CASS_HOME="${3}"
NODE_ID="${4}"
#SSD_MOUNT_POINT=/tmp # This will be replaced by the user's own SSD scratch path assigned
SSD_MOUNT_POINT=/mnt/nvme0n1p4/cassandra/ # This will be replaced by the user's own SSD scratch path assigned
USERNAME=$(whoami)
STORAGE=""
DEBUG=1

if [ "$DC" == "1" ]; then
    mkdir -p $SSD_MOUNT_POINT/$USERNAME
    STORAGE=$SSD_MOUNT_POINT/$USERNAME
elif [ "$DC" == "2" ]; then
    STORAGE=$CASS_HOME
fi

if [ "$DEBUG" == "1" ]; then
    echo "I'm host "$(hostname)
    echo "DC: "$DC
    echo "JOBNAME: "$JOBNAME
    echo "CASS_HOME: "$CASS_HOME
    echo "NODE_ID: "$NODE_ID
    echo "SSD_MOUNT_POINT: "$SSD_MOUNT_POINT
    echo "USERNAME: "$USERNAME
    echo "STORAGE: "$STORAGE
fi

# Cleaning and setting symlinks to reach the correct rackdc config file
if [ -L /tmp/rack-properties-softlink-$USERNAME ]; then
    rm /tmp/rack-properties-softlink-$USERNAME
fi
ln -s $CASS_HOME/conf/cassandra-rackdc.properties-dc$DC /tmp/rack-properties-softlink-$USERNAME

if [ ! -L $CASS_HOME/conf/cassandra-rackdc.properties ]; then
    ln -s /tmp/rack-properties-softlink-$USERNAME $CASS_HOME/conf/cassandra-rackdc.properties
fi

# Setting symlinks to reach the configuration file for this Cassandra node
if [ -L /tmp/cassandra.yaml-$USERNAME ]; then
    rm /tmp/cassandra.yaml-$USERNAME
fi
ln -s $CASS_HOME/conf/yaml/cassandra.yaml-$(hostname) /tmp/cassandra.yaml-$USERNAME

if [ ! -L $CASS_HOME/conf/cassandra.yaml ]; then
    ln -s /tmp/cassandra.yaml-$USERNAME $CASS_HOME/conf/cassandra.yaml
fi

# Creating the necessary directories to place the Cassandra Data for this Datacenter
mkdir -p $STORAGE/cassandra-DC$DC
mkdir -p $STORAGE/cassandra-DC$DC/$USERNAME
mkdir -p $STORAGE/cassandra-DC$DC/$USERNAME/$JOBNAME
mkdir -p $STORAGE/cassandra-DC$DC/$USERNAME/$JOBNAME/data
mkdir -p $STORAGE/cassandra-DC$DC/$USERNAME/$JOBNAME/commit
mkdir -p $STORAGE/cassandra-DC$DC/$USERNAME/$JOBNAME/data/node$NODE_ID

# Setting the data path in the config file (safely)
sed "/data_file_directories:/!b;n;c     - $STORAGE/cassandra-DC$DC/$USERNAME/$JOBNAME/data/node$NODE_ID" $CASS_HOME/conf/cassandra-cfg.yaml > $CASS_HOME/conf/aux.$$.yaml
sed "s+.*commitlog_directory:.*+commitlog_directory: $STORAGE/cassandra-DC$DC/$USERNAME/$JOBNAME/commit/node$NODE_ID+g" $CASS_HOME/conf/aux.$$.yaml > $CASS_HOME/conf/aux2.$$.yaml
mv $CASS_HOME/conf/aux2.$$.yaml $CASS_HOME/conf/yaml/cassandra.yaml-$(hostname)
