#!/bin/bash
HOME_PATH=${1}
DATA_PATH=${2}
COMM_PATH=${3}
RECOVER_FILE=cassandra-recover-file.txt

#If the data path exists, cleans the content, otherwise it is created
#It gives group write permissions by default 
if [ -d $DATA_PATH ]; then
    rm -rf $DATA_PATH/*
else
    mkdir $DATA_PATH
    chmod g+w $DATA_PATH
fi

#Commit Log folder reset
#It gives group write permissions by default
#By default it is /tmp/cassandra-commitlog, if you change it you should also change the cassandra.yaml file
if [ -d $COMM_PATH ]; then
    rm -rf $COMM_PATH/*
else
    mkdir $COMM_PATH
    chmod g+w $COMM_PATH

fi
#set the data path in the config file (safely)
sed "/data_file_directories:/!b;n;c     - $DATA_PATH" $HOME_PATH/conf/cassandra-cfg.yaml > $HOME_PATH/conf/aux.yaml
mv $HOME_PATH/conf/aux.yaml $HOME_PATH/conf/cassandra-cfg.yaml-$(hostname)

#check & set of the symlinks to cassandra.yaml file for this hostname
if [ ! -f /tmp/cassandra.yaml ]
then
    ln -s $HOME_PATH/conf/cassandra-cfg.yaml-$(hostname) /tmp/cassandra.yaml
fi
if [ ! -f $HOME_PATH/conf/cassandra.yaml ]
then
    ln -s /tmp/cassandra.yaml $HOME_PATH/conf/cassandra.yaml
fi
