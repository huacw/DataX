#!/bin/sh

SCRIPT_HOME=$(cd $(dirname $0); pwd)
cd $SCRIPT_HOME/..
mvn clean package -DskipTests assembly:assembly

cd $SCRIPT_HOME/target/datax/plugin/writer/

if [ -d "estransportwriter" ]; then
    tar -zcvf estransportwriter.tgz eswriter
    cp estransportwriter.tgz $SCRIPT_HOME
    cd $SCRIPT_HOME
ansible-playbook -i hosts main.yml -u vagrant -k
fi




