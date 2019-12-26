#!/bin/bash
pcount=$#
if((pcount==0)); then
    echo "no args!args in {start|stop}";
exit;
fi

user=`whoami`
#start|stop
if [ $1 == "start" ]
then
    for((host=102; host<105; host++))
        do
        {
        	echo --------------------- $user@hadoop$host start kafka----------------;
            ssh $user@hadoop$host "source /etc/profile;cd /opt/module/kafka;rm -f kfk_stop.log;bin/kafka-server-start.sh config/server.properties > kfk_start.log 2>&1 &";
        }
        done
elif [ $1 == "stop" ]
then
    for((host=102; host<105; host++))
        do
        {
        	echo --------------------- $user@hadoop$host stop kafka----------------;
            ssh $user@hadoop$host "source /etc/profile;cd /opt/module/kafka;rm -f kfk_start.log;bin/kafka-server-stop.sh stop > kfk_stop.log 2>&1 &";
        }
        done
else                
    echo "wrong args!args in {start|stop}";
fi
