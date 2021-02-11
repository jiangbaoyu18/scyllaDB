#! /bin/bash                                                                                                                     
 
server_name="scylla"
server_path="/scylla/scylla/build/debug/scylla --developer-mode=1  --options-file /scylla/scylla/conf/scylla.yaml -c 5 -m 10G"
 
 
start(){
    echo "--start-"
    nohup $server_path &
    pid=$(ps -ef|grep $server_name |grep -v grep |awk '{print $2}')
    if [ ! $pid ]; then
        echo "service is not exit ..."
    else
        echo "pid=$pid"
    fi  
}
 
stop(){
    pid=$(ps -ef|grep $server_name |grep -v grep |awk '{print $2}')
    if [ ! $pid ]; then
        echo "service is not exit ..."
    else
        kill -9 $pid
    fi  
}
 
checkrun(){ 
    while true
    do
        pid=$(ps -ef|grep $server_name |grep -v grep |awk '{print $2}')
        if [[ $pid -eq 0 ]]; then
            echo "--restart-"
            start 
        fi  
 
        sleep 30s 
    done
} 
 
case $1 in
start):
    start
    ;;
stop):
    echo "--stop"
    echo $pid
    stop
    ;;
checkrun):
    echo "--restart"
    checkrun
    ;;
*):
    echo "error ..."
    ;;
esac
 
exit 0 
