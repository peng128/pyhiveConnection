from kazoo.client import KazooClient
from pyhive import hive
import random

#for LUDP Connection
def LUDPConnect(username, passwd):
    # for LUDP test environment
    zkhost="node15.test:2181,node16.test:2181"
    znodeName="/hiveserver2"
    serviceKeyword="serverUri"
    return connection(zkhost, znodeName, serviceKeyword, username, passwd)

#connect hive by pyhive and return cursor
def connection(zkhost, znodeName, serviceKeyword,username,passwd):
    hostList = discoveryThriftSerivcehost(zkhost, znodeName, serviceKeyword)
    hostLength = hostList.__len__()
    random.seed()
    isConnected=False
    while isConnected is False and hostLength > 0:
        index = random.randint(0, hostLength-1)
        hostStr = hostList.pop(index).split(":")
        try:
            cursor = hive.connect(host=hostStr[0], port=hostStr[1], username=username, password=passwd).cursor()
            isConnected = True
        except:
            isConnected = False
            if hostLength > 1:
                print("ERROR:Can not connect "+hostStr[0]+":"+hostStr[1]+" .try another thrift server...")
            else:
                print("ERROR:Can not connect hiveserver2, please check the connection config and the hiveserver")
                return 0
        hostLength -= 1
    return cursor

#discovery the thrfit service host list
def discoveryThriftSerivcehost(zkhost,znodeName,serviceKeyword):
    zkClient = KazooClient(hosts=zkhost)
    zkClient.start()
    #get the children name of zonde
    result = zkClient.get_children(znodeName)
    zkClient.stop()
    length = result.__len__()
    hostList = list()
    for i in range(0, length):
        resultHost = {}
        map(lambda x: resultHost.setdefault(x.split("=")[0], x.split("=")[1]), str(result[i]).split(";"))
        hostList.append(resultHost.get(serviceKeyword))
    return hostList
