__author__ = 'ayscb'

import threading
import socket
import string
import Queue

# collector simulation
# 	work flow :
#       1) connect with master( IP:port ) by tcp socket
#       2) send request to master for available workers ( ip:udpPort )	[a thread]
#       3) send data to available workers [another thread]
#
BUFFER_SIZE = 1000


def tcpConnector(masterip, masterport):
    clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientsocket.connect_ex((masterip, masterport))
    print "try to connect with master ( " + masterip + " : " + masterport + ")"

    clientsocket.send("###")
    while True:
        data = clientsocket.recv(BUFFER_SIZE)
        if not data:
            continue
        else:
            firstpost = string.find(data, "$")
            if firstpost == -1:
                continue
            else:
                print data

def unpackdata(data, offset):
    ost = offset + 1
    num = data(ost)     # total ip + port number
    for i in range ( num ) :
        string.atoi('a')

if __name__ == '__main__':
    tcpConnector()


