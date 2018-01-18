import socket
import sys
import getopt
import threading
import subprocess

TARGET   = ''
PORT     = 0
FILE     = ''

def main():
    global PORT
    global TARGET
    global FILE

    if not len(sys.argv[1:]):
        print("Usage is python netcat_client_python_file.py -h <HOST> -p <PORT> -f <FILE>\n")

    try:
        opts, args = getopt.getopt(sys.argv[1:],"h:p:f:", \
            ["TARGET", "PORT", "PATH OF FILE"])
    except getopt.GetoptError as err:
       print str(err)
    print opts, args
    for o, a in opts:
        if o in ('-h', '--host'):
            TARGET = a
        elif o in ('-p', '--port'):
            PORT = int(a)
        elif o in ('-f', '--file'):
            FILE = a
            print(FILE)
        else:
            assert False, "Unhandled option"

    # NETCAT client
    if len(TARGET) and PORT > 0:
        from time import time, sleep
        from random import random, shuffle
        import os
        if FILE == '':
            buffer = ["sample text\n"]
        else:
            listfiles = [FILE]
            for singlefile in listfiles:
                buffer = open(singlefile).readlines()
                permutations = range(len(buffer))
                shuffle(permutations)
                for i in permutations:	
                    client_sender(buffer[i])
                    sleep(random()*0.5)

def client_sender(buffer):
    client = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
    try:
        client.connect(( TARGET, PORT ))
        if len(buffer): 
            sent =  client.send(buffer)
            print "Data sent: " + buffer,
    except:
        print '[*] Exception. Exiting.'
        client.close()
        
if __name__ == '__main__':
    main()



    
