#
#   Hello World server in Python
#   Binds REP socket to tcp://*:5555
#   Expects b"Hello" from client, replies with b"World"
#

import time
import zmq

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5555")
poller = zmq.Poller()
poller.register(socket, zmq.POLLIN)

while True:
    #  Wait for next request from client
    socks = dict(poller.poll(timeout=1000))
    
    if socket in socks and socks[socket] == zmq.POLLIN:
        message = socket.recv_string()
        print("Received request: %s" % message)

        #  Do some 'work'
        time.sleep(1)

        #  Send reply back to client
        socket.send_string("World")