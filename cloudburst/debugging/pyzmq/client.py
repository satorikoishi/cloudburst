#
#   Hello World client in Python
#   Connects REQ socket to tcp://localhost:5555
#   Sends "Hello" to server, expects "World" back
#

import zmq

service_addr = 'tcp://' + '127.0.0.1' + ':%d'
CONNECT_PORT = 5000

context = zmq.Context()

#  Socket to talk to server
print("Connecting to hello world server…")
socket = context.socket(zmq.REQ)
socket.connect(service_addr % CONNECT_PORT)

#  Do 10 requests, waiting each time for a response
for request in range(10):
    print("Sending request %s …" % request)
    socket.send_string("Hello")

    #  Get the reply.
    message = socket.recv_string()
    print("Received reply %s [ %s ]" % (request, message))