from socket import *
import json
import threading


def main():
    # Message class for storing messages!
    class Message:
        def __init__(self, type, value):
            self.type = type
            self.value = value

    # Peer class contains two sockets: receive and send
    # The first one listens for messages and the second one sends them
    # If the Peer is the initializer of the conversation then it binds the sockets with to ports
    # otherwise the peer just connects to the sockets
    # Every peer is on a separate thread waiting for a message to reply
    class Peer(threading.Thread):

        def __init__(self, receive_ip, receive_port, send_ip, send_port):
            threading.Thread.__init__(self)
            self.receive_ip = receive_ip
            self.receive_port = receive_port
            self.send_ip = send_ip
            self.send_port = send_port
            self.receive_socket = None
            self.send_socket = None
            self.connected = False
            self.initialize_sockets()

        def connect(self):
            self.receive_socket.connect((self.receive_ip, self.receive_port))
            self.send_socket.connect((self.send_ip, self.send_port))
            self.connected = True

        def bind(self):
            self.receive_socket.bind((self.receive_ip, self.receive_port))
            self.receive_socket.listen(1)
            self.send_socket.bind((self.send_ip, self.send_port))
            self.send_socket.listen(1)

        def run(self):
            if not self.connected:
                self.accept_connection()
            while self.connected:
                data = self.receive_socket.recv(1024)
                self.connected = self.resolve_message(data)
            self.receive_socket.close()

        # accepting the other peers connection request
        def accept_connection(self):
            self.receive_socket, addr = self.receive_socket.accept()
            self.send_socket, addr = self.send_socket.accept()
            self.connected = True

        def send_message(self, type, value):
            message = Message(type, value)
            byte_array = json.dumps(message.__dict__).encode("utf-8")
            self.send_socket.send(byte_array)

        def initialize_sockets(self):
            self.receive_socket = socket(AF_INET, SOCK_STREAM)
            self.send_socket = socket(AF_INET, SOCK_STREAM)

        # This method takes a byte array, converts it to json and the message
        # and gives appropriate answer to that message
        def resolve_message(self, data):
            if data == b'':
                return False
            message = Message(**json.loads(data))
            print(message.type, " ", message.value)
            if message.type == "start" and message.value == "hello":
                self.send_message("start", "hi")
                return True
            elif message.type == "start" and message.value == "hi":
                self.send_message("end", "goodbye")
                return True
            elif message.type == "end" and message.value == "goodbye":
                self.send_message("end", "bye")
                return True
            elif message.type == "end" and message.value == "bye":
                self.send_socket.close()
                return False

    # getting input form terminal
    sr, ip1, port1, ip2, port2 = input().split()

    if sr == "r":
        peer = Peer(ip1, int(port1), ip2, int(port2))
        peer.bind()
        peer.start()

    elif sr == "s":
        peer = Peer(ip1, int(port1), ip2, int(port2))
        peer.connect()
        peer.start()
        peer.send_message("start", "hello")

    else:
        print("Invalid type identifier")


if __name__ == "__main__":
    main()
