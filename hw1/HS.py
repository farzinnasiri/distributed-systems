# Written by Farzin Nasiri
# Distributed Systems 2021
import time
from socket import *
import json
import threading


# Message class for storing messages
class Token:
    def __init__(self, src_pid, type_, direction, max_hop_count, hop_count):
        self.src_pid = src_pid
        self.type_ = type_  # type can be: PROBE, REPLY,ANNOUNCEMENT
        self.direction = direction  # direction can be: CLOCKWISE, ANTICLOCKWISE
        self.max_hop_count = max_hop_count
        self.hop_count = hop_count


class NeighbourNode:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port


class Node(threading.Thread):
    def __init__(self, pid, receive_ip, receive_port, channel_delay):
        threading.Thread.__init__(self)

        self.pid = pid
        self.receive_ip = receive_ip
        self.receive_port = receive_port

        self.channel_delay = channel_delay

        self.right_node = None
        self.left_node = None

        self.receive_socket = None
        self.send_right_socket = None
        self.send_left_socket = None

        self.is_active = False
        self.is_leader = False
        self.is_leader_known = False
        self.leader_pid = None

        self.round = 0

        self.receive_socket = socket(AF_INET, SOCK_DGRAM)

    def set_left_node(self, ip, port):
        self.left_node = NeighbourNode(ip, port)
        self.send_left_socket = socket(AF_INET, SOCK_DGRAM)

    def set_right_node(self, ip, port):
        self.right_node = NeighbourNode(ip, port)
        self.send_right_socket = socket(AF_INET, SOCK_DGRAM)

    def send_to_left(self, token: Token):
        self.send(channel=self.send_left_socket, other_node=self.left_node, token=token)

    def send_to_right(self, token: Token):
        self.send(channel=self.send_right_socket, other_node=self.right_node, token=token)

    def send(self, channel: socket, other_node: NeighbourNode, token: Token):
        time.sleep(self.channel_delay)
        channel.sendto(json.dumps(token.__dict__).encode("utf-8"), (other_node.ip, other_node.port))

    def probe(self, token):
        if not (self.is_leader or self.is_leader_known) and self.is_active:
            self.send_to_left(token)
            self.send_to_right(token)

    def relay_forward(self, token: Token):
        if self.is_active:
            if token.direction == "CLOCKWISE":
                self.send_to_left(token)

            elif token.direction == "ANTICLOCKWISE":
                self.send_to_right(token)

    def reply(self, token: Token):
        if self.is_active:
            token.type_ = "REPLY"
            if token.direction == "CLOCKWISE":
                token.direction = "ANTICLOCKWISE"
                self.send_to_right(token)

            elif token.direction == "ANTICLOCKWISE":
                token.direction = "CLOCKWISE"
                self.send_to_left(token)

    def run(self):
        print("process ", self.pid, "starting...")

        if not self.is_active:
            self.receive_socket.bind((self.receive_ip, self.receive_port))
            self.probe(token=Token(src_pid=self.pid, type_="PROBE", max_hop_count=(2 ** self.round), hop_count=0))

        while self.is_active:
            payload, _ = self.receive_socket.recvfrom(2048)
            self.is_active = self.transition(data=payload)

        self.receive_socket.close()

    def transition(self, data):
        if data == b'':
            return False
        # print("================================")
        # print("receiving message at ", time.strftime("%I:%M:%S %p", time.localtime()))

        message = Token(**json.loads(data))

        print("payload: ", message.type, " ", message.value)
        print("================================")
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


def main():
    # Node class contains two sockets: receive and send
    # The first one listens for messages and the second one sends them
    # If the Node is the initializer of the conversation then it binds the sockets with to ports
    # otherwise the node just connects to the sockets
    # Every node is on a separate thread waiting for a message to reply

    # getting input form
    sr, ip1, port1, ip2, port2 = input().split()

    if sr == "r":
        node = Node(ip1, int(port1), ip2, int(port2))
        node.bind()
        print("process starting as receiver...")
        node.start()

    elif sr == "s":
        node = Node(ip1, int(port1), ip2, int(port2))
        node.connect()
        print("process starting as sender...")
        node.start()
        node.send_message("start", "hello")

    else:
        print("Invalid type identifier")


if __name__ == "__main__":
    main()

# First start the receiver than the sender
# to start a process as a receiver : r 127.0.0.1 54329 127.0.0.1 54328
# to start a process as a sender : s 127.0.0.1 54328 127.0.0.1 54329
