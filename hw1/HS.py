# Written by Farzin Nasiri
# Distributed Systems 2021
import time
from socket import *
import json
import threading

lock = threading.Lock()


def log(*args):
    lock.acquire()
    print(time.strftime("%I:%M:%S %p", time.localtime()), " ", *args)
    lock.release()


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
        self.current_rounds_replies = 0

        self.receive_socket = socket(AF_INET, SOCK_DGRAM)

    def set_left_node(self, ip, port):
        self.left_node = NeighbourNode(ip, port)
        self.send_left_socket = socket(AF_INET, SOCK_DGRAM)

    def set_right_node(self, ip, port):
        self.right_node = NeighbourNode(ip, port)
        self.send_right_socket = socket(AF_INET, SOCK_DGRAM)

    def send_to_left(self, token: Token):
        token.direction = "CLOCKWISE"
        threading.Thread(target=self.send(channel=self.send_left_socket, other_node=self.left_node, token=token)) \
            .start()

    def send_to_right(self, token: Token):
        token.direction = "ANTICLOCKWISE"
        threading.Thread(target=self.send(channel=self.send_right_socket, other_node=self.right_node, token=token)) \
            .start()

    def send(self, channel: socket, other_node: NeighbourNode, token: Token):
        log(self.pid, ": sending a token of type ", token.type_,
            " from ", token.src_pid, " in ",
            token.direction, "direction")

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
                self.send_to_right(token)

            elif token.direction == "ANTICLOCKWISE":
                self.send_to_left(token)

    def run(self):
        log("process ", self.pid, " starting...")

        if not self.is_active: # first round probing

            self.receive_socket.bind((self.receive_ip, self.receive_port))
            self.is_active = True

            self.probe(token=Token(src_pid=self.pid, type_="PROBE", direction=None, max_hop_count=(2 ** self.round),
                                   hop_count=0))

        while self.is_active:
            # listening for incoming tokens
            payload, _ = self.receive_socket.recvfrom(2048)
            self.transition(data=payload)

        self.end_process()

    def transition(self, data):
        if data == b'':
            return False

        token = Token(**json.loads(data))
        log(self.pid, ": a token of type ", token.type_,
            " received from ", token.src_pid, " in ", token.direction, " direction")

        token.hop_count += 1

        if token.type_ == "PROBE":
            if token.src_pid == self.pid:  # leader is found
                # process should announces itself as the leader to other nodes
                if not (self.is_leader or self.is_leader_known):
                    self.is_leader = True
                    self.is_leader_known = True
                    self.leader_pid = self.pid

                    log("process ", self.pid, ": leader is ",
                        self.leader_pid, " ANNOUNCING the leader now")

                    self.send_to_right(Token(src_pid=self.pid, type_="ANNOUNCEMENT",
                                             direction=None,
                                             max_hop_count=2 ** self.round,
                                             hop_count=0))
            elif token.src_pid > self.pid:
                if token.hop_count < token.max_hop_count:
                    self.relay_forward(token)
                else:
                    token.type_ = "REPLY"
                    self.reply(token)
            else:
                # token is discarded
                pass

        elif token.type_ == "REPLY":
            if token.src_pid != self.pid:
                self.relay_forward(token)
            else:  # a REPLY from a previous probing is returned
                self.current_rounds_replies += 1
                if self.current_rounds_replies == 2:  # process goes to the next round (two tokens have arrived)
                    self.current_rounds_replies = 0
                    self.round += 1
                    self.probe(
                        token=Token(src_pid=self.pid, type_="PROBE",
                                    direction=None,
                                    max_hop_count=(2 ** self.round),
                                    hop_count=0))

        elif token.type_ == "ANNOUNCEMENT":
            if self.pid != token.src_pid and not self.is_leader_known:
                # a NON-LEADER process identifies the leader

                self.is_leader_known = True
                self.leader_pid = token.src_pid
                self.relay_forward(token)

                log(self.pid, ": leader is ",
                    self.leader_pid)

            elif self.pid == token.src_pid and self.is_leader:
                # the LEADER gets its announcement back which means all other processes now know the leader

                log("election completed with process ",
                    self.leader_pid, " elected as the leader")

            self.is_active = False

    def end_process(self):
        log("process ", self.pid, " stopping...")
        time.sleep(5)
        self.receive_socket.close()


def main():
    n = int(input())

    nodes = []

    port = 8000

    for i in range(n):
        pid, delay = input().split()
        temp_port = port + i

        node = Node(int(pid), "localhost", temp_port, int(delay))

        nodes.append(node)

    # setting the right/left neighbours of each node to establish connection
    for i, node in enumerate(nodes):
        right_port = port + (i + n - 1) % n
        left_port = port + (i + 1) % n

        node.set_right_node("localhost", right_port)
        node.set_left_node("localhost", left_port)

    print(time.strftime("%I:%M:%S %p", time.localtime()), " Starting Election...")
    for node in nodes:
        node.start()

    for node in nodes:
        node.join()

    print(time.strftime("%I:%M:%S %p", time.localtime()), "Exiting program...")


if __name__ == "__main__":
    main()
