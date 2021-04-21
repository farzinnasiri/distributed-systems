# Written by Farzin Nasiri
# Distributed Systems 2021
import time
from socket import *
import json
import random
import threading
from datetime import datetime

lock = threading.Lock()

WINNER = "WINNER"
LOSER = "LOSER"
UNKNOWN = "UNKNOWN"

ANNOUNCEMENT = "ANNOUNCEMENT"
COMPARISON = "COMPARISON"


def log(pid: int, *args):
    lock.acquire()
    print(time.strftime("%I:%M:%S %p ", time.localtime()), "process ", pid, ": ", *args)
    lock.release()


class Message:
    def __init__(self, src_pid, type_, value):
        self.src_pid = src_pid
        self.type_ = type_  # type can be: ANNOUNCEMENT, COMPARISON
        self.value = value  # value can be: WINNER, LOSER, UNKNOWN or a random integer


class NeighbourNode:
    def __init__(self, id_: int, ip, port, channel_delay: int):
        self.id = id_

        self.ip = ip
        self.port = port

        self.send_channel = socket(AF_INET, SOCK_DGRAM)

        self.channel_delay = channel_delay


class Node(threading.Thread):
    def __init__(self, id_, receive_ip, receive_port, round_timer:int):
        threading.Thread.__init__(self)

        self.id = id_
        self.receive_ip = receive_ip
        self.receive_port = receive_port

        self.neighbours = []

        self.is_active = False
        self.status = UNKNOWN

        self.network_size = 0
        self.values = []

        self.current_value = 0

        self.round = 0
        self.count_neighbours_messages = 0

        self.round_timer = round_timer

        self.receive_socket = socket(AF_INET, SOCK_DGRAM)

    def set_network_size(self, n: int):
        self.network_size = n
        self.values = range(1, n ** 4)

    def get_random_value(self):
        random.seed(datetime.now())
        return random.sample(self.values, 1)[0]

    def add_neighbour(self, node: NeighbourNode):
        self.neighbours.append(node)

    def remove_neighbour(self, id_: int):
        for i, neighbour in enumerate(self.neighbours):
            if neighbour.id == id_:
                self.neighbours.pop(i)
                break

    def broadcast_to_neighbours(self, message: Message):
        log(self.id, "broadcasting message of type", message.type_,
            "with value", message.value)

        for neighbour in self.neighbours:
            threading.Thread(
                target=self.send, args=(neighbour.send_channel,
                                        neighbour, message)).start()

    def find_winner(self):
        while self.is_active:
            if len(self.neighbours) == 0:  # if a node becomes single, its a winner
                self.is_active = False
                self.status = WINNER
                log(self.id, "WINNER")

                self.end_process()
                break

            self.count_neighbours_messages = 0
            self.current_value = self.get_random_value()
            self.round += 1

            log(self.id, "starting stage", self.round,
                "with value", self.current_value)

            self.broadcast_to_neighbours(message=
                                         Message(src_pid=self.id,
                                                 type_=COMPARISON,
                                                 value=self.current_value))

            time.sleep(self.round_timer)

    def send(self, channel: socket, receiver: NeighbourNode, message: Message):
        log(self.id, "sending a message of type", message.type_,
            "with value", message.value,
            "to", receiver.id)

        time.sleep(receiver.channel_delay)
        channel.sendto(json.dumps(message.__dict__).encode("utf-8"), (receiver.ip, receiver.port))

    def run(self):
        log(self.id, "starting...")

        # first round starting mechanism
        if not self.is_active:
            self.receive_socket.bind((self.receive_ip, self.receive_port))
            self.is_active = True

            threading.Thread(target=self.find_winner).start()

        while self.is_active:
            # listening for incoming messages
            payload, _ = self.receive_socket.recvfrom(2048)
            self.transition(data=payload)

        self.end_process()

    def transition(self, data):
        if data == b'':
            return False

        message = Message(**json.loads(data))
        log(self.id,
            "receiving a message of type", message.type_,
            "with value of", message.value,
            "from", message.src_pid)

        if message.type_ == ANNOUNCEMENT:
            if message.value == WINNER:  # if a neighbour is a WINNER, all incident nodes become losers
                log(self.id, "LOSER")
                self.status = LOSER
                self.broadcast_to_neighbours(Message(src_pid=self.id,
                                                     type_=ANNOUNCEMENT,
                                                     value=LOSER))

                self.is_active = False

            elif message.value == LOSER:
                self.remove_neighbour(message.src_pid)

        elif message.type_ == COMPARISON:
            if self.current_value > int(message.value):
                self.count_neighbours_messages += 1

            # if a node is the local maximum, than it becomes a winner
            if self.count_neighbours_messages >= len(self.neighbours):
                log(self.id, "WINNER")
                self.status = WINNER
                self.broadcast_to_neighbours(Message(src_pid=self.id,
                                                     type_=ANNOUNCEMENT,
                                                     value=WINNER))

                self.is_active = False

    def end_process(self):
        log(self.id, "stopping...")
        time.sleep(1)
        self.receive_socket.close()


def check_exists(nodes: list, id_: int):
    for node in nodes:
        if node.id == id_:
            return True, node
    return False, None


def main():

    # for getting input from files, uncomment the commented codes

    # file = open("tests/test1.txt", "r")

    # timer = int(file.readline())

    timer = int(input())

    nodes = []

    port = 8000

    n = 0
    while True:
        # inputs = file.readline()
        inputs = input()

        if inputs == "\n" or not inputs:
            break

        id1, id2, delay = inputs.split()
        id1 = int(id1)
        id2 = int(id2)
        delay = int(delay)

        exists, node = check_exists(nodes, id1)

        if exists:
            node.add_neighbour(NeighbourNode(id2, "localhost", port + id2, delay))

        else:
            n += 1
            node = Node(id1, "localhost", port + id1, timer)
            node.add_neighbour(NeighbourNode(id2, "localhost", port + id2, delay))
            nodes.append(node)

        exists, node = check_exists(nodes, id2)

        if exists:
            node.add_neighbour(NeighbourNode(id1, "localhost", port + id1, delay))

        else:
            n += 1
            node = Node(id2, "localhost", port + id2, timer)
            node.add_neighbour(NeighbourNode(id1, "localhost", port + id1, delay))
            nodes.append(node)

    for node in nodes:
        node.set_network_size(n)

    print(time.strftime("%I:%M:%S %p", time.localtime()), " Starting MIS...")

    for node in nodes:
        node.start()

    for node in nodes:
        node.join()

    print(time.strftime("%I:%M:%S %p", time.localtime()), "Exiting program...")


if __name__ == "__main__":
    main()
