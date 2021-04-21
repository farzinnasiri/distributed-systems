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
    def __init__(self, id_, ip, port, channel_delay):
        self.id = id_

        self.ip = ip
        self.port = port

        self.send_channel = socket(AF_INET, SOCK_DGRAM)

        self.channel_delay = channel_delay


class Node(threading.Thread):
    def __init__(self, id_, receive_ip, receive_port, stage_timer):
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

        self.stage = 0
        self.count_neighbours_messages = 0

        self.stage_timer = stage_timer

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
        # log(self.id, "removing", id_)
        # log(self.id, len(self.neighbours))
        for i, neighbour in enumerate(self.neighbours):
            # log(self.id, neighbour.id)
            if neighbour.id == id_:
                # log(self.id, "found it")
                self.neighbours.pop(i)
                break
        # log(self.id, len(self.neighbours))

    def broadcast_to_neighbours(self, message: Message):
        log(self.id, "broadcasting message of type", message.type_,
            "with value", message.value)

        for neighbour in self.neighbours:
            threading.Thread(
                target=self.send, args=(neighbour.send_channel,
                                        neighbour, message)).start()

    def find_winner(self):
        while self.is_active:
            if len(self.neighbours) == 0:
                self.is_active = False
                self.status = WINNER
                log(self.id, "WINNER")

                self.end_process()
                break

            self.count_neighbours_messages = 0
            self.current_value = self.get_random_value()
            self.stage += 1

            log(self.id, "starting stage", self.stage,
                "with value", self.current_value)

            self.broadcast_to_neighbours(message=
                                         Message(src_pid=self.id,
                                                 type_=COMPARISON,
                                                 value=self.current_value))

            time.sleep(self.stage_timer)

    def send(self, channel: socket, receiver: NeighbourNode, message: Message):
        log(self.id, "sending a message of type", message.type_,
            "with value", message.value,
            "to", receiver.id)

        time.sleep(receiver.channel_delay)
        channel.sendto(json.dumps(message.__dict__).encode("utf-8"), (receiver.ip, receiver.port))

    def run(self):
        log(self.id, "starting...")

        if not self.is_active:
            self.receive_socket.bind((self.receive_ip, self.receive_port))
            self.is_active = True

            threading.Thread(target=self.find_winner).start()

        while self.is_active:
            # listening for incoming tokens
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
            if message.value == WINNER:
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
    file = open("./input.txt", "r")

    timer = int(file.readline())

    nodes = []

    port = 8000

    n = 0
    while True:
        inputs = file.readline()

        # print(inputs)

        if inputs == "\n":
            break

        id1, id2, delay = inputs.split()

        exists, node = check_exists(nodes, int(id1))

        if exists:
            node.add_neighbour(NeighbourNode(int(id2), "localhost", port + int(id2), int(delay)))

        else:
            n += 1
            node = Node(int(id1), "localhost", port + int(id1), int(timer))
            node.add_neighbour(NeighbourNode(int(id2), "localhost", port + int(id2), int(delay)))
            nodes.append(node)

        exists, node = check_exists(nodes, int(id2))

        if exists:
            node.add_neighbour(NeighbourNode(int(id1), "localhost", port + int(id1), int(delay)))

        else:
            n += 1
            node = Node(int(id2), "localhost", port + int(id2), int(timer))
            node.add_neighbour(NeighbourNode(int(id1), "localhost", port + int(id1), int(delay)))
            nodes.append(node)

    # print(n)
    # for i, node in enumerate(nodes):
    #     print("source")
    #     print(node.id, " ",node.receive_port)
    #     print("neighbours")
    #     for nei in node.neighbours:
    #         print(nei.id, " ", nei.channel_delay)

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
