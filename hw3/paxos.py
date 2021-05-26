# Written by Farzin Nasiri
# Distributed Systems 2021
import math
import time
from socket import *
import json
import random
import threading
from datetime import datetime

lock = threading.Lock()

# node states
LEADER = "LEADER"
POTENTIAL_LEADER = "POTENTIAL_LEADER"
ACCEPTOR = "ACCEPTOR"
UNKNOWN = "UNKNOWN"

# message types
PREPARE = "PREPARE"
V_PROPOSE = "V_PROPOSE"
V_DECIDE = "V_DECIDE"

POTENTIAL_LEADER_ACK = "POTENTIAL_LEADER_ACK"
V_PROPOSE_ACK = "V_PROPOSE_ACK"


def log(nid: int, *args):
    lock.acquire()
    print(time.strftime("%I:%M:%S %p ", time.localtime()), "node ", nid, ": ", *args)
    lock.release()


class Message:
    def __init__(self, nid, type_, value):
        self.nid = nid
        self.type_ = type_
        self.value = value


class NeighbourNode:
    def __init__(self, id_: int, ip, port, delay: float):
        self.id = id_

        self.ip = ip
        self.port = port

        self.channel = socket(AF_INET, SOCK_DGRAM)

        self.delay = delay


class Node(threading.Thread):
    def __init__(self, id_: int, ip, port, network_size: int,
                 startup_delay: float, phase1_timer: float,
                 phase2_timer: float):

        threading.Thread.__init__(self)

        self.id = id_
        self.ip = ip
        self.port = port

        self.neighbours = []

        self.is_active = False
        self.state = ""

        self.network_size = network_size

        self.startup_delay = startup_delay
        self.phase1_timer = phase1_timer
        self.phase2_timer = phase2_timer

        self.leader_id = 0

        self.proposing_value = -1
        self.phase = 0

        self.leader_ids = [0]
        self.value_proposed_leader_ids = []

        self.chosen_value = -1

        self.count_promises = 0
        self.count_accepts = 0

        self.is_decided = False

        self.receive_socket = socket(AF_INET, SOCK_DGRAM)

    def add_neighbour(self, node: NeighbourNode):
        self.neighbours.append(node)

    def broadcast(self, message: Message):
        log(self.id, "broadcasting message of type", message.type_,
            "with value", message.value)

        for neighbour in self.neighbours:
            threading.Thread(
                target=self.send_to_node, args=(neighbour, message)).start()

    def send_to_node(self, receiver_node: NeighbourNode, message: Message):
        log(self.id, "sending a message of type", message.type_,
            "with value", message.value,
            "to", receiver_node.id)

        time.sleep(receiver_node.delay)
        receiver_node.channel.sendto(json.dumps(message.__dict__).encode("utf-8"),
                                     (receiver_node.ip, receiver_node.port))

    def send_to(self, receiver_node_id: int, message: Message):
        receiver_node = None

        for neighbour in self.neighbours:
            if neighbour.id == receiver_node_id:
                receiver_node = neighbour
                break

        threading.Thread(
            target=self.send_to_node, args=(receiver_node, message)).start()

    def run(self):
        log(self.id, "starting...")

        if not self.is_active:
            self.receive_socket.bind((self.ip, self.port))
            self.is_active = True
            self.state = POTENTIAL_LEADER

            threading.Thread(target=self.prepare).start()

        while self.is_active:
            # listening for incoming messages
            payload, _ = self.receive_socket.recvfrom(2048)
            self.transition(data=payload)

        self.stop()

    def prepare(self):
        if self.is_active and self.state == POTENTIAL_LEADER:

            time.sleep(self.startup_delay)

            if self.is_active and not self.is_decided:
                self.state = POTENTIAL_LEADER

                self.leader_id = max(self.leader_ids) + 1
                self.count_promises += 1

                self.broadcast(Message(nid=self.id,
                                       type_=POTENTIAL_LEADER,
                                       value=self.leader_id))

                time.sleep(self.phase1_timer)

                if self.is_active and self.state != LEADER:
                    self.state = ACCEPTOR

    def propose(self):
        if self.is_active and self.state == LEADER:
            if len(self.value_proposed_leader_ids) == 0:
                self.chosen_value = self.id * self.network_size
            else:
                self.chosen_value = \
                    max(self.value_proposed_leader_ids,
                        key=lambda i: i[0])[1]

            self.count_accepts += 1

            self.broadcast(Message(nid=self.id,
                                   type_=V_PROPOSE,
                                   value="%s,%s" % (self.leader_id,
                                                    self.chosen_value)))
            time.sleep(self.phase2_timer)

            if self.is_active:
                self.state = ACCEPTOR

    def decide(self):
        if self.is_active and self.state == LEADER and self.is_decided:
            log(self.id, "decided on value ", self.chosen_value)
            self.broadcast(Message(nid=self.id,
                                   type_=V_DECIDE,
                                   value=self.chosen_value))
            self.is_active = False
            self.stop()

    def transition(self, data):
        if data == b'':
            return False

        message = Message(**json.loads(data))

        log(self.id,
            "receiving a message of type", message.type_,
            "with value of", message.value,
            "from", message.nid)

        if message.type_ == POTENTIAL_LEADER:

            leader_id = int(message.value)

            if leader_id > self.leader_id and (self.state == LEADER or self.state == POTENTIAL_LEADER):
                self.state = ACCEPTOR

            if leader_id > max(self.leader_ids) and self.state == ACCEPTOR:
                self.leader_ids.append(leader_id)

                max_leader_id_proposed, proposed_value = 0, -1

                if len(self.value_proposed_leader_ids) > 0:
                    max_leader_id_proposed, proposed_value = \
                        max(self.value_proposed_leader_ids,
                            key=lambda i: i[0])[0:]

                self.send_to(message.nid,
                             Message(nid=self.id,
                                     type_=POTENTIAL_LEADER_ACK,
                                     value="%s,%s" % (
                                         max_leader_id_proposed,
                                         proposed_value
                                     )))

        if message.type_ == V_PROPOSE and self.state == ACCEPTOR:
            leader_id, proposed_value = [int(x) for x in message.value.split(",")]

            self.value_proposed_leader_ids.append(tuple((leader_id, proposed_value)))

            if leader_id == max(self.leader_ids):
                self.chosen_value = proposed_value

                self.send_to(message.nid,
                             Message(nid=self.id,
                                     type_=V_PROPOSE_ACK,
                                     value="-1"))

        if message.type_ == V_DECIDE:
            chosen_value = int(message.value)
            self.chosen_value = chosen_value

            log(self.id, "decided on value ", self.chosen_value)
            self.is_decided = True
            self.is_active = False

        if message.type_ == POTENTIAL_LEADER_ACK and self.state == POTENTIAL_LEADER:
            self.count_promises += 1

            max_leader_id_proposed, proposed_value = [int(x) for x in message.value.split(",")]

            if proposed_value != -1:
                self.value_proposed_leader_ids.append(tuple((max_leader_id_proposed, proposed_value)))

            if self.count_promises >= math.floor(self.network_size / 2) + 1 and self.state == POTENTIAL_LEADER:
                self.state = LEADER
                threading.Thread(target=self.propose).start()

        if message.type_ == V_PROPOSE_ACK and self.state == LEADER:
            self.count_accepts += 1

            if not self.is_decided and self.count_accepts >= math.floor(
                    self.network_size / 2) + 1 and self.state == LEADER:
                self.is_decided = True
                threading.Thread(target=self.decide).start()

    def stop(self):
        log(self.id, "stopping...")
        time.sleep(1)
        self.receive_socket.close()


def main():
    nodes = []

    port = 9000

    n = int(input())

    for i in range(n):

        nid, startup_delay, phase1_timer, phase2_timer = \
            [float(x) for x in input().split()]
        nid = int(nid)

        node = Node(nid, "localhost", port + nid, n, startup_delay,
                    phase1_timer, phase2_timer)

        for j in range(n - 1):
            neighbour_nid, delay = [float(x) for x in input().split()]
            neighbour_nid = int(neighbour_nid)
            node.add_neighbour(NeighbourNode(neighbour_nid, "localhost",
                                             port + neighbour_nid, delay))

        nodes.append(node)

    print(time.strftime("%I:%M:%S %p", time.localtime()), " Starting PAXOS...")

    # for node in nodes:
    #     for nn in node.neighbours:
    #         print(node.id, "->", nn.id," : ",nn.delay)

    for node in nodes:
        node.start()

    for node in nodes:
        node.join()

    print(time.strftime("%I:%M:%S %p", time.localtime()), "Exiting program...")


if __name__ == "__main__":
    main()
