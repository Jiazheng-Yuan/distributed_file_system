import socket
import threading
import json
import random
import logging
import time
import os
import sys
from collections import deque
import bisect

class Peer:
    def __init__(self,ip):
        self.id = ip #id is initialized as ip of the machine, later will be changed when joining
        self.member_ship_list_mutex = threading.Lock()
        self.ping_list_mutex = threading.Lock()
        self.message_list_mutex = threading.Lock()
        self.introducer = "10.193.243.84"
        self.member_ship_list = []
        self.rand = random.seed()
        self.message_list = deque([])#keep recently received messages to avoid duplication
        self.ping_list = []#list of node to ping
        self.message_map = {'f': "failed", "l": "leave", "j": "join", "i": "id", 't': "ttl",'q': "quit", "p": "print"}
        #self.tcp_list = {}
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock1 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock1.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        #print(self.id)
        sock1.bind((self.id, 7004))
        sock.bind((self.id, 7005))
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock2 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock2.bind(("0.0.0.0", 7003))
        sock.settimeout(0.8)
        self.ack_receiving_sock = sock # socket for receiving ack from node it pinged
        self.ping_response_sock = sock1 # socket for
        self.listener_sock = sock2 #listen to join ,fail and other command messages
        self.extra_check = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        self.extra_check.bind(("0.0.0.0",5004))
        self.extra_check_receive = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        self.extra_check_receive.bind(("0.0.0.0",5005))
        self.extra_check_receive.settimeout(0.8)

        logging.basicConfig(filename='vm.log', level=logging.INFO)
        #threading.Thread(target=self.double_check).start()
    '''    
    def double_check(self):
        max_data = 8192
        while True:
            
            while True:
                message = ""
                data = self.extra_check.recv(max_data)
                message += str(data.decode())
                if len(data) == 0:
                    break
                elif len(data) != max_data and message.endswith("}"):
                    break
    '''


    def get_member_ship_list(self):
        return self.member_ship_list
    #pop any message added 5 seconds ago

    def message_list_maintainer(self):
        while True:
            while len(self.message_list) > 0:
                    if time.time() - self.message_list[0][1] > 10:
                        self.message_list_mutex.acquire()
                        self.message_list.popleft()
                        self.message_list_mutex.release()
                    else:
                        break

    #listen for any command, fail, leave and other machine's id
    def listen_on_7003(self):
        max_data = 8192
        while True:
            message = ""
            while True:
                data = self.listener_sock.recv(max_data)
                message += str(data.decode())
                if len(data) == 0:
                    break
                elif len(data) != max_data and message.endswith("}"):
                    break
            self.message_handler(message)

    # listen for any command,fail, leave and other machine's id
    def start(self):
        threading.Thread(target=self.check_heartbeat).start()
        threading.Thread(target=self.message_list_maintainer).start()
        threading.Thread(target=self.listen_ping_and_reply,args=(self.ping_response_sock, 7005)).start()
        threading.Thread(target=self.listen_ping_and_reply, args=(self.extra_check,5005)).start()
        self.join()
        threading.Thread(target=self.listen_on_7003).start()

    #spread received message,each time send message to the first element in ping list,
    # which is the member after itself in the membership list, and other 2 random nodes.
    def spread(self, message):
        ttl = message["t"]#number of times to spread it, actually is t in epidemic gossip style
        if ttl > 0:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.connect((self.introducer, 7003))
            sock.send(json.dumps(message).encode())
            sock.close()
        while ttl > 0:
            #get one element in the ping list
            chosen = {}
            ttl -= 1
            #this number coulb be changed according to the number of members
            adjustable_num_neighbor = 2
            self.ping_list_mutex.acquire()
            if len(self.ping_list) > 0:
                chosen[self.ping_list[0]] = 1
            self.ping_list_mutex.release()
            membership_num = len(self.member_ship_list)

            #choose 2 other random node each round
            for i in range(min(adjustable_num_neighbor,membership_num - 1)):
                message_sent = False
                while not message_sent:
                    try:
                        self.member_ship_list_mutex.acquire()
                        self.member_ship_list_mutex.release()
                        self.member_ship_list_mutex.acquire()
                        index = -1
                        #only have itself, no spread
                        if len(self.member_ship_list) <= 1:
                            self.member_ship_list_mutex.release()
                            return
                        while index in chosen or index < 0 or self.member_ship_list[index] == self.id:
                            index = random.randint(0, len(self.member_ship_list) - 1)
                        chosen[index] = 1
                        contact_ip = self.member_ship_list[index].split("#")[0]
                        self.member_ship_list_mutex.release()
                        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                        sock.connect((contact_ip, 7003))
                        message["t"] = ttl
                        sock.send(json.dumps(message).encode())
                        sock.close()
                        message_sent = True
                    except:
                        #if message was not sent, then chose another node to resend
                        message_sent = False

    #given an ip, remove the corresponding id in the membership list
    def remove_ip_from_membership_list(self,ip):
        self.member_ship_list_mutex.acquire()
        for i in range(len(self.member_ship_list)):
            if self.member_ship_list[i].startswith(ip):
                ans = self.member_ship_list.pop(i)
                self.member_ship_list_mutex.release()
                return ans
        ans = ""
        self.member_ship_list_mutex.release()
        return ans

    #listen to the ping by other threads and reply ack
    def listen_ping_and_reply(self, input_sock,reply_port):
        while True:
            data = input_sock.recv(1024).decode()
            reply_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            reply_sock.connect((data, reply_port))
            reply = self.id.split("#")[0]
            reply_sock.send(reply.encode())
            reply_sock.close()

    #receive ack from the nodes it pinged and remove any node did not respond in time. False positive is possible
    def receiving_ack(self, pinged_list, start_time,input_socket):

        while True:
            if time.time() - start_time >= 1 or len(pinged_list) == 0:
                break
            try:
                new_packet = input_socket.recv(1024).decode()
                if new_packet in pinged_list:
                    pinged_list.remove(new_packet)
            except:
                logging.debug("some process might failed")




    #every 0.1 seconds ping the nodes on ping list.
    def check_heartbeat(self):
        while True:
            time.sleep(0.1)
            pinged_list = []
            sub_ping_list = []
            self.ping_list_mutex.acquire()
            for id in self.ping_list:
                sub_ping_list.append(id)
            self.ping_list_mutex.release()

            for id in sub_ping_list:
                ip = id.split("#")[0]
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

                try:
                    sock.connect((ip, 7004))
                    sock.send(self.id.split("#")[0].encode())#send its own ip to the node on ping list, so they can reply back.

                    #sock1.send(self.id.split("#")[0].encode())
                    pinged_list.append(ip)

                except:
                    print("something happened")
                    self.member_ship_list_mutex.acquire()
                    already_gone = True
                    if id in self.member_ship_list:
                        self.member_ship_list.remove(id)
                        already_gone = False
                    self.member_ship_list_mutex.release()
                    if not already_gone:
                        adjustable_num_neighbor = 6
                        message = {"f": ip, "t": adjustable_num_neighbor}
                        self.message_handler(json.dumps(message))
            #after pinging, starts receiving messages
            pinged_list_7005 = [ip for ip in pinged_list]
            pinged_list_5005 = pinged_list_7005

            if not len(pinged_list_7005) == 0:
                for ip in pinged_list_5005:
                    sock1 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    sock1.connect((ip, 5004))
                    sock1.send(self.id.split("#")[0].encode())
                    self.receiving_ack(pinged_list_5005, time.time(), self.extra_check_receive)
                self.receiving_ack(pinged_list_7005, time.time(), self.ack_receiving_sock)
            for ip in list(set(pinged_list_5005) & set(pinged_list_7005)):
                adjustable_num_neighbor = 6
                message = {"f": ip, "t": adjustable_num_neighbor}
                self.message_handler(json.dumps(message))
    #update ping list according to the new membership list, pick the next three index after itself in the
    #membership list, or the rest of the list if list size <= 4
    def update_ping_list(self):
        new_ping_list = []
        self.member_ship_list_mutex.acquire()
        for i in range(len(self.member_ship_list)):
            if self.member_ship_list[i] == self.id:
                for j in range(1, 4):
                    cur = self.member_ship_list[(i + j) % len(self.member_ship_list)]
                    if cur not in new_ping_list and cur != self.id:
                        new_ping_list.append(cur)
                break
        self.member_ship_list_mutex.release()
        self.ping_list_mutex.acquire()
        self.ping_list = new_ping_list
        self.ping_list_mutex.release()

    #deal with membership list and ping list according to message,
    # meaning of letters are in the map in the initializer
    def handle_membership_list_and_log(self,message):
        id = ""
        for key in message.keys():
            if key != "t":
                id = message[key]
                break
        if "j" in message or "i" in message:
            if "j" in message:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                host_address = (id.split("#")[0], 7003)
                sock.connect(host_address)
                reply = json.dumps({"i": self.id})
                for i in range(0, 5):
                    try:
                        sock.send(reply.encode())
                    except:
                        logging.debug("node "+self.id+" failed to send new joining node " +id.split("#")[0]+" it own id" )
                        pass
                sock.close()
                logging.info("add " + id + "at " + str(time.time()))

            self.member_ship_list_mutex.acquire()
            skip = False
            if id not in self.member_ship_list:
                self.member_ship_list.insert(bisect.bisect_left(self.member_ship_list,id),id)
            else:
                skip = True
            self.member_ship_list_mutex.release()
            if not skip:
                self.update_ping_list()
        elif "q" in message:
            adjustable_num_accord_membership_list = 6
            message = {"l": self.id, "t": adjustable_num_accord_membership_list}
            self.spread(message)
            logging.info(self.id + "quit at " + str(time.time()))
            exit(0)
        else:
            actual_id = self.remove_ip_from_membership_list(id)
            if actual_id != "":
                logging.info("removing " + actual_id + "at " + str(time.time()) + " because it "+self.message_map[key])
                self.update_ping_list()
    #handle message
    def message_handler(self,message):
        message = json.loads(message)
        if "p" in message:
            instruction = message['p']
            if instruction == "i":
                print(self.id)
            elif instruction == "p":
                for pingee in self.ping_list:
                    print(pingee)
            else:
                for member in self.member_ship_list:
                    print(member)
            return
        id = ""
        for key in message.keys():
            if key != "t":
                id = message[key]
                break
        #check message duplicate, if duplicate then return
        self.message_list_mutex.acquire()
        for message_and_time in self.message_list:
            temp_message = id + " " + key
            if message_and_time[0] == temp_message:
                self.message_list_mutex.release()
                return

        self.message_list.append([id + " " + key, time.time()])
        self.message_list_mutex.release()
        self.handle_membership_list_and_log(message)
        if "ack" not in message and "i" not in message and "h" not in message:
            self.spread(message)
    #initiate a join and send message to introducer
    def join(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect((self.introducer, 7003))
        now = str(time.time())
        logging.info(self.id + "join initiated at " + str(now))
        message = json.dumps({"j": self.id + '#'+str(now),"t": 6})
        self.id = self.id + "#"+now
        for i in range(0,6):
            sock.send(message.encode())
        self.message_list_mutex.acquire()
        self.message_list.append([self.id + " j", time.time()])
        self.message_list_mutex.release()
        self.member_ship_list_mutex.acquire()
        self.member_ship_list.append(self.id)
        self.member_ship_list_mutex.release()
        self.message_handler(message)


if __name__ == "__main__":
    host_name = socket.gethostname()
    p = Peer(host_name)
    p.start()

