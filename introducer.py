import socket
import threading
import json
import random
import logging
import time
import bisect
from collections import deque

class introducer:
    def __init__(self):
        self.ip = 'fa18-cs425-g26-01.cs.illinois.edu'
        self.ID = self.ip + '#' + str(time.time())
        self.membership_list = []
        self.message_list = deque([]) # a list consist of ID+operation(join/fail/leave)
        self.message_list_mutex = threading.Lock()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("0.0.0.0", 7003))	
        self.listener_sock = sock
        logging.basicConfig(filename='vm.log', level=logging.INFO)

    ''' message list stores recnet messages and is cleared periodly '''
    def message_list_maintainer(self):
        while True:
            while len(self.message_list) > 0:
            # the period depends on scale
                if time.time() - self.message_list[0][1] > 6000:
                    self.message_list_mutex.acquire()
                    self.message_list.popleft()
                    self.message_list_mutex.release()
                else:
                    break

    ''' handle different types of operations for message '''
    def message_handler(self, message):
        message = json.loads(message)

        ''' for demo, p means print to terminal '''
        if "p" in message:
            instruction = message['p']
            if instruction == "i":
                print(self.ID)
            elif instruction == "m":
                print(self.membership_list)
            else:
                print('Introducer doesn\'t have ping list.')
            return
	
        for key in message.keys():
            if message['t'] < 1:
                return
            if key != "t":
                ID = message[key]
                break
	
        ''' filt the messages '''
        incoming_message = ID + ' ' + key # key is join/fail/leave
        for stored_message in self.message_list:
            if incoming_message == stored_message[0]:
                return
        self.message_list.append([incoming_message, time.time()])
        applicant_ip = ID.split('#')[0]

        ''' join operation, update membership list, log, reply and forward message if needed '''
        if 'j' in message:            
            ''' forward join message '''
            for iii in range(3):
                message['t']-=1
                if message['t'] < 1:
                    break
                chosen_list = [-1]; index = -1
                group_size = len(self.membership_list)
                num_to_sent = min(3, group_size)
                while num_to_sent > 0:
                    while index in chosen_list:
                        index = random.randint(0, group_size-1)
                    contact_ip = self.membership_list[index].split('#')[0]
                    try:
                        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                        sock.connect((contact_ip, 7003))
                        sock.send(json.dumps(message).encode())
                        sock.close() 
                        num_to_sent -=1
                        chosen_list.append(index)
                        break
                    except:
                        chosen_list.append(index)
            
            ''' update membership list '''
            bisect.insort(self.membership_list, ID)
            
            ''' logging '''
            logging.info(applicant_ip + ' joined at introducer time ' + str(time.time()) + ' with ID: ' + ID)
        
        #when someone leaves
        elif 'l' in message:
            ''' update membership list and log'''
            member = ""
            for member in self.membership_list:
                if member.startswith(ID):
                    self.membership_list.remove(member)
                    break
            logging.info(applicant_ip + ' left at introducer time ' + str(time.time()) + ' with ID: ' + member)
            
        #when someone fails
        elif 'f' in message:
            ''' update membership list and log'''
            member = ""
            for member in self.membership_list:
                if member.startswith(ID):
                    self.membership_list.remove(member)
                    break
            logging.info(applicant_ip + ' failed at introducer time ' + str(time.time()) + ' with ID: ' + member)


    def listen(self):
        ''' listen for join/leave/fail '''
        max_data = 8192
        while True:
            message = ''
            while True:
                data = self.listener_sock.recv(max_data)
                message += str(data.decode())
                if len(data) == 0:
                    break
                elif len(data) != max_data and message.endswith("}"):
                    break
            self.message_handler(message)

if __name__ == '__main__':
    intro = introducer()
    threading.Thread(target=intro.message_list_maintainer).start()
    threading.Thread(target=intro.listen).start()
    
