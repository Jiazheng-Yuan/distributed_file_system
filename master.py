import socket
import json
import threading
import random
import time

class master:
    def __init__(self, membership_manager):
        self.padding = "jyuan18?yixinz6"
        self.host_name = socket.gethostname()
        self.op_listen_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.op_listen_socket.bind(("0.0.0.0", 6666))
        
        self.ack_listen_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.ack_listen_socket.bind(("0.0.0.0", 9999))

        self.op_listen_socket.listen(15)
        self.ack_listen_socket.listen(15)
        
        self.file_addr = {} # record filename:[addr list, True], if not True, can't read
        self.addr_file = {} # record addr:filename_list
        self.occupied = 0
        
        self.membership_list = membership_manager.member_ship_list
        #self.membership_list = {'10.193.200.176'}
        
        self.seq = 0 # the sequence number of op
        self.seq_dict = {0: 'init'} # records seq->client_connection
        self.ack_dict = {0: [0]} # seq -> expected_from_[ips]
        
        # open a thread for replica recovery
        threading.Thread(target=self.check_replica_remove).start()
        
        
    def copy_membership_list(self):
        mem_copy = [mem.split('#')[0] for mem in self.membership_list]
        return mem_copy
    
    def check_membership_list_len(self):
        return len(self.membership_list)
    
    def check_replica_remove(self):
        list_old = self.copy_membership_list()
        while True:
            len_old = len(list_old)
            len_new = self.check_membership_list_len()
            if len_new == len_old:
                continue
            list_new = self.copy_membership_list()
            test_start = time.time()
            print('len changed from {2} to {3};   membership list from {0} \n to {1}'.format(list_old, list_new, len_old, len_new))
            failed_mems = []
            for mem in list_old:
                if mem not in list_new:
                    failed_mems.append(mem)
            list_old = list_new
            if failed_mems != []:
                self.recover_failed_replicas(failed_mems, test_start)

    # For a failed node, for each file in addr_file[failed_node]:
    # make the sdfs_filename in file_addr -> False
    # random choose a new_ip, remove old addr
    # send a message to a random replica addr: {op: recover s:... old:failed_mem new:an_ip ips:[](for new_ip) seq:...}
    # wait for an ack from this new chosen replica addr, if timeout choose again
    # update file_addr[s...][0](change), addr_file[new_chosen_ip](add)
    # to all old replica addr: {op:change s:... old:failed_mem new:chosen_ip ips:[]}
    # one file finished: False -> True
    # all files finished, then del addr_file[failed_node]
    def recover_failed_replicas(self, failed_mems, test_start):
        for failed_mem in failed_mems:
            print('{0} failed'.format(failed_mem))
            if failed_mem not in self.addr_file:
                print('no file stored on {0}'.format(failed_mem))
                continue
            for sdfs_filename in self.addr_file[failed_mem]:
                #print('handling {0}'.format(sdfs_filename))
                #assert (self.file_addr[sdfs_filename][1] == True), "failed when writing!!!"
                self.file_addr[sdfs_filename][1] = False
                self.seq += 1
                my_seq = self.seq
                chosen_addrs = self.file_addr[sdfs_filename][0]
                chosen_addrs.remove(failed_mem)
                new_message = {'op': 'recover', 's': sdfs_filename, 'old': failed_mem, 'seq': my_seq}
                '''cot = 0
                while cot < 3: #################
                    if cot == 2:
                        while True:
                            pass
                    cot += 1'''
                while True:
                    print('trying recover {0} which used to locate at {1}'.format(sdfs_filename, failed_mem))
                    while True:
                        new_ip = random.sample(self.membership_list, 1)[0].split('#')[0]
                        if new_ip not in chosen_addrs:
                            break
                    new_message['new'] = new_ip
                    new_message['ips'] = [new_ip] + chosen_addrs
                    print('new chosen replica addr: ' + str(new_ip))
                    '''count = 0
                    while count<3:
                        if count == 2:
                            while True:
                                pass
                        count += 1 ##################'''
                    while True:
                        exec_addr = random.sample(chosen_addrs, 1)[0]
                        try:
                            sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                            sock.connect((exec_addr, 2333))
                            sock.send((json.dumps(new_message) + self.padding).encode())
                            sock.close()
                            print('in recovery, master sent <{0}>    to {1}'.format(new_message, exec_addr))
                            break
                        except Exception as e:
                            #print(e)
                            print('failed to send instr \'replicate {1}\' to old addr {0}'.format(exec_addr, sdfs_filename))
                            #while True:
                            #    pass
                    print('finish chosen exec_node: {0}'.format(exec_addr))
                    flag = False
                    self.ack_dict[my_seq] = [new_ip, exec_addr] # waiting ack from new replica addr or exec_node, if any one replies then good!
                    #start = time.time()
                    while True:
                        #print('waiting the recovery instr to finish')
                        #print('ack_dict: <{0}>,     my_seq: <{1}>'.format(self.ack_dict, my_seq))
                        tmp = self.ack_dict[my_seq]
                        #print('self.ack_dict[my_seq]: {0}'.format(tmp))
                        if tmp == []: # recovery finished
                            flag = True
                            break
                        membership_copy = self.copy_membership_list()
                        if new_ip not in membership_copy and exec_addr not in membership_copy:
                            print('new_ip ({0}) and exec_node ({1}) both failed'.format(new_ip, exec_addr))
                            break
                        '''if time.time() - start > 100:
                            print('replication timeout')
                            #while True:
                            #    pass
                            break'''
                    del self.ack_dict[my_seq]
                    if flag:
                        #print('recovery of {0} on failed {1} ok, now broadcasting updated messages'.format(sdfs_filename, failed_mem))
                        self.file_addr[sdfs_filename][0].append(new_ip)
                        if new_ip in self.addr_file:
                            self.addr_file[new_ip].append(sdfs_filename)
                        else:
                            self.addr_file[new_ip] = [sdfs_filename]
                        updated_message = {'op': 'change', 's': sdfs_filename,
                                          'ips': new_message['ips']}
                        updated_message = (json.dumps(updated_message) + self.padding).encode()
                        #print('in recovery, ready to send updated message <{0}>'.format(updated_message))
                        for chosen_addr in chosen_addrs:
                            try:
                                sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                                sock.connect((chosen_addr, 2333))
                                sock.send(updated_message)
                                sock.close()
                            except:
                                print('tring to send {0} to {1} and find it failed, ignore'.format(updated_message, chosen_addr))
                        print('finished send updated message <{0}> to <{1}>'.format(updated_message, chosen_addrs))
                        break
                self.file_addr[sdfs_filename][1] = True
            del self.addr_file[failed_mem]
            test_time = time.time() - test_start
            print('re replication time for failed mem {0} is {1}'.format(failed_mem, test_time))
    
    def check_writing_status(self, sdfs_filename):
        '''if sdfs_filename not in self.file_addr: # no such file, shouldn't happen
            return 2'''
        if self.file_addr[sdfs_filename][1]: # finish write
            return 0
        else: # writing
            return 1
    
    def wait_writing_finished(self, sdfs_filename, count):
        if sdfs_filename not in self.file_addr: 
            return 2 # no such file
        counter = 0; flag = True
        while counter < count:
            if self.check_writing_status(sdfs_filename) == 0:
                flag = False
                break
            counter += 1
        if flag: # writing
            return 1
        return 0 # finished
    
    def listen(self, sock, ack):
        max_data = 8192
        #print('start listen, sock: {0}, ack: {1}'.format(sock, ack))
        while True:
            message = ""
            connection, client_address = sock.accept()
            while True:
                data = connection.recv(max_data)
                message += str(data.decode())
                if len(data) == 0:
                    break
                elif len(data) != max_data and message.endswith("}"):
                    break
        
            try:
                message = json.loads(message)
            except:
                print('at line 66 in master.py, can\'t json.loads(message): {0}'.format(message))
                continue
            
            client_address = socket.gethostbyaddr(client_address[0])[0]

            #print('master received message: {0}, from addr: {1}, ack: {2}'.format(message, client_address, ack))
            
            if ack:
                #self.ack_handler(message, client_address)
                threading.Thread(target=self.ack_handler, args = (message, client_address)).start()
            else:
                self.message_handler(message, client_address, connection)
        
    def ack_handler(self, message, client_address):
        #print('start ack handler for message: {0}'.format(message))
        op = message['op']
        if 'op' not in message or op != 'ack':
            return

        op_seq = message['seq']
        if op_seq not in self.ack_dict:
            print('this op_seq {0} has finished'.format(op_seq))
            return
        
        #print('It\' an ack, now op_seq: {0}, self.ack_dict: {1}, self.seq_dict: {2}'.format(op_seq, self.ack_dict, self.seq_dict))
            
        if client_address not in self.ack_dict[op_seq]:
            print('seems that node {0} has already send master ack of op_seq {1}'.format(client_address, op_seq))
            return
        
        if op_seq not in self.seq_dict: # this ack is for recovery
            print('recovery seq {0} received ack from {1} so assumed recovery finished.'.format(op_seq, client_address))
            self.ack_dict[op_seq] = []
            #print('finsh recover ack_handler for message < {0} >, now ack_dict: {1},  now seq_dict: {2}'.format(message, self.ack_dict, self.seq_dict))
            return
        
        self.ack_dict[op_seq].remove(client_address)
            
        #print('self.ack_dict: {0}'.format(self.ack_dict))        
        
        if self.ack_dict[op_seq] == []:
            # reply client with ack
            new_message = {'op': 'ack', 'seq': op_seq}
            try:
                self.seq_dict[op_seq].send(json.dumps(new_message).encode())
            except:
                # client fails, ignore
                pass
            del self.ack_dict[op_seq]
            del self.seq_dict[op_seq]
            print('master replied ack to client for op_seq {0}'.format(op_seq))
        
        #print('finsh put ack_handler for message < {0} >, now ack_dict: {1},  now seq_dict: {2}'.format(message, self.ack_dict, self.seq_dict))
    
    def message_handler(self, message, client_address, connection):
        self.seq += 1
        if 's' in message:
            sdfs_filename = message['s']

        if 'op' in message:
            op = message['op']
            ''' 
            put: 
                1. Reply to client with a list of 4 ips 
                (the first one is the client itself,
                last 3 one chosen from membership list,
                master itself can be chosen).
                2. Wait for acks from the chosen ips.
                3. If get all 3 acks, reply to client with 'ack'.
            '''
            if op == 'put':
                # check if the file is still being written
                w_finished = self.wait_writing_finished(sdfs_filename, 5)
                if w_finished == 1: # being written
                    message['failed'] = 'this file still being written'
                    try:
                        connection.send(json.dumps(message).encode())
                    except:
                        print('client fails, ignore')
                    print('master reject putting into a being written file')
                    return
                
                # choose 4 ips from membership list
                self.occupied += 1
                if w_finished == 0: # has been written
                    self.file_addr[sdfs_filename][1] = False
                    chosen_addrs = self.file_addr[sdfs_filename][0]
                else: # hasn't been written
                    if self.occupied > 8 and len(self.membership_list) > 4:
                        chosen_indexes = random.sample(range(1, len(self.membership_list)), 4)
                        chosen_addrs = [self.membership_list[i] for i in chosen_indexes]
                    else:
                        chosen_addrs = random.sample(self.membership_list, min(4, len(self.membership_list)))
                    for i in range(len(chosen_addrs)):
                        chosen_addrs[i] = chosen_addrs[i].split('#')[0]
                    
                # update expected ack list
                left_ips = [chosen_addrs[i] for i in range(0, len(chosen_addrs))] # 4 ips
                self.ack_dict[self.seq] = left_ips
                self.seq_dict[self.seq] = connection
                
                # reply to client with all replica addr list
                # message format: 
                # {op:put, ips:[ip(not necessarily client now) ip ip ip], l:localfilename, 
                # s:sdsfilename, ttl: #}
                message['ips'] = chosen_addrs
                message['ttl'] = len(chosen_addrs) # now 4, because need to foward to 4 servers
                message['seq'] = self.seq
		
                try:
                    connection.send(json.dumps(message).encode())
                except:
                    print('client fails, ignore')
                    return
                
                self.file_addr[sdfs_filename] = [chosen_addrs, True]
                for node in chosen_addrs:
                    if node not in self.addr_file:
                        self.addr_file[node] = [sdfs_filename]
                    else:
                        if sdfs_filename not in self.addr_file[node]:
                            self.addr_file[node].append(sdfs_filename)
                #print('master received a put intr and send message: {0}'.format(message))
                
            # get:
            #     1. tell the location
            # get-versions: 
            #     {op:get-versions  s:sdfsfilename  num_versions:#  l:localfilename}
            elif op == 'get' or op == 'get-versions':
                # check if being written or no such file
                w_finished = self.wait_writing_finished(sdfs_filename, 10)
                if w_finished == 2:
                    message['failed'] = 'no such file'
                elif w_finished == 0:
                    chosen_addrs = self.file_addr[sdfs_filename][0]
                    if client_address in chosen_addrs:
                        message['ips'] = client_address
                    else:
                        message['ips'] = chosen_addrs[0]
                else:
                    message['failed'] = 'this file still being written'
                    
                # check num_versions < 5
                if 'num_versions' in message:
                    if message['num_versions'] > 5:
                        info = 'num_versions > 5, get latest 5 versions'
                        if 'failed' in message:
                            message['failed'] += '; ' + info
                        else:
                            message['failed'] = info
                        message['num_versions'] = 5
                    
                try:
                    connection.send(json.dumps(message).encode())
                except:
                    print('client fails, ignore')
                    return
                #print('master received a get instr and send message: {0}'.format(message))
                
            # delete:
            # {op:del, s:sdfsfilename}
            # 1. send del info to replica addr: {op:del, s:sdfsfilename}
            # 2. ack to client: {op:ack, seq: op_seq}
            elif op == 'del':
                print('del...')
                # check if the file is still being written
                w_finished = self.wait_writing_finished(sdfs_filename, 10)
                if w_finished == 2:
                    message['failed'] = 'no such file'
                elif w_finished == 1:
                    message['failed'] = 'this file still being written'
                else:
                    chosen_addrs = self.file_addr[sdfs_filename][0]
                    new_message = (json.dumps({'op': 'del', 's': sdfs_filename}) + self.padding).encode()
                    for addr in chosen_addrs:
                        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                        sock.connect((addr, 2333))
                        sock.send(new_message)
                        sock.close()
                        print('master sent message < {0} > to addr <{1}>.'.format(new_message, addr))
                    message = {'op': 'ack', 'seq': self.seq}  
                try:
                    connection.send(json.dumps(message).encode())
                except:
                    print('client fails, ignore')
                if w_finished != 2:
                    del self.file_addr[sdfs_filename]  
                print('after del <{0}>, master send message: {1}'.format(sdfs_filename, message))
 
            # ls: {op:ls s:sdfsfilename}
            # 1. not exist: {op:ls s:sdfsfilename failed:nosuchfile}
            # 2. exist: {op:ls s:... ips:addrs_list}
            elif op == 'ls':
                w_finished = self.wait_writing_finished(sdfs_filename, 10)
                if w_finished == 2:
                    message['failed'] = 'no such file'
                else:
                    if w_finished == 1:
                        message['failed'] = 'this file still being written but might have available replicas already'
                    message['ips'] = self.file_addr[sdfs_filename][0]
                try:
                    connection.send(json.dumps(message).encode())
                except:
                    print('client fails, ignore')
                    
            else:
                print('master: none of my business')
                           

