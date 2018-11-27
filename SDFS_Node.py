from peer import Peer
import threading
import socket
import json
from collections import deque
import os
from master import master
import itertools
import shutil
import logging

class SDFS_Node:
    def __init__(self):
        host_name = socket.gethostname()
        if os.path.isdir("sdfs/"):
            shutil.rmtree("sdfs/")
        os.system("mkdir sdfs")
        self.membership_manager = Peer(host_name)
        self.membership_manager.start()

        self.padding = "jyuan18?yixinz6"

        self.file_receive_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.file_receive_socket.bind(("0.0.0.0",2333))

        self.file_dict = {"filename":[1,2,3,4,5]}

        self.file_dict_lock = threading.Lock()
        self.file_to_node = {"filename":["ip1","ip2","ip3"]}

        self.ip = socket.gethostname()
        self.file_receive_socket.listen(100)
        logging.basicConfig(filename='vm1.log', level=logging.INFO)
        threading.Thread(target=self.receive_file).start()

        if self.ip == "fa18-cs425-g26-01.cs.illinois.edu":
            Master = master(self.membership_manager)
            threading.Thread(target=Master.listen, args=(Master.op_listen_socket, False)).start()
            threading.Thread(target=Master.listen, args=(Master.ack_listen_socket, True)).start()


    #replication, to 3  members in membership(if someone fails, ignore,master take care of it)

    def receive_file(self):
        max_data = 8192
        while True:
            file = deque([])
            connection, addr = self.file_receive_socket.accept()
            while True:
                data = connection.recv(max_data)
                data = data.decode()
                file.append(data)

                if len(data) == 0:
                    break
                elif len(data) != max_data and (data.endswith("jyuan18?yixinz6")):#marking end of file
                    break
                elif len(data) < len(self.padding) and data.endswith(self.padding[-1*(len(data)):]):
                    break


            self.message_handler(file, addr)
        pass


    # all the message type
    def message_handler(self,message,addr):

        # first chunk has the instruction

        first_chunk = message[0]

        instruction = first_chunk.split("jyuan18?yixinz6")[0]

        if len(json.dumps(instruction)) < 5:
            print("what the fuck iadjoasinodaasda")
            print("what the fuck iadjoasinodaasda")
            print("what the fuck iadjoasinodaasda")
            print("what the fuck iadjoasinodaasda")
            print("what the fuck iadjoasinodaasda")
            print(message)
            return
        try:
            instruction = json.loads(instruction)#convert instruction back to dictionary
        except:
            return
        first_chunk = first_chunk.split("jyuan18?yixinz6")[1]
        message[0] = first_chunk#change the first part of the message, take out the instruction
        operation = instruction["op"]
        print("operation is:" + operation)
        if operation == "put":
            self.put(instruction,message)
        elif operation == "get":
            self.get(instruction, addr)
        elif operation == "del":
            self.delete(instruction)
        elif operation == "get-versions":
            self.get_version(instruction, addr)
        elif operation == "recover":
            self.recover(instruction)
        elif operation == "recover_copy":
            self.receive_copy(instruction,message)
        elif operation == "change":
            self.change(instruction)

    # make a socket connected to master
    def master_socket(self):
        master_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master_sock.connect((self.membership_manager.member_ship_list[0].split("#")[0], 9999))
        return master_sock

    # recover, send the replica of the specificd file to the specified machine.
    def recover(self,instruction):
        ips = [instruction["new"]]
        sdfs_filenames = [instruction["s"]]
        master_sock = self.master_socket()
        seq = instruction["seq"]
        # change the index on this machine about the replica location

        # send all the versions of a file to the specified node
        for ip in ips:  # send to recipient
            for sdfs_filename in sdfs_filenames:

                versions = self.file_dict[sdfs_filename]
                for version in reversed(versions):
                    try:
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.connect((ip, 2333))
                    except:
                        print("connect recover copy to node "+ip+"fail ")
                        continue
                    version_filename = sdfs_filename+ "-"+str(version)
                    with open(version_filename,'r') as file_reader:
                        content = file_reader.read()
                    instruction = {"s": version_filename, "seq": seq, "op": "recover_copy"}
                    if versions.index(version) == 0:
                        instruction["final"] = "true"
                    instruction = json.dumps(instruction) + self.padding
                    message_list = [instruction,content,self.padding]
                    try:
                        sock.send(("".join(message_list)).encode())
                    except:
                        print("send recover copy to node " + ip + "fail ")
                        continue
                    ack = {"op": "ack", "seq": seq}
                    master_sock.send(json.dumps(ack).encode())
                    sock.close()

        master_sock.close()

    # receive replicas in case of node failure
    def receive_copy(self,instruction,file):
        versioned_filename = str(instruction['s'])
        filename = versioned_filename[:versioned_filename.rfind('-')]
        version = versioned_filename[versioned_filename.rfind('-') + 1:]

        self.file_dict_lock.acquire()
        if filename not in self.file_dict:
            self.file_dict[filename] = [int(version)]
        else:
            self.file_dict[filename].append(int(version))
        self.file_dict_lock.release()

        file_writer = open(versioned_filename, "w")
        for chunk in itertools.islice(file, 0, max(len(file) - 2, 0)):
            file_writer.write(chunk)
        second_last = ""
        if len(file) > 1:
            second_last = file[-2]
        file_writer.write((second_last + file[-1]).split("jyuan18?yixinz6")[0])
        if "final" in instruction:
            master_socket = self.master_socket()
            message = json.dumps({"seq": instruction["seq"], "op": "ack"})
            master_socket.send(message.encode())
            master_socket.close()

    # change the info about which node has a specific file after failure recovery
    def change(self,instruction):
        self.file_dict_lock.acquire()
        self.file_to_node[instruction['s']] = instruction["ips"]
        self.file_dict_lock.release()
        logging.info("update membership list after recovery, the file "+instruction['s'] + "is currently at" +str(instruction["ips"]))

    def delete(self,instruction):
        versions = self.file_dict.pop(instruction['s'])
        self.file_to_node.pop(instruction['s'])
        for version in versions:
            os.system("rm " + instruction['s'] + "-" + str(version))

    def get(self, instruction, addr):
        sdfs_filename = instruction['s']
        conn = socket.socket(socket.AF_INET,socket.SOCK_STREAM)

        conn.connect((addr[0], 6667))

        if sdfs_filename not in self.file_dict:
            message = "No such file."
            try:
                conn.send(message.encode())
            except:
                print("return to client failed")
                return
        else:
            with open(sdfs_filename + "-" + str(self.file_dict[sdfs_filename][-1]), "r") as file_reader:
                content = file_reader.read()
            content += self.padding
            conn.send(content.encode())

    def get_version(self,instruction, addr):
        sdfs_filename = instruction['s']
        conn = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        num_versions = instruction["num_versions"]

        conn.connect((addr[0],6667))
        if sdfs_filename not in self.file_dict:
            message = "No such file."
            try:
                conn.send(message.encode())
            except:
                print("return to client failed")

                return

        else:
            combined_file = []
            versions = self.file_dict[sdfs_filename]
            if num_versions > len(versions):
                message = "too many versions."
                try:
                    conn.send(message.encode())
                except:
                    print("return to client failed")
                    return
            #combine each version
            versions = versions[(-1)*num_versions:]
            for version in versions:

                with open(sdfs_filename + "-" + str(version), "r") as file_reader:
                    content = file_reader.read()
                    combined_file.append("version "+str(version)+"\n\n\n")
                    combined_file.append(content)
                    combined_file.append("\n\n\n")
            combined_file.append(self.padding)
            content = "".join(combined_file)
            conn.send(content.encode())
        logging.info("the last "+str(num_versions)+ " of versions of the file " +sdfs_filename+" was read" )
        logging.info("this file " + sdfs_filename + "is also stored in "+str(self.file_to_node[sdfs_filename]))

    # send ack to master when required
    def reply_ack(self,instruction):
        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        sock.connect((self.membership_manager.member_ship_list[0].split("#")[0],9999))
        ins = json.dumps({"seq":instruction["seq"], "op": "ack"})
        sock.send(ins.encode())
        sock.close()

    def put(self,instruction,file):
        instruction["ttl"] -= 1

        sdfs_filename = instruction["s"]
        self.file_to_node[sdfs_filename] = instruction["ips"]
        threading.Thread(target=self.spread, args=(instruction, file)).start()

        if sdfs_filename in self.file_dict:
            if len(self.file_dict[sdfs_filename]) < 5:
                self.file_save(sdfs_filename,instruction,file)
            else:#deletion of old file

                self.file_save(sdfs_filename, instruction, file)
                #starts removing the oldest version

                obsolete_version = self.file_dict[sdfs_filename].pop(0)

                file_to_delete =  sdfs_filename + "-"+str(obsolete_version)
                if os.path.isfile(file_to_delete):
                    os.remove(file_to_delete)
                else:
                    print("There is no file: " + file_to_delete)
        else:

            self.file_save(sdfs_filename, instruction, file)
        self.reply_ack(instruction)

    def file_save(self,sdfs_filename,instruction, file):#simply saving this as newest version to file

        if sdfs_filename not in self.file_dict:# not already exist:initialize
            self.file_dict[sdfs_filename] = [0]
        new_version_num = self.file_dict[sdfs_filename][-1] + 1

        if self.file_dict[sdfs_filename][0] == 0:#if this time initialization happened
            self.file_dict[sdfs_filename].pop(0)

        self.file_dict[sdfs_filename].append(new_version_num)
        sdfs_filename = sdfs_filename + "-" + str(new_version_num)
        sum = 0
        for c in file:
            sum+=len(c)
        file_writer = open(sdfs_filename, "w")
        for chunk in itertools.islice(file, 0, max(len(file) - 2, 0)):
            file_writer.write(chunk)
        second_last = ""
        if len(file) > 1:
            second_last = file[-2]
        file_writer.write((second_last + file[-1]).split("jyuan18?yixinz6")[0])
    # forward the file to another replica when put until specified number of forward is done
    def spread(self, instruction, file):
        if instruction["ttl"] < 1:
            return
        next_ip = instruction["ips"][(instruction["ips"].index(self.ip) + 1) % len(instruction["ips"])]
        temp_list = []
        temp_list.append(json.dumps(instruction)+self.padding)
        for chunk in file:
            temp_list.append(chunk)
        temp_list.append(self.padding)
        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        sock.connect((next_ip,2333))
        sock.send("".join(temp_list).encode())


if __name__ == "__main__":
    node = SDFS_Node()

