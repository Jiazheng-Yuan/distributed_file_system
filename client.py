import socket
import json
import time
import sys
import threading
import os
class Client:
        def __init__(self):
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect(("fa18-cs425-g26-01.cs.illinois.edu", 6666))
            self.padding = "jyuan18?yixinz6"
            self.listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.listen_socket.bind(("0.0.0.0",6667))
            self.listen_socket.listen(5)
            # sock.listen(5)
        '''
        def listening(self):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(("0.0.0.0", 1111))
            sock.listen(5)
            while True:
                conn,addr = sock.accept()
                data = conn.recv(8192)
                data = data.decode()
                instruction = json.loads(data)
                with open("/users/jiazheng/Documents/CLIENT OP.txt", "r") as file:
                    content = file.read()
                file_and_instr = data + content
                ip = instruction["ips"][(instruction["ips"].index(socket.gethostname()) + 1) % len(instruction["ips"])]

                print(ip)
                new_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                new_sock.connect((ip, 2333))
                new_sock.send(file_and_instr.encode())
        '''
        def put(self, l, s):
            #sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            #sock.connect(("fa18-cs425-g26-02.cs.illinois.edu",6666))
            #sock.listen(5)
            message = {"op": "put", "s": "sdfs/" + s}
            message = json.dumps(message)
            self.sock.send(message.encode())
            while True:
                #conn,addr = sock.accept()
                data = self.sock.recv(8192)
                data = data.decode()
                instruction = json.loads(data)
                #instruction["ttl"] = instruction["ttl"] + 1
                data = json.dumps(instruction)
                with open("local/" + l, "r") as file:
                    content = file.read()
                file_and_instr = data +"jyuan18?yixinz6"+ content+"jyuan18?yixinz6"
                #instruction["ips"].append(socket.gethostname())
                ip = instruction["ips"][0]
                print(instruction)
                print(ip)
                #print(file_and_instr)
                #print(file_and_instr.encode().decode())
                new_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                new_sock.connect((ip, 2333))
                new_sock.send(file_and_instr.encode())
                ack = self.sock.recv(8192)
                print(ack.decode())
                return

        def get(self, op, version, s, l):

            #sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            #sock.connect(("fa18-cs425-g26-02.cs.illinois.edu", 6666))
            # sock.listen(5)
            if op == "get":
                message = {"op": "get", "s": "sdfs/"+s}
            else:
                message = {"op": "get-versions","num_versions":version,"s": "sdfs/" + s}
            message = json.dumps(message)
            self.sock.send(message.encode())
            start = time.time()
            while True:
                file = []
                data = self.sock.recv(8192)
                data = data.decode()
                instr = json.loads(data)
                if "ips" not in instr:
                    print("no such file")
                    return
                ip = instr["ips"]
                print(instr)
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((ip,2333))
                sock.send((message + self.padding).encode())
                conn,addr = self.listen_socket.accept()
                while True:
                    data = conn.recv(8192)
                    data = data.decode()
                    file.append(data)

                    if len(data) == 0:
                        break
                    elif len(data) != 8192 and (data.endswith("jyuan18?yixinz6")):
                        break
                    elif len(data) < len(self.padding) and data.endswith(self.padding[-1*(len(data)):]):
                        break

                file_writer = open("local/"+l, "w")
                for chunk in file[:-2]:
                    file_writer.write(chunk)
                second_last = ""
                if len(file) > 1:
                    second_last = file[-2]
                file_writer.write((second_last + file[-1]).split("jyuan18?yixinz6")[0])
                #file_and_instr = data + "jyuan18?yixinz6" + content + "jyuan18?yixinz6"
                # instruction["ips"].append(socket.gethostname())
                #print(file[0])
                print("finished saving")
                return start

        def delete(self,s):
            message = {"op": "del", "s": "sdfs/"+s}
            message = json.dumps(message)
            self.sock.send(message.encode())
            while True:
                data = self.sock.recv(8192)
                data = data.decode()
                print(data)
                return
        def ls(self, filename):
            message = {"op": "ls", "s": "sdfs/" + filename}
            message = json.dumps(message)
            self.sock.send(message.encode())
            while True:
                data = self.sock.recv(8192)
                instruction = json.loads(data.decode())
                if "ips" not in instruction:
                    print("no such file")
                    return
                ips = instruction["ips"]
                print(filename + " stored in machines: ")
                for ip in ips:
                    print(str(ip))
                return
        def store(self):
            print(os.popen("ls sdfs/").read())

if __name__ == "__main__":
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #sock.connect(("fa18-cs425-g26-07.cs.illinois.edu",7004))
    #self.padding = "jyuan18?yixinz6"
    op = sys.argv[1]
    C = Client()
    start = time.time()
    if op == "get":
        s = sys.argv[2]
        l = sys.argv[3]
        C.get("get",-1,s,l)
    elif op == "put":
        l = sys.argv[2]
        s = sys.argv[3]
        C.put(l, s)
    elif op == "delete":
        s = sys.argv[2]
        C.delete(s)
    elif op == "ls":
        s = sys.argv[2]
        C.ls(s)
    elif op == "store":
        C.store()
    elif op == "get-versions":
        s = sys.argv[2]
        v = int(sys.argv[3])
        l = sys.argv[4]
        C.get(op,v,s,l)
    print(time.time() - start)

    C.listen_socket.close()

    #C.get()

    #sock.send(m.encode())
    #threading.Thread(target=listening())

    #print(socket.gethostbyaddr("172.22.156.85")[0])

