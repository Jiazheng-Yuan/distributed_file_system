import paramiko

# all functions in the script ssh into the server specified by ips.txt
# and execute a command in shell

# pull from gitlab on all machines
def clone():
    with open("ips.txt",'r') as file:
        for line in file:
            ip = line.split(":")[0]
            try:
                #ssh into servers
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(ip, username="yixinz6", password="Zyyy9-9-")
                ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("git clone git@gitlab.engr.illinois.edu:jyuan18/cs_425_mp3.git")
                print(ssh_stdout.read().decode())
            except:
                print(ip)
                print("error!")

def pull():
    with open("ips.txt",'r') as file:
        for line in file:
            ip = line.split(":")[0]
            try:
                #ssh into servers
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(ip, username="yixinz6", password="Zyyy9-9-")
                ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("cd ~/cs_425_mp3/;git stash;git pull")
                print(ssh_stdout.read().decode())
            except:
                print(ip)
                print("error!")

#start servers on all machines
def start_server():
    with open("ips.txt", 'r') as file:
        for line in file:
            ip = line.split(":")[0]

            if ip == "fa18-cs425-g26-01.cs.illinois.edu":
                continue
            try:
                # ssh into servers
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(ip, username="yixinz6", password="Zyyy9-9-")
                ssh.exec_command("cd ~/cs_425_mp3/;python SDFS_Node.py &")
            except Exception as e:
                print(ip)
                print(e)
                print("error!")

#kill  servers on all machines
def kill_server():
    with open("ips.txt", 'r') as file:
        for line in file:
            # ssh into servers
            ip = line.split(":")[0]
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(ip, username="yixinz6", password="Zyyy9-9-")
            ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("netstat -tulpn")
            lines = (ssh_stdout.readlines())

            #find the process listening on port 7002, which is server, and kills it
            for line in lines:
                if line.count(":7003") == 1 :
                    line = line.split(" ")
                    for i in line:
                        if(i.count("/python") == 1):
                            pid = i.split("/")[0]
            for line in lines:
                if line.count(":2333") == 1 :
                    line = line.split(" ")
                    for i in line:
                        if(i.count("/python") == 1):
                            pid = i.split("/")[0]
            try:
                ssh.exec_command("kill -9 " + pid)
            except:
                pass
#clear all previous log
def clear_log():
    with open("ips.txt",'r') as file:
        for line in file:
            ip = line.split(":")[0]
            try:
                #ssh into servers
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(ip, username="yixinz6", password="Zyyy9-9-")
                ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("cd ~/cs_425_mp3/;rm vm.log")
                print(ssh_stdout.read().decode())
            except:
                print(ip)
                print("error!")

def three():
    kill_server()
    clear_log()
    pull()
def combine():
    with open("ips.txt", 'r') as file:
        for line in file:
            ip = line.split(":")[0]
            try:
                # ssh into servers
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(ip, username="yixinz6", password="Zyyy9-9-")
                ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("cd ~/cs_425_mp3/;rm -rf local0;git pull")
                print(ssh_stdout.read().decode())
                ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("netstat -tulpn")
                lines = (ssh_stdout.readlines())
            except:
                pass

            # find the process listening on port 7002, which is server, and kills it
            try:
                for line in lines:
                    if line.count(":7003") == 1:
                        line = line.split(" ")
                        for i in line:
                            if (i.count("/python") == 1):
                                pid = i.split("/")[0]
            except:
                pass
            try:
                for line in lines:
                    if line.count(":2333") == 1:
                        line = line.split(" ")
                        for i in line:
                            if (i.count("/python") == 1):
                                pid = i.split("/")[0]
                try:
                    ssh.exec_command("kill -9 " + pid)
                except:
                    pass
            except:
                pass

if __name__=="__main__":

    
    #pull()
    #combine()
    kill_server()
    #three()
    #clone()
    clear_log()
    #start_server()