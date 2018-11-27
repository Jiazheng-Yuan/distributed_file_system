import socket
import sys
if __name__ == "__main__":
    command = sys.argv[1]
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    host_name = socket.gethostname()
    host_ip = socket.gethostbyname(host_name)
    sock.connect((host_ip,7003))
    sock.send(command)
