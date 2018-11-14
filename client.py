import sys
import socket

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

while True:
    nodeAddr = input("Enter node port: ")
    if nodeAddr == 'exit':
        break

    try:
        port = int(nodeAddr)
        ip = "127.0.0.1"
        cmd = input("Enter Command: ")
        cmd = str.encode(cmd)
        sock.connect((ip, port))
        sock.sendall(cmd)
        print()
    except:
        break

sock.close()
