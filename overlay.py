import sys
import socket
from hashlib import sha1
from random import randint
import threading

HASH_BITS = 160
LOGICAL_SIZE = 2**HASH_BITS
nodeIP = "127.0.0.1"
nodePort = 4450
sep = "-"*30 + "\n"
sep2 = "-"*30
recvBytes = 4096

def getKey(ip, port):
    raw = str(ip) + str(port)
    key = int(sha1(raw.encode('utf-8')).hexdigest(), 16)
    return key

class Node(threading.Thread):
    def __init__(self, ip, port, dynamicNode=False):
        threading.Thread.__init__(self)
        self.ip = ip
        self.port = port
        self.id = getKey(ip, port)
        self.info = [self.id, self.ip, self.port]
        self.predecessor = None
        self.fingerTable = {}
        self.dataTable = []
        self.sock = None
        self.dynamicNode = dynamicNode

    def updateFingerTable(self):
        for i in range(HASH_BITS):
            self.fingerTable[i] = self.info
            for j in allNodes:
                if self.endInclusive(j[0], (self.id + 2**i) % LOGICAL_SIZE ,self.fingerTable[i][0] ):
                    self.fingerTable[i] = j

    def updateFingerTable2(self):
        for i in range(HASH_BITS):
            self.fingerTable[i] = [self.id, self.ip, self.port]
            f = False
            for j in allNodes:
                if j[0] >= (self.id + 2**i) % LOGICAL_SIZE:
                    self.fingerTable[i] = j
                    f = True
                    break

            if not f:
                self.fingerTable[i] = allNodes[0]

    def between(self, n1, n2, n3):
        ## if n1 is in between n2 and n3
        if n2 < n3:
            return (n2 < n1 < n3)
        else:
            return (n1 < n3 or n1 > n2)

    def endInclusive(self, n1, n2, n3):
        if n1 == n3:
            return True
        else:
            return self.between(n1, n2, n3)

    def startInclusive(self, n1, n2, n3):
        if n1 == n2:
            return True
        else:
            return self.between(n1, n2, n3)

    def findNode(self, key, startNodeAdd):
        key = key % LOGICAL_SIZE
        startNodeIP, startNodePort = startNodeAdd.split(':')

        # print("In findNode() method of node: " + str(self) + " for the key: " + str(key))
        # print(sep)

        nextHop = [self.id, self.ip, self.port]
        for i in range(HASH_BITS):
            if self.endInclusive(key, self.id, self.fingerTable[i][0]):
                break
            else:
                nextHop = self.fingerTable[i]

        if nextHop[0] == self.id:
            newsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            newsock.connect((startNodeIP, int(startNodePort)))
            resultString = ' '.join(map(str, self.fingerTable[0])) + " " + ' '.join(map(str, self.info))
            newsock.sendall(b'foundNode ' + resultString.encode())
            newsock.close()
            return
        else:
            next_ip = nextHop[1]
            next_port = nextHop[2]
            newsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            newsock.connect((next_ip, next_port))
            newsock.sendall(b'findNode ' + str(key).encode() + ' ' + startNodeAdd)
            newsock.close()

    def joinNetwork(self, newNode):
        newNodeID = getKey(newNode[0], newNode[1])
        newsock_ip = nodeIP
        newsock_port = 18001

        print("Got a request to add new node: " + newNode[0] + ":" + str(newNode[1]) + ". I am node: " + str(self) )
        print(sep)

        newsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        newsock.bind((newsock_ip, newsock_port))
        self.findNode(newNodeID, str(newsock_ip)+':'+str(newsock_port))
        data, addr = newsock.recvfrom(recvBytes)
        data = data[len('foundNode '):].split()
        newNodeSucc = data[:3]
        newNodePred = data[3:]
        newsock.close()

        infoNeighbours = ' '.join(map(str, newNodeSucc)) + " " + ' '.join(map(str, newNodePred))
        self.sock.sendto(str(infoNeighbours), (newNode[0], int(newNode[1])))


    def initialize_finger_table(self):
        succIP = self.fingerTable[0][1]
        succPort = self.fingerTable[0][2]
        for i in range(1, HASH_BITS):
            nextKey = (self.id + 2**i) % LOGICAL_SIZE
            reqString = "findNode " + str(nextKey) + " " + str(self.ip)+':'+str(self.port)
            self.sock.sendto(reqString, (succIP, succPort))
            data, addr = self.sock.recvfrom(recvBytes)
            data = data.split()
            # print("$$$$$")
            # print(str(data))
            # print("$$$$$")
            self.fingerTable[i] = [int(data[1]), data[2], int(data[3])]
        # resultString = str(self.id) + " " + self.ip + " " + str(self.port)
        # updateMsg = "changeNode 1 " + resultString
        # self.sock.sendto(updateMsg, (self.predecessor[1], int(self.predecessor[2])))
        # updateMsg = "changeNode 0 " + resultString
        # self.sock.sendto(updateMsg, (self.fingerTable[0][1], int(self.fingerTable[0][2])))

    def update_other_pointers(self):
        resultString = str(self.id) + " " + self.ip + " " + str(self.port)
        updateMsg = "changeNode 0 " + resultString
        self.sock.sendto(updateMsg, (self.predecessor[1], int(self.predecessor[2])))
        updateMsg = "changeNode 1 " + resultString
        self.sock.sendto(updateMsg, (self.fingerTable[0][1], int(self.fingerTable[0][2])))
        updateMsg = "newAdded " + resultString
        self.sock.sendto(updateMsg, (self.fingerTable[0][1], int(self.fingerTable[0][2])))

    def invoke_content_sharing(self):
        newsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        contentMsg = "sendContents " + self.ip + " " + str(self.port)
        newsock.sendto(contentMsg, (self.fingerTable[0][1], int(self.fingerTable[0][2])))

    def sendContentToNewNode(self, newNodeAddr):
        # print("Sending contents belonging to new node")
        # print(sep)
        newNodeID = getKey(newNodeAddr[0], newNodeAddr[1])
        contentList = []
        for item in self.dataTable:
            k = self.getMsgKey(item[0])
            if not self.endInclusive(k, newNodeID, self.id):
                contentList.append(item)

        #  print('sending to new node')
        if len(contentList) > 0:
            newsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            for elem in contentList:
                ## assuming value is a list
                # elemMsg = 'contentUpdate ' + ' '.join(map(str, elem))
                elemMsg = 'contentUpdate ' + elem[0] + ' ' + ' '.join(map(str, elem[1])) + ' ' + str(elem[2])
                newsock.sendto(elemMsg, tuple(newNodeAddr))
                self.dataTable.remove(elem)
            newsock.close()
        #  print('sent to new node')

    def updateMyContent(self, msg):
        print('Receiving and updating my content as a new node')
        print(sep)
        topic = msg[0]
        vote = int(msg[-1])
        member = msg[1:-1]
        self.dataTable.append([topic, member, vote])

    def handleNewAdded(self, newNodeID, newNodeIP, newNodePort):

        newNodeID = int(newNodeID)
        newNodePort = int(newNodePort)
        if(self.id == newNodeID):
            return
        else:
            for i in range(HASH_BITS):
                if self.endInclusive(newNodeID, (self.id + 2**i) % LOGICAL_SIZE, self.fingerTable[i][0] ):
                    self.fingerTable[i] = [newNodeID, newNodeIP, newNodePort]
                    # print("Updated my " + str(i) + " finger table entry to [" + str(newNodeID) + ", " + newNodeIP + ", " + str(newNodePort) + "]. I am " + str(self))
                    # print(sep)
            resultString = str(newNodeID) + " " + newNodeIP + " " + str(newNodePort)
            updateMsg = "newAdded " + resultString
            self.sock.sendto(updateMsg, (self.fingerTable[0][1], int(self.fingerTable[0][2])))

    def getMsgKey(self, msg):
        key = int(sha1(msg.encode('utf-8')).hexdigest(), 16)
        return key

    def keyPresent(self, key):
        return (key in self.dataTable)

    def putContent(self, msg):
        msgList = msg.split()
        username = msgList[0]
        password = msgList[1:]
        msgKey = self.getMsgKey(username)
        msgKey = msgKey % LOGICAL_SIZE

        newsock_ip = nodeIP
        newsock_port = 18111
        newsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        newsock.bind((newsock_ip, newsock_port))
        self.findNode(msgKey, str(newsock_ip)+':'+str(newsock_port))

        data, addr = newsock.recvfrom(recvBytes)
        data = data[len('foundNode '):].split()
        msgKeySucc = data[:3]
        newsock.close()

        print("About to put the msg: *"+msg+"* with msgKey "+str(msgKey)+" in node: " + str(msgKeySucc))
        print(sep)

        resultString = "putYourContent " + msg
        self.sock.sendto(resultString, (msgKeySucc[1], int(msgKeySucc[2])))

    def vote(self, msg):
        msgList = msg.split()
        username = msgList[0]
        password = msgList[1:]
        msgKey = self.getMsgKey(username)
        msgKey = msgKey % LOGICAL_SIZE

        newsock_ip = nodeIP
        newsock_port = 18111
        newsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        newsock.bind((newsock_ip, newsock_port))
        self.findNode(msgKey, str(newsock_ip)+':'+str(newsock_port))

        data, addr = newsock.recvfrom(recvBytes)
        data = data[len('foundNode '):].split()
        msgKeySucc = data[:3]
        newsock.close()

        resultString = "vYourContent " + msg
        self.sock.sendto(resultString, (msgKeySucc[1], int(msgKeySucc[2])))

    def putInMyContent(self, msg):
        msgList = msg.split()
        username = msgList[0]
        password = msgList[1:]
        # msgKey = self.getMsgKey(username)
        # msgKey = msgKey % LOGICAL_SIZE
        self.dataTable.append([username, password, 0])

    def voteInMyContent(self, msg):
        msgList = msg.split()
        username = msgList[0]
        password = msgList[1:]
        # msgKey = self.getMsgKey(username)
        # msgKey = msgKey % LOGICAL_SIZE
        for item in self.dataTable:
            if (item[0] == username and item[1] == password):
                item[2] += 1

    def getContent(self, msg):
        msgKey = self.getMsgKey(msg)
        msgKey = msgKey % LOGICAL_SIZE

        newsock_ip = nodeIP
        newsock_port = 18112
        newsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        newsock.bind((newsock_ip, newsock_port))
        self.findNode(msgKey, str(newsock_ip)+':'+str(newsock_port))

        data, addr = newsock.recvfrom(recvBytes)
        data = data[len('foundNode '):].split()
        msgKeySucc = data[:3]
        newsock.close()

        print("About to get the msg: *"+msg+"* with msgKey "+str(msgKey)+" from node: " + str(msgKeySucc))
        print(sep)

        resultString = "getYourContent " + msg
        self.sock.sendto(resultString, (msgKeySucc[1], int(msgKeySucc[2])))

    def fetchMyContent(self, msg, queryNode):
        # msgKey = self.getMsgKey(msg)
        # msgKey = msgKey % LOGICAL_SIZE
        queryNodeIP = queryNode[0]
        queryNodePort = queryNode[1]

        exists = False
        num = 0

        for item in self.dataTable:
            if(item[0] == msg):
                exists = True
                num+=1

        if exists:
            response = str(num) + " "
            for item in self.dataTable:
                if(item[0] == msg):
                    response += ' '.join(item[1]) + "$$$ "
            for item in self.dataTable:
                if(item[0] == msg):
                    response += str(item[2]) + " "
            print('About to send the response to msg:*'+msg+'* from my data to queryNode: '+ queryNodeIP + ":" + str(queryNodePort) +'. I am node: ' + str(self))
            print(sep)
            resultString = "responseQuery2 " + response
            self.sock.sendto(resultString, (queryNodeIP, queryNodePort))

        else:
            response = "Couldn't find this username!"
            print('About to send the response to msg:*'+msg+'* from my data to queryNode: '+ queryNodeIP + ":" + str(queryNodePort) +'. I am node: ' + str(self))
            print(sep)
            resultString = "responseToQuery " + response
            self.sock.sendto(resultString, (queryNodeIP, queryNodePort))

    def printNodes(self, startNode):
        print(self)
        if self.fingerTable[0][0] != startNode:
            succ_ip = self.fingerTable[0][1]
            succ_port = self.fingerTable[0][2]
            newsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            newsock.connect((succ_ip, succ_port))
            newsock.sendall(b'printNodes ' + str(startNode).encode())
            newsock.close()
        else:
            print(sep)

    def printNeighbourInfo(self):
        print("I am " + str(self))
        print("My predecessor is " + str(self.predecessor))
        self.printFingerTable()
        print(sep)

    def printFingerTable(self):
        print("Here are my finger table entries: ")
        for i in range(HASH_BITS):
            print(str(i) + ': ' + str(self.fingerTable[i]))
        print(sep)

    def printMyDataContents(self):
        print("Contents of node: " + str(self) + " are: ")
        print(sep2)
        for item in self.dataTable:
            print(item)
        print(sep)

    def printAllContents(self, startNodeID=None):
        if (self.id == startNodeID):
            return
        elif (startNodeID == None):
            startNodeID = self.id
        self.printMyDataContents()
        resultString = 'allContents ' + str(startNodeID)
        self.sock.sendto(resultString, (self.fingerTable[0][1], self.fingerTable[0][2]))

    def run(self):
        if not self.dynamicNode:
            self.updateFingerTable()
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                self.sock.bind((self.ip, self.port))
            except:
                print("Port bind error")
                exit(1)
        else:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                self.sock.bind((self.ip, self.port))
            except:
                print("Port bind error")
                exit(1)
            self.sock.sendto("joinNetwork", (nodeIP, nodePort))
            data, addr = self.sock.recvfrom(recvBytes)
            data = data.split()
            self.fingerTable[0] = [int(data[0]), data[1], int(data[2])]
            self.predecessor = [int(data[3]), data[4], int(data[5])]
            self.initialize_finger_table()
            self.update_other_pointers()
            self.invoke_content_sharing()
            # self.updateContents()
            print("I have joined the network! Here are my neighbours:")
            self.printNeighbourInfo()


        while True:
            cmd, addr = self.sock.recvfrom(recvBytes)
            if cmd.startswith(b'printNodes'):
                startNode = self.id
                if cmd != b'printNodes':
                    startNode = int(cmd.split()[1])
                self.printNodes(startNode)

            elif cmd.startswith(b'findNode'):
                lst = cmd.split()
                if len(lst)==2:
                    #startNode
                    key = int(lst[1])
                    self.findNode(key, str(self.ip)+':'+str(self.port))
                else:
                    #intermediate
                    key = int(lst[1])
                    startNode = lst[2]
                    self.findNode(key, startNode)

            elif cmd.startswith(b'foundNode'):
                lst = cmd[len('foundNode '):].split()
                targetNode = lst[:3]
                targetPred = lst[3:]
                print("I am node: " + str(self))
                print("Target node is " + str(targetNode))
                print(sep)

            elif cmd.startswith(b'addMember'):
                msg = ' '.join(cmd.split()[1:])
                print("About to invoke addMember from node: " + str(self) + " for the message: *" + msg +"*")
                print(sep)
                thread1 = threading.Thread(target = self.putContent, args = (msg, ))
                thread1.start()

            elif cmd.startswith(b'vote'):
                msg = ' '.join(cmd.split()[1:])
                print("About to invoke vote from node: " + str(self) + " for the message: *" + msg +"*")
                print(sep)
                thread1 = threading.Thread(target = self.vote, args = (msg, ))
                thread1.start()

            elif cmd.startswith(b'putYourContent'):
                msg = ' '.join(cmd.split()[1:])
                print('Received a request to add msg:*'+msg+'* into my data. I am node: ' + str(self))
                print(sep)
                self.putInMyContent(msg)

            elif cmd.startswith(b'vYourContent'):
                msg = ' '.join(cmd.split()[1:])
                print('Received a request to vote for:*'+msg+'* in my data. I am node: ' + str(self))
                print(sep)
                self.voteInMyContent(msg)

            elif cmd.startswith(b'getScores'):
                msg = ' '.join(cmd.split()[1:])
                print("About to invoke getScores from node: " + str(self) + " for the message: *" + msg +"*")
                print(sep)
                thread2 = threading.Thread(target = self.getContent, args = (msg, ))
                thread2.start()

            elif cmd.startswith(b'getYourContent'):
                msg = ' '.join(cmd.split()[1:])
                print('Received a request to get msg:*'+msg+'* from my data. I am node: ' + str(self))
                print(sep)
                self.fetchMyContent(msg, addr)

            elif cmd.startswith(b'responseToQuery'):
                response = ' '.join(cmd.split()[1:])
                print('Got a response *' + response + '*. I am node: ' + str(self))
                print(sep)

            elif cmd.startswith(b'responseQuery2'):
                response = cmd.split()[1:]
                num = int(response[0])
                msgs = response[1:(-1)*num]
                msgs = (' '.join(msgs)).split('$$$')
                # for i in range(num):
                #     msgs[i] = msgs
                votes = response[(-1)*num:]
                print('Got a response. I am node: ' + str(self))
                for i in range(num):
                    print(votes[i] + " :: " + msgs[i])
                print(sep)

            elif cmd == b'joinNetwork':
                thread1 = threading.Thread(target = self.joinNetwork, args = (addr, ))
                thread1.start()

            elif cmd.startswith(b'changeNode'):
                # print(cmd)
                lst = cmd.split()
                if lst[1] == '0':
                    self.fingerTable[0] = [int(lst[2]), lst[3], int(lst[4])]
                    print("Updating my successor to " + str(self.fingerTable[0]) + ". I am node: " + str(self))
                    print(sep)
                elif (lst[1] == str(1)):
                    self.predecessor = [int(lst[2]), lst[3], int(lst[4])]
                    print("Updating my predecessor to " + str(self.predecessor)+ "I am node: " + str(self))
                    print(sep)

            elif cmd.startswith(b'newAdded'):
                lst = cmd.split()
                self.handleNewAdded(lst[1], lst[2], lst[3])

            elif cmd == b'myContents':
                self.printMyDataContents()

            elif cmd.startswith(b'allContents'):
                lst = cmd.split()
                if len(lst) == 1:
                    #startNode
                    self.printAllContents()
                else:
                    self.printAllContents(int(lst[1]))

            elif cmd.startswith(b'sendContents'):
                lst = cmd.split()[1:]
                newNode_addr = [lst[0], int(lst[1])]
                self.sendContentToNewNode(newNode_addr)

            elif cmd.startswith(b'contentUpdate'):
                lst = cmd.split()[1:]
                self.updateMyContent(lst)

            elif cmd == b'nInfo':
                self.printNeighbourInfo()

            elif cmd == b'fingerTable':
                self.printFingerTable()

            elif cmd == 'exit':
                self.sock.sendto('exit', (self.fingerTable[0][1], self.fingerTable[0][2]))
                break

        self.sock.close()


    def __str__(self):
        return str([self.id, self.ip, self.port])


if __name__ == "__main__":
    Nnodes = int(input("Number of nodes: "))
    allNodes = []
    print("Nodes in the network:")
    for i in range(Nnodes):
        allNodes.append([getKey(nodeIP, nodePort+i), nodeIP, nodePort+i])

    allNodes.sort()
    print("Nodes in the network:")
    for i in allNodes:
        print(i)
    print(sep)
    # print("Nodes in the network:")
    # for i in range(Nnodes):
    #     print(allNodes[i])

    nodeThreads = []

    for i in range(Nnodes):
        nodeThread = Node(allNodes[i][1], allNodes[i][2])
        nodeThread.predecessor = allNodes[(i-1)%Nnodes]
        nodeThread.start()
        nodeThreads.append(nodeThread)

    for node in nodeThreads:
        node.join()
