from connarchitecture.messenger import SocketMsgSenderReceiver
import sys
import select
import threading
from queue import Queue
import socket
import select
import argparse


class ClientThread(threading.Thread, SocketMsgSenderReceiver):
    CMD_QUIT = ('quit', 'quit client')
    COMMANDS = [CMD_QUIT]

    def __init__(self, host, port):
        threading.Thread.__init__(self)
        SocketMsgSenderReceiver.__init__(self)
        self._host = host
        self._port = port
        self._user_input_queue = Queue()
        self._continue = True

    def _hello(self, server_hello):
        hello = ""
        hello += server_hello
        hello += "Client commands:\n"
        hello += "-------------------------------------------------------------\n"
        for cmd in ClientThread.COMMANDS:
            hello += f"{cmd[0]:>10}: {cmd[1]}\n"
        hello += "=============================================================\n"
        return hello

    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((self._host, self._port))

            server_hello = self.recv_msg(sock)
            print(self._hello(server_hello))

            print("command>", end='', flush=True)
            while self._continue:
                try:
                    readable, w, x = select.select([sys.stdin, sock], [], [])
                except select.error as e:
                    break

                for s in readable:
                    if s is sys.stdin:
                        user_input = sys.stdin.readline().strip()
                        if user_input == ClientThread.CMD_QUIT[0]:
                            self.stop()
                            break
                        self.send_msg(sock, user_input)
                    elif s is sock:
                        data = self.recv_msg(sock)
                        if data:
                            print(data)
                            print("command>", end='', flush=True)
                        else:
                            self.stop()
                            break
        except socket.error as sock_err:
            print(sock_err)
        finally:
            sock.close()

    def stop(self):
        self._continue = False


class Client:
    def __init__(self, host, port):
        self._client_thread = ClientThread(host, port)

    def connect(self):
        self._client_thread.start()
        self._client_thread.join()

    def disconnect(self):
        self._client_thread.stop()


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-p', metavar='N', type=int, nargs='?', default=35813, help='use if you want to specify a custom port number.')
    arg_parser.add_argument('hostname', action='store', type=str, default='127.0.0.1',  help='use if you want to provide a custom hostname.')
    args = arg_parser.parse_args()

    port = args.p
    host = args.hostname

    client = Client(host, port)
    client.connect()


if __name__ == "__main__":
    main()
