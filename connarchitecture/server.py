from connarchitecture.models import ServerEvent
from connarchitecture.messenger import SocketMsgSenderReceiver
from connarchitecture.logging_component import LoggingComponent
import threading
import socket
import select
from abc import ABC, abstractmethod


class ClientConnectionThread(ABC, threading.Thread, SocketMsgSenderReceiver, LoggingComponent):
    def __init__(self, server, connection, address):
        threading.Thread.__init__(self)
        SocketMsgSenderReceiver.__init__(self)
        name = f"{address[0]}:{address[1]}"
        LoggingComponent.__init__(self, name)
        self._connection = connection
        self._server = server
        self.setDaemon(True)

    def run(self):
        self.log(f"connected")

        try:
            hello = self._server.get_hello_message()
            self.send_msg(self._connection, hello)

            while True:
                message = self.recv_msg(self._connection)
                if message:
                    server_event = ServerEvent(message, self._connection, self.get_name())
                    self._server.event_received(server_event)
        finally:
            self._connection.close()


class ThreadedServer(threading.Thread, SocketMsgSenderReceiver, LoggingComponent):
    def __init__(self, name, host, port):
        threading.Thread.__init__(self)
        LoggingComponent.__init__(self, name)
        self._host = host
        self._port = port
        self._continue = True
        self.setDaemon(True)

    def _setup_socket(self, host, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.listen(5)
        return sock

    def run(self):
        self.log(f"server started at {self._host}:{self._port}")
        sock = None
        try:
            sock = self._setup_socket(self._host, self._port)

            # Sockets from which we expect to read
            inputs = [sock]

            # Sockets to which we expect to write
            outputs = []

            while self._continue:
                readable, _, _ = select.select(inputs, outputs, inputs)

                for s in readable:
                    if s is sock:
                        # A "readable" server socket is ready to accept a connection
                        connection, client_address = s.accept()
                        client_thread = ClientConnectionThread(
                            self, connection, client_address)
                        client_thread.start()

        finally:
            if sock:
                sock.close()

    def stop_server(self):
        self._continue = False

    @abstractmethod
    def get_hello_message(self):
        pass

    @abstractmethod
    def event_received(self, event):
        pass
