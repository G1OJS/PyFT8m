import threading, socket, json, time

class UdpComms:
    def __init__(self, listen_on_ports = None, rx_callback = None):
        self.running = True
        self.listen_on_ports = listen_on_ports
        self.rx_callback = rx_callback
        self.listening_sockets = []
        self.sock_out = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.thread = None
        if self.listen_on_ports is not None:
            if isinstance(self.listen_on_ports, int):
                self.listen_on_ports = [self.listen_on_ports]
            for port in self.listen_on_ports:
                self.listening_sockets.append(socket.socket(socket.AF_INET, socket.SOCK_DGRAM))
                self.listening_sockets[-1].settimeout(0.1)
                self.listening_sockets[-1].bind(('', port))
                
            self.thread = threading.Thread(target = self.listen, daemon = True).start()

    def _shutdown(self):
        self.running = False
        self.sock_out.close()
        print("UDP comms sending socket is closed\n")
        if self.listen_on_ports:
            self.sock_in.close()
            print(f"UDP comms listener is shut down for port {self.listen_on_port}\n")
        if self.thread and self.thread is not threading.current_thread():
            self.thread.join(timeout=0.5)
        
    def udp_send_dict(self, msg_dict, dest_host = 'localhost', dest_port = 0):
        self.udp_send_bytes(json.dumps(msg_dict).encode('utf-8'), dest_host = dest_host, dest_port = dest_port)

    def udp_send_bytes(self, msg_bytes, dest_host = 'localhost', dest_port = 0):
        if self.running:
            self.sock_out.sendto(msg_bytes, (dest_host, dest_port))
          #  print(f"[UDP] sent {f'{msg_dict}'[:25]} to {dest_port}")

    def listen(self):
        while self.running:
            sock_in  = self.listening_sockets[0]
                
            rx_bytes = None
            try:
                rx_bytes, _ = self.sock_in.recvfrom(1024)
            except:
                pass
            if rx_bytes:
                msg_dict = json.loads(rx_bytes.decode('utf-8'))
               # print(f"[UDP] received message {f'{msg_dict}'[:25]}")
                self.rx_callback(msg_dict)
                if msg_dict['mtype'] == 'shutdown':
                    self._shutdown()



