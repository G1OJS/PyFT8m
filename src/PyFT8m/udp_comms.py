import threading, socket, json, time

class UdpComms:
    def __init__(self, ports = {'listen':None,'send':None}, rx_callback = None, remote_host = 'localhost'):
        self.running = True
        self.ports = ports
        self.remote_host = remote_host
        self.rx_callback = rx_callback
        self.sock_listen = None
        self.sock_send = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.thread = None
        if self.ports['listen']:
            self.sock_listen = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock_listen.settimeout(0.1)
            self.sock_listen.bind(('', self.ports['listen']))
            self.thread = threading.Thread(target = self.listen, daemon = True).start()

# listener ==============================
    def listen(self):
        while self.running:
            rx_bytes = None
            try:
                rx_bytes, _ = self.sock_listen.recvfrom(1024)
            except:
                pass
            if rx_bytes:
                msg_dict = json.loads(rx_bytes.decode('utf-8'))
                self.rx_callback(msg_dict)
                if msg_dict['mtype'] == 'shutdown':
                    self.shutdown()
                    
    def shutdown(self):
        self.running = False
        self.sock_send.close()
        print("UDP comms sending socket is closed\n")
        if self.ports['listen']:
            self.sock_listen.close()
            print(f"UDP comms listener is shut down for port {self.ports['listen']}\n")
        if self.thread and self.thread is not threading.current_thread():
            self.thread.join(timeout=0.5)
        
# senders ==============================
    def udp_send_dict(self, msg_dict):
        self.udp_send_bytes(json.dumps(msg_dict).encode('utf-8'))

    def udp_send_bytes(self, msg_bytes):
        if self.running:
            self.sock_send.sendto(msg_bytes, (self.remote_host, self.ports['send']))


