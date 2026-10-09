import threading, socket, json, time
global UdpCommsRunning
UdpCommsRunning = True

class UdpComms:
    def __init__(self, listen_on_port = None, rx_callback = None):
        self.listen_on_port = listen_on_port
        self.rx_callback = rx_callback
        self.sock_out = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if listen_on_port:
            self.sock_in = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock_in.bind(('', listen_on_port))
            threading.Thread(target = self.listen).start()

    def udp_send_dict(self, msg_dict, dest_host = 'localhost', dest_port = 0):
        self.udp_send_bytes(json.dumps(msg_dict).encode('utf-8'), dest_host = dest_host, dest_port = dest_port)

    def udp_send_bytes(self, msg_bytes, dest_host = 'localhost', dest_port = 0):
        if UdpCommsRunning:
            self.sock_out.connect((dest_host, dest_port))
            self.sock_out.sendall(msg_bytes)
          #  print(f"[UDP] sent {f'{msg_dict}'[:25]} to {dest_port}")

    def listen(self):
        global UdpCommsRunning
        while UdpCommsRunning:
            time.sleep(0.1)
            rx_bytes, _ = self.sock_in.recvfrom(1024)
            if rx_bytes:
                msg_dict = json.loads(rx_bytes.decode('utf-8'))
               # print(f"[UDP] listen_on_port {self.listen_on_port} received message {f'{msg_dict}'[:25]}")
                self.rx_callback(msg_dict)
                if msg_dict['mtype'] == 'shutdown_all':
                    UdpCommsRunning = False
        self.sock_in.close()
        self.sock_out.close()
        print("Udp comms is shut down")

