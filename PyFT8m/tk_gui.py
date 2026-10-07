import tkinter as tk
import time, threading, socket, queue, json
import numpy as np

myCall, myGrid = "G1OJS", "IO90"

their_snr = None
in_qso_with = ''

def determine_reply(rx_message):
    global their_snr, in_qso_with
    hail, their_call, grid_rpt = rx_message.split(' ')
    in_qso_with = their_call
    if hail.startswith("CQ"):
        reply = f"{their_call} {myCall} {myGrid[:4]}"   
    elif hail.startswith(myCall):
        reply = f"{their_call} {myCall} {their_snr}"
        if any([m for m in ['+','-'] if m in grid_rpt]):
            reply = f"{their_call} {myCall} R{their_snr}"
        if any([m for m in ['R+','R-','RRR'] if m in grid_rpt]):
            reply = f"{their_call} {myCall} RR73"
        if grid_rpt == 'RR73':
            reply = f"{their_call} {myCall} 73"
            in_qso_with = ''
    return reply

class Gui:
    def __init__(self, sock_gui_cmd = 2122, sock_rcvr_out = 2121):
        self.sock_gui_cmd = sock_gui_cmd
        self.call_hashes = {}
        self.sock_in = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock_in.bind(('', sock_rcvr_out))
        self.sock_out = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_in = queue.Queue()
        self.root = tk.Tk()
        self.app_container = tk.Frame(self.root)

        self.sidebar_container = tk.Frame(self.app_container, height = 500, width = 100)
        self.sidebar_container.pack(side = 'left', fill = 'y')
        self.buttons = []
        bc = self.sidebar_container
        self.buttons.append(tk.Button(bc, text = 'CQ', command = self.call_cq))
        for btn in self.buttons:
            btn.pack(side = 'top', anchor = 'n')
        
        self.waterfall_container = tk.Frame(self.app_container, height = 100, width = 500)
        self.waterfall_canvas = tk.Canvas(self.waterfall_container, height = 100, width = 500, bg = 'pink')
        self.waterfall_canvas.pack(side = 'top')
        self.waterfall_line = self.waterfall_canvas.create_line(0,0,500,0, fill = 'green', width = 2)
        self.waterfall_vals = None
        self.waterfall_new_vals = None
        
        self.decodes_container = tk.Frame(self.app_container) 
        self.scrollbar = tk.Scrollbar(self.decodes_container)
        self.scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.text_widget = tk.Text(self.decodes_container, wrap=tk.WORD, yscrollcommand=self.scrollbar.set)
        self.text_widget.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        fontsize = 10
        self.text_widget.tag_config('norm', foreground = 'white', background = 'blue', font=('Helvetica', fontsize))
        self.text_widget.tag_config('info', foreground = 'black', background = 'white', font=('Helvetica', fontsize))
        self.text_widget.tag_config('cq', foreground = 'white', background = 'green', font=('Helvetica', fontsize, 'bold'))
        self.text_widget.tag_config('to_me', foreground = 'white', background = 'red', font=('Helvetica', fontsize, 'bold'))        
        self.text_widget.tag_config('from_me', foreground = 'black', background = 'yellow', font=('Helvetica', fontsize, 'bold'))
        self.text_widget.bind('<Button-1>', self.row_click)

        self.scrollbar.config(command=self.text_widget.yview)
        self.first_decode = False
        self.waterfall_container.pack(side = 'top')
        self.app_container.pack(side = 'top')
        self.decodes_container.pack(side = 'top')

        self.current_decodes = []
        self.root.bind("<<received_udp>>", self.received_udp)
        threading.Thread(target = self.monitor_udp, daemon = True).start()
        self.text_widget.insert(tk.END, f"PyFT8m\n", 'info')
        self.update_waterfall()
        self.root.mainloop()
        
    def send_udp(self, msg):
        self.sock_out.connect(('localhost', self.sock_gui_cmd))
        self.sock_out.send(json.dumps(msg).encode('utf-8'))

    def monitor_udp(self):
        while True:
            time.sleep(0.1)
            rx_bytes, _ = self.sock_in.recvfrom(1024)
            if rx_bytes:
                self.udp_in.put(json.loads(rx_bytes.decode('utf-8')))
                self.root.after(0, lambda: self.root.event_generate("<<received_udp>>"))

    def received_udp(self, e):
        msg_dict = self.udp_in.get()
        display_text = ''
        if msg_dict['mtype'] == 'decode':
            self.current_decodes.append(msg_dict)
            their_snr, fHz, dt, msg_tuple = msg_dict['their_snr'], msg_dict['fHz'], msg_dict['dt'], msg_dict['msg_tuple'], 
            idx = 1 * msg_tuple[0].startswith("CQ") + 2* msg_tuple[0].startswith(myCall) + 3 * (msg_tuple[1] == myCall)
            display_type = ['norm','cq','to_me','from_me', 'from_me'][idx]
            display_text = f"{their_snr:4s} {dt:5s} {fHz:6s} ~ {' '.join(msg_tuple)}"
            if msg_tuple[1] == in_qso_with:
                reply = determine_reply(' '.join(msg_tuple))
                self.send_udp({'mtype':'transmit', 'message':reply})
        elif msg_dict['mtype'] == 'rollover':
            display_type = 'info'
            display_text = ''
            self.first_decode = False
        elif msg_dict['mtype'] == 'waterfall':
            self.waterfall_new_vals = [100 - int(v) for v in msg_dict['data'].split(',')]
            display_text = ''
        if display_text:
            if not self.first_decode:
                self.text_widget.delete(1.0, tk.END)
                self.first_decode = True
            self.text_widget.insert(tk.END, f"{display_text}\n", display_type)
            self.text_widget.see('end')

    def update_waterfall(self):
        if self.waterfall_vals is None:
            self.waterfall_vals = self.waterfall_new_vals
        if self.waterfall_vals is not None:
            n = len(self.waterfall_vals)
            v, nv = self.waterfall_vals, self.waterfall_new_vals
            self.waterfall_vals = [(5*v[i] + nv[i]) / 6 for i in range(n)]
            dw = 500/n
            xys = [(i*dw, v) for i,v in enumerate(self.waterfall_vals)]
            self.waterfall_canvas.coords(self.waterfall_line, xys)
        self.waterfall_canvas.after(500, self.update_waterfall)

    def row_click(self, e):
        global their_snr
        curr = e.widget.index("current").split('.')[0]
        row_txt = e.widget.get(f"{curr}.0", f"{curr}.end")
        if "~" in row_txt:
            rx_message = row_txt.split('~')[1][1:]
            their_snr = row_txt[:3]
            reply = determine_reply(rx_message)
            self.send_udp({'mtype':'transmit', 'message':reply})

    def call_cq(self):
        print("click")
        self.send_udp({'mtype':'transmit', 'message':f"CQ {myCall} {myGrid}"})

