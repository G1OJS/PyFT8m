import tkinter as tk
import time, threading, socket, queue, json, os, sys
import numpy as np

class Settings:
    def __init__(self, config_location):
        self.root = tk.Tk()
        self.root.protocol("WM_DELETE_WINDOW", self._iconify)
        self.cfg_file = config_location
        self.cfg_vars = {'my_call':tk.StringVar(),'my_grid':tk.StringVar(),
                         'tx_keywords':tk.StringVar(),'rx_keywords':tk.StringVar()}
        self.cfg_labels = {'my_call':'My call','my_grid':'My grid',
                           'tx_keywords':'Sound out keywords','rx_keywords':'Sound in keywords'}
        self.cfg_var_entries = []
        for cfg_var in self.cfg_vars:
            self.cfg_var_entries.append(self._labelled_entry(self.root, cfg_var))
        for widg in self.cfg_var_entries:
            widg.pack(side = 'top')
        self._load()
        self._iconify()

    def _labelled_entry(self, parent, cfg_var):
        frm = tk.Frame(parent)
        self.ent = tk.Entry(frm, textvariable = self.cfg_vars[cfg_var])
        label_text = self.cfg_labels[cfg_var] if cfg_var in self.cfg_labels else cfg_var
        tk.Label(frm, text = label_text).pack(side = 'left')
        self.ent.pack(side = 'left')
        self.ent.delete('0', 'end')
        self.ent.insert('0', self.cfg_vars[cfg_var].get())
        return frm

    def get(self, cfg_var):
        return self.cfg_vars[cfg_var].get()

    def open(self, modal = False):
        self.root.deiconify()

    def _iconify(self):
        self._save()
        self.root.iconify()

    def _save(self):
        cfg_dict = {k: v.get() for k, v in self.cfg_vars.items()}
        with open(self.cfg_file, 'w') as f:
            json.dump(cfg_dict, f)

    def _load(self):
        if os.path.exists(self.cfg_file):
            with open(self.cfg_file, 'r') as f:
                cfg_dict = json.load(f)
            for k in cfg_dict:
                if k in self.cfg_vars:
                    self.cfg_vars[k].set(cfg_dict[k])

class Gui:
    def __init__(self, sock_gui_cmd = 2122, sock_rcvr_out = 2121, config_location = '', rx_start = None, tx_start = None):
        self.sock_gui_cmd = sock_gui_cmd
        self.settings = Settings(config_location)
        self.rx_start = rx_start
        self.tx_start = tx_start
        self.call_hashes = {}
        self.sock_in = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock_in.bind(('', sock_rcvr_out))
        self.sock_out = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_in = queue.Queue()
        self.root = tk.Tk()
        self.root.protocol("WM_DELETE_WINDOW", self._graceful_exit)
        self.app_container = tk.Frame(self.root)

        self.sidebar_container = tk.Frame(self.app_container, height = 500, width = 100)
        self.sidebar_container.pack(side = 'left', fill = 'y')
        self.buttons = []
        bc = self.sidebar_container
        self.buttons.append(tk.Button(bc, text = 'Settings', command = self.open_settings))
        self.buttons.append(tk.Button(bc, text = 'CQ', command = self.call_cq))
        self.buttons.append(tk.Button(bc, text = 'STOP', command = self.stop_transmit))
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

        if self.settings.get('tx_keywords') == '' or self.settings.get('rx_keywords') == '':
            self.settings.open()
            print("Please close and re-open after editing")
        else:
            self.init_qso_vars()
            self.rx_start(self.settings.get('rx_keywords'))
            self.tx_start(self.settings.get('tx_keywords'))
        
        self.shutdown = False
        self.current_decodes = []
        self.root.bind("<<received_udp>>", self.received_udp)
        threading.Thread(target = self.monitor_udp, daemon = True).start()
        self.text_widget.insert(tk.END, f"PyFT8m\n", 'info')
        self.update_waterfall()
        self.root.mainloop()

    def _graceful_exit(self):
        self.shutdown = True
        self.sock_out.close()
        self.root.destroy()
        sys.exit(1)

    def open_settings(self):
        self.settings.open()
        
    def send_udp(self, msg):
        self.sock_out.connect(('localhost', self.sock_gui_cmd))
        self.sock_out.send(json.dumps(msg).encode('utf-8'))

    def monitor_udp(self):
        while not self.shutdown:
            time.sleep(0.1)
            rx_bytes, _ = self.sock_in.recvfrom(1024)
            if rx_bytes and not self.shutdown:
                self.udp_in.put(json.loads(rx_bytes.decode('utf-8')))
                self.root.after(0, lambda: self.root.event_generate("<<received_udp>>"))
        self.sock_in.close()

    def received_udp(self, e):
        msg_dict = self.udp_in.get()
        display_text = ''
        if msg_dict['mtype'] == 'decode':
            self.current_decodes.append(msg_dict)
            their_snr, fHz, dt, msg_tuple = msg_dict['their_snr'], msg_dict['fHz'], msg_dict['dt'], msg_dict['msg_tuple'], 
            idx = 1 * msg_tuple[0].startswith("CQ") + 2* msg_tuple[0].startswith(self.my_call) + 3 * (msg_tuple[1] == self.my_call)
            display_type = ['norm','cq','to_me','from_me', 'from_me'][idx]
            display_text = f"{their_snr:4s} {dt:5s} {fHz:6s} ~ {' '.join(msg_tuple)}"
            if msg_tuple[1] == self.their_call:
                reply = self.determine_reply(' '.join(msg_tuple))
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

    def init_qso_vars(self):
        self.their_call = ''
        self.their_snr = -30
        self.my_call = self.settings.get('my_call')
        self.my_grid = self.settings.get('my_grid')

    def determine_reply(self, rx_message):
        hail, self.their_call, grid_rpt = rx_message.split(' ')
        if hail.startswith("CQ"):
            reply = f"{self.their_call} {self.my_call} {self.my_grid[:4]}"   
        elif hail.startswith(self.my_call):
            reply = f"{self.their_call} {self.my_call} {self.their_snr}"
            if any([m for m in ['+','-'] if m in grid_rpt]):
                reply = f"{self.their_call} {self.my_call} R{self.their_snr}"
            if any([m for m in ['R+','R-','RRR'] if m in grid_rpt]):
                reply = f"{self.their_call} {self.my_call} RR73"
            if grid_rpt == 'RR73':
                reply = f"{self.their_call} {self.my_call} 73"
                self.their_call = ''
        return reply

    def row_click(self, e):
        curr = e.widget.index("current").split('.')[0]
        row_txt = e.widget.get(f"{curr}.0", f"{curr}.end")
        if "~" in row_txt:
            rx_message = row_txt.split('~')[1][1:]
            self.their_snr = row_txt[:3]
            reply = self.determine_reply(rx_message)
            self.send_udp({'mtype':'transmit', 'message':reply})

    def call_cq(self):
        if self.my_call and self.my_grid:
            self.send_udp({'mtype':'transmit', 'message':f"CQ {self.my_call} {self.my_grid}"})

    def stop_transmit(self):
        self.send_udp({'mtype':'stop_transmit'})


if __name__ == "__main__":
    from PyFT8m import Receiver, Transmitter
    
    config_location = os.path.join(os.path.expanduser("~"), 'PyFT8m.cfg')

    rx = Receiver(max_freq = 2900, latest_decode = 2, sock_rcvr_out = 2121)
    tx = Transmitter(max_tx_cycletime_start = 3, sock_gui_cmd = 2122)
    tx.init_hamlib(com_rig = 'COM4', com_baud = 9600, rigctld = 'C:/WSJT/wsjtx/bin/rigctld-wsjtx', rig_code = 3070, hamlib_port = 4532)

    gui = Gui(sock_gui_cmd = 2122, sock_rcvr_out = 2121, config_location = config_location, rx_start = rx.start, tx_start = tx.start)

