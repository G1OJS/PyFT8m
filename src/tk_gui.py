import tkinter as tk
import time, threading, struct, random, json, socket, os, sys, psutil, subprocess
import numpy as np
import paho.mqtt.client as mqtt
from ast import literal_eval

MAX_REPORTS = 90

class PSKR_MQTT_listener:
    def __init__(self, home_square, on_spot):
        self.home_square = home_square
        self.on_spot = on_spot
        client_id = "PyFT8_" + ''.join(random.choice('0123456789ABCDEF') for i in range(16))
        mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id)
        mqttc.on_connect = self.on_connect
        mqttc.on_message = self.on_message
        try:
            mqttc.connect("mqtt.pskreporter.info", 1883, 60)
        except:
            print("[MQTT] connection error")
        threading.Thread(target = mqttc.loop_forever, daemon = True).start()

    def on_connect(self, client, userdata, flags, reason_code, properties):
        #pskr/filter/v2/{band}/{mode}/{sendercall}/{receivercall}/{senderlocator}/{receiverlocator}/{sendercouniTxRxy}/{receivercouniTxRxy}
        print(f"[MQTT] Requesting mqtt feed for {self.home_square}")
        client.subscribe(f"pskr/filter/v2/+/FT8/+/+/{self.home_square}/#")
        client.subscribe(f"pskr/filter/v2/+/FT8/+/+/+/{self.home_square}/#")

    def on_message(self, client, userdata, msg):
        try:
            d = literal_eval(msg.payload.decode())
        except:
            return
        self.on_spot(d)

class PSKR_upload:
    # https://pskreporter.info/pskdev.html
    # https://pskreporter.info/cgi-bin/psk-analysis.pl
    def __init__(self, mycall, mygrid, software):
        self.RxInfoRecDescriptor_CallLocSoft = b"\x00\x03\x00\x24\x99\x92\x00\x03\x00\x01\x80\x02\xFF\xFF\x00\x00\x76\x8F\x80\x04\xFF\xFF\x00\x00\x76\x8F\x80\x08\xFF\xFF\x00\x00\x76\x8F\x00\x00"
        self.SenderInfoRecDescriptor_SenderFreqSNRiMDModeSourceTime = b"\x00\x02\x00\x3C\x99\x93\x00\x07\x80\x01\xFF\xFF\x00\x00\x76\x8F\x80\x05\x00\x04\x00\x00\x76\x8F\x80\x06\x00\x01\x00\x00\x76\x8F\x80\x07\x00\x01\x00\x00\x76\x8F\x80\x0A\xFF\xFF\x00\x00\x76\x8F\x80\x0B\x00\x01\x00\x00\x76\x8F\x00\x96\x00\x04"
        self.last_descriptors_time = 0
        self.descriptors_sent_count = 0
        self.last_report_time = time.time() - 300 + 60
        self.udp_comms = UdpComms()
        self.addr = ("report.pskreporter.info", 4739)
        self.session_id = random.getrandbits(32)
        self.seq = 1
        self.reports = {}
        rx = self._enc_str(mycall) + self._enc_str(mygrid) + self._enc_str(software)
        self.rx_block =  self._block(b"\x99\x92", rx)
        self.lock = threading.Lock()
        print(f"[PSKR_upload] Spots will upload to pskreporter if Rx band is known")

    def add_report(self, msg_dict):
        if msg_dict['mtype'] == 'decode':
            their_snr, fHz = int(msg_dict['their_snr']), float(msg_dict['fHz'])
            dxcall, mode = msg_dict['msg_tuple'][1], "FT8"
            source, tt = 1, int(time.time())
            freq_hz = 14074000 + fHz
            with self.lock:
                self.reports[dxcall] = (dxcall, freq_hz, their_snr, mode, source, (tt // 15) * 15)

    def _enc_str(self, s):
        b = s.encode("ascii")
        return struct.pack("B", len(b)) + b

    def _block(self, block_type, payload):
        len_with_header = len(payload) + 4
        pad_len = (4 - (len_with_header % 4)) % 4
        len_with_pad = len_with_header + pad_len
        blk = block_type + struct.pack("!H", len_with_pad) + payload + b"\x00" * pad_len
        return blk 

    def send_reports(self):
        with self.lock:
            if len(self.reports) >= MAX_REPORTS or (time.time() - self.last_report_time) > 300:
                if (time.time() - self.last_descriptors_time) > 3600:
                    self.descriptors_sent_count = 0
                    self.last_descriptors_time = time.time()
                self._send(includeDescriptors = (self.descriptors_sent_count <4))
                self.descriptors_sent_count +=1
           
    def _send(self, includeDescriptors = False):
        if not self.reports:
            return
        tt = int(time.time())
        ipfx_header = struct.pack("!H", 10) + b"\x00\x00" + struct.pack("!I", tt) + struct.pack("!I", self.seq) + struct.pack("!I", self.session_id)
        header = ipfx_header
        if includeDescriptors:
            print(f"[pskr_upload] Packing descriptors")
            header = header + self.RxInfoRecDescriptor_CallLocSoft + self.SenderInfoRecDescriptor_SenderFreqSNRiMDModeSourceTime
        senders = bytearray()
        for dxcall, freq_hz, snr, mode, source, tt in self.reports.values():
            print(f"[pskr_upload] Packing report {dxcall}, {freq_hz}, {snr}, {mode}, {source}, {tt}")
            sender = self._enc_str(dxcall) + struct.pack("!I", int(freq_hz)) + struct.pack("b", int(snr)) + struct.pack("b", 0) + self._enc_str(mode) + struct.pack("B", source) + struct.pack("!I", tt)
            senders += sender
        packet = bytearray(header + self.rx_block + self._block(b"\x99\x93", senders))
        struct.pack_into("!H", packet, 2, len(packet))
        self.seq += len(self.reports)
        self.udp_comms.udp_send_bytes(packet, dest_host = self.addr[0], dest_port = self.addr[1])
        self.reports = {}
        self.last_report_time = time.time()
        print(f"[pskr_upload] Reports sent")

class ADIF:
    def __init__(self, logfile):
        self.adif_log_file = logfile
        ensure_file_exists(self.adif_log_file, header = "header <eoh>\n")
        self.cache = self._build_cache()
              
    def log(self, log_dict):
        with open(self.adif_log_file,'a') as f:
            for k, v in log_dict.items():
                v = str(v)
                f.write(f"<{k}:{len(v)}>{v} ")
            f.write(f"<eor>\n")
        cbm = log_dict['call'] + "_" + log_dict['band'] + "_FT8"
        tm = time_utils.time()
        self.cache[log_dict['call']] = tm
        self.cache[cbm] = tm

    def get_worked_before_info(self, their_call):
        wb_time = self.cache.get(their_call,'') 
        return f"wb: {time_utils.format_duration(time_utils.time() - float(wb_time))}" if wb_time else ''

    def _build_cache(self):
        import calendar, time
        def parse(rec, field):
            p = rec.find(field)
            if p<0:
                p = rec.find(field.upper())
            if p<0:
                p = rec.find(field.lower())
            if p > 0:
                p1, p2 = rec.find(':',p), rec.find('>',p)
                n = int(rec[p1+1:p2])
                return rec[p2+1: p2+1+n]
        cache = {}
        with open(self.adif_log_file, 'r') as f:
            for l in f.readlines():
                if parse(l, 'mode') == "FT8":
                    c, b, d, t = parse(l, 'call:'), parse(l, 'band'), parse(l, 'qso_date'), parse(l, 'time_on')
                    if c and b and d and t:
                        time_tuple = time.strptime(d+t, "%Y%m%d%H%M%S")
                        tm = calendar.timegm(time_tuple)
                        cache[c] = tm
                        cache[c + "_"+b+"_FT8"] = tm
        return cache

class Rig:
    def __init__(self, hamlib_port):
        self._ensure_hamlib_running()
        time.sleep(0.1)
        self.hamlib_port = hamlib_port
        self.sock_hamlib = socket.create_connection(('localhost', self.hamlib_port))
        time.sleep(0.1)
        self._send_tcp("M PKTUSB 0")

    def _ensure_hamlib_running(self):
        com_rig, com_baud, rigctld, rig_code = 'COM4',9600,'C:/WSJT/wsjtx/bin/rigctld-wsjtx', 3070
        # above 4 params to go in config eventually
        if not any(['rigctld' in i.name() for i in psutil.process_iter()]):
            cmd = f"{rigctld} -m {rig_code} -r {com_rig} -s {com_baud}"
            threading.Thread(target = subprocess.run, args = (cmd,)).start()   

    def shutdown(self):
        self.sock_hamlib.close()
        print("Rig control is shut down")

    def _send_tcp(self, cmd):
        self.sock_hamlib.sendall((cmd + "\n").encode())

    def start_transmit(self):
        self._send_tcp(f"T 1")

    def stop_transmit(self):
        self._send_tcp(f"T 0")
        

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
    def __init__(self, tx_cmd_port = 2122, rx_msg_port = 2121, rx_cmd_port = 2123, pskr_uploader_msg_port = 2123, config_location = '',
                 hamlib_port = 4532, max_tx_cycletime_start = 3, settings = None):
        self.tx_cmd_port = tx_cmd_port
        self.rx_cmd_port = rx_cmd_port
        self.transmit_starter = None
        self.max_tx_cycletime_start = max_tx_cycletime_start 
        self.settings = settings
        self.rig = Rig(hamlib_port)
        self.call_hashes = {}

        self.root = tk.Tk()
        self.root.protocol("WM_DELETE_WINDOW", lambda: self._graceful_exit())
        self.app_container = tk.Frame(self.root)
        self.app_container.pack(side = 'top')
        self.sidebar_container = tk.Frame(self.app_container)
        self.sidebar_container.pack(side = 'left', fill = 'both')
        self.info_container = tk.Frame(self.app_container)
        self.info_container.pack(side = 'top', fill = 'both')
        self.waterfall_container = tk.Frame(self.app_container)
        self.waterfall_container.pack(side = 'top', fill = 'both')
        self.decodes_container = tk.Frame(self.app_container) 
        self.decodes_container.pack(side = 'top')

        self.info_label = tk.Label(self.info_container, text = 'text', bg = '#707070')
        self.info_label.pack(side = 'top', fill = 'both')

        self.buttons = []
        bc = self.sidebar_container
        self.buttons.append(tk.Button(bc, width = 10, text = 'Settings', command = self.open_settings))
        self.buttons.append(tk.Button(bc, width = 10, text = 'CQ', command = self.call_cq))
        self.buttons.append(tk.Button(bc, width = 10, text = 'STOP TX', command = self.stop_transmit))
        for btn in self.buttons:
            btn.pack(side = 'top', anchor = 'n')
        
        self.waterfall_canvas = tk.Canvas(self.waterfall_container, height = 100, bg = '#909090')
        self.waterfall_canvas.pack(side = 'top', fill = 'both')
        self.waterfall_line = self.waterfall_canvas.create_line(0,0,600,0, fill = 'green', width = 2)
        self.waterfall_vals = None
        self.waterfall_new_vals = None
        
        self.scrollbar = tk.Scrollbar(self.decodes_container)
        self.scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.text_widget = tk.Text(self.decodes_container, wrap=tk.WORD, yscrollcommand=self.scrollbar.set)
        self.text_widget.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        fontsize = 10
        self.text_widget.tag_config('norm', foreground = 'white', background = 'blue', font=('Helvetica', fontsize, 'bold'))
        self.text_widget.tag_config('info', foreground = 'black', background = 'white', font=('Helvetica', fontsize, 'bold'))
        self.text_widget.tag_config('cq', foreground = 'white', background = 'green', font=('Helvetica', fontsize, 'bold'))
        self.text_widget.tag_config('to_me', foreground = 'white', background = 'red', font=('Helvetica', fontsize, 'bold'))        
        self.text_widget.tag_config('from_me', foreground = 'black', background = 'yellow', font=('Helvetica', fontsize, 'bold'))
        self.text_widget.bind('<Button-1>', self.row_click)

        self.scrollbar.config(command=self.text_widget.yview)
        self.first_decode = False
        
        self.init_qso_vars()
        self.pskr_upload = PSKR_upload(self.settings.get('my_call'), self.settings.get('my_grid'), "PyFT8m")
        self.tx_cmd_port
        self.text_widget.insert(tk.END, f"PyFT8m\n", 'info')
        self.udp_comms = UdpComms(rx_msg_port, self._process_udp_msg)
        self.update_waterfall()
        self.update_pskr_uploader()
        self.root.mainloop()

    def _graceful_exit(self):
        self.rig.shutdown()
        self.udp_comms.udp_send_dict({'mtype':'shutdown_all'}, dest_port = self.rx_cmd_port)
        self.udp_comms.udp_send_dict({'mtype':'shutdown_all'}, dest_port = self.tx_cmd_port)
        self.root.after(0, lambda: self.root.destroy())
        time.sleep(0.5)
        sys.exit(0)

    def open_settings(self):
        self.settings.open()

    def _process_udp_msg(self, msg_dict):
        display_text = ''
        if msg_dict['mtype'] == 'decode':
            self.pskr_upload.add_report(msg_dict)
            their_snr, fHz, dt, msg_tuple = msg_dict['their_snr'], f"{float(msg_dict['fHz']):07.2f}", msg_dict['dt'], msg_dict['msg_tuple']
            idx = 1 * msg_tuple[0].startswith("CQ") + 2* msg_tuple[0].startswith(self.my_call) + 3 * (msg_tuple[1] == self.my_call)
            display_type = ['norm','cq','to_me','from_me', 'from_me'][idx]
            display_text = f"{their_snr:4s} {dt:5s} {fHz:7s} ~ {' '.join(msg_tuple)}"
            if msg_tuple[1] == self.their_call:
                reply = self.determine_reply(' '.join(msg_tuple))
                self.queue_transmit(reply)
        elif msg_dict['mtype'] == 'rollover':
            display_type = 'info'
            display_text = ''
            self.first_decode = False
        elif msg_dict['mtype'] == 'waterfall':
            self.waterfall_new_vals = [int(v) for v in msg_dict['data'].split(',')]
            display_text = ''
        if display_text:
            if not self.first_decode:
                self.text_widget.delete(1.0, tk.END)
                self.first_decode = True
            self.text_widget.insert(tk.END, f"{display_text}\n", display_type)
            self.text_widget.see('end')

    def update_pskr_uploader(self):
        self.pskr_upload.send_reports()
        self.root.after(60, self.update_pskr_uploader)

    def update_waterfall(self):
        if self.waterfall_vals is None:
            self.waterfall_vals = self.waterfall_new_vals
        if self.waterfall_vals is not None:
            n = len(self.waterfall_vals)
            v, nv = self.waterfall_vals, self.waterfall_new_vals
            self.waterfall_vals = [max(0.8*v[i], nv[i]) for i in range(n)]
            dw = self.waterfall_canvas.winfo_width()/n
            xys = [(i*dw, 100-v) for i,v in enumerate(self.waterfall_vals)]
            self.waterfall_canvas.coords(self.waterfall_line, xys)
        self.waterfall_canvas.after(250, self.update_waterfall)

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
                self.their_call = ''
            if grid_rpt == 'RR73':
                reply = f"{self.their_call} {self.my_call} 73"
                self.their_call = ''
        else:
            reply = ''
        return reply

    def row_click(self, e):
        curr = e.widget.index("current").split('.')[0]
        row_txt = e.widget.get(f"{curr}.0", f"{curr}.end")
        if "~" in row_txt:
            rx_message = row_txt.split('~')[1][1:]
            self.their_snr = row_txt[:3]
            reply = self.determine_reply(rx_message)
            self.queue_transmit(reply)

    def call_cq(self):
        if self.my_call and self.my_grid:
            self.queue_transmit(f"CQ {self.my_call} {self.my_grid}")

    def queue_transmit(self, message):
        T_CYC, TX_T0 = 15, 0.5
        self.transmit_starter = None
        if message:
            mtx = self.max_tx_cycletime_start
            ct = (time.time() - TX_T0) % T_CYC
            delay =  0 if ct < mtx else T_CYC - ct
            self.tx_message = message
            self.transmit_starter = self.root.after(int(delay * 1000), self.start_transmit)
            self.root.after(int(delay * 1000 + 13000), self.stop_transmit)

    def start_transmit(self):
        self.rig.start_transmit()
        self.udp_comms.udp_send_dict({'mtype':'transmit', 'message':self.tx_message}, dest_port = self.tx_cmd_port)
        self.transmit_starter = None

    def stop_transmit(self):
        self.rig.stop_transmit()
        if self.transmit_starter:
            self.root.after_cancel(self.transmit_starter)

if __name__ == "__main__":
    from PyFT8m import Receiver, Transmitter, UdpComms
    
    config_location = os.path.join(os.path.expanduser("~"), 'PyFT8m.cfg')
    settings = Settings(config_location)

    if settings.get('tx_keywords') == '' or settings.get('rx_keywords') == '':
        settings.open()
        print("Please close and re-open after editing")
    else:       
        rx = Receiver(max_freq = 2900, latest_decode = 2, rx_msg_port = 2121, input_keywords = settings.get('rx_keywords'))
        tx = Transmitter(tx_cmd_port = 2122, output_keywords = settings.get('tx_keywords'))

        gui = Gui(tx_cmd_port = 2122, rx_msg_port = 2121, rx_cmd_port = 2123, pskr_uploader_msg_port = 2123, settings = settings,
                  config_location = config_location, hamlib_port = 4532, max_tx_cycletime_start = 3)

