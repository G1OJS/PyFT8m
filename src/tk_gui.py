import tkinter as tk
import time, threading, struct, random, json, socket, os, queue, sys, psutil, subprocess
import numpy as np
import paho.mqtt.client as mqtt
from ast import literal_eval
from PyFT8m import Receiver, Transmitter, UdpComms

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
    def __init__(self, mycall, mygrid, software, pskr_addr = {'host':'report.pskreporter.info', 'port':4739}):
        self.pskr_addr = pskr_addr
        self.udp_comms_pskr = UdpComms(ports = {'listen':None, 'send':pskr_addr['port']}, remote_host = pskr_addr['host'])
        self.RxInfoRecDescriptor_CallLocSoft = b"\x00\x03\x00\x24\x99\x92\x00\x03\x00\x01\x80\x02\xFF\xFF\x00\x00\x76\x8F\x80\x04\xFF\xFF\x00\x00\x76\x8F\x80\x08\xFF\xFF\x00\x00\x76\x8F\x00\x00"
        self.SenderInfoRecDescriptor_SenderFreqSNRiMDModeSourceTime = b"\x00\x02\x00\x3C\x99\x93\x00\x07\x80\x01\xFF\xFF\x00\x00\x76\x8F\x80\x05\x00\x04\x00\x00\x76\x8F\x80\x06\x00\x01\x00\x00\x76\x8F\x80\x07\x00\x01\x00\x00\x76\x8F\x80\x0A\xFF\xFF\x00\x00\x76\x8F\x80\x0B\x00\x01\x00\x00\x76\x8F\x00\x96\x00\x04"
        self.last_descriptors_time = 0
        self.descriptors_sent_count = 0
        self.last_report_time = time.time() - 300 + 60
        self.session_id = random.getrandbits(32)
        self.seq = 1
        self.reports = {}
        rx = self._enc_str(mycall) + self._enc_str(mygrid) + self._enc_str(software)
        self.rx_block =  self._block(b"\x99\x92", rx)
        self.lock = threading.Lock()
        print(f"[PSKR_upload] Spots will upload to pskreporter if Rx band is known")

    def shutdown(self):
        self.udp_comms_pskr.shutdown()

    def add_report(self, dial_freq_Hz, msg_dict):
        if msg_dict['mtype'] == 'decode':
            their_snr, fHz = int(msg_dict['their_snr']), float(msg_dict['fHz'])
            dxcall, mode = msg_dict['msg_tuple'][1], "FT8"
            source, tt = 1, int(time.time())
            freq_hz = dial_freq_Hz + fHz
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
        self.udp_comms_pskr.udp_send_bytes(packet)
        self.reports = {}
        self.last_report_time = time.time()
        print(f"[pskr_upload] Reports sent")

class ADIF:
    def __init__(self, logfile):
        self.adif_log_file = logfile
        if not os.path.exists(self.adif_log_file):
            with open(self.adif_log_file, 'w') as f:
                f.write("header <eoh>\n")
        self.cache = self._build_cache()
              
    def log(self, my_call, my_grid, their_call, their_grid, their_snr, my_snr, fHz):
        gmt = time.gmtime()
        band = '20m'
        log_dict = {'operator':my_call, 'station_callsign':my_call, 'my_gridsquare':my_grid, 'mode':'FT8',
                    'time_on': time.strftime("%H%M%S", gmt), 'qso_date':time.strftime("%Y%m%d", gmt),
                    'band':band, 'freq':int(fHz/1e6),
                    'call':their_call, 'gridsquare': their_grid, 'rst_sent':their_snr, 'rst_rcvd':my_snr}
        with open(self.adif_log_file,'a') as f:
            for k, v in log_dict.items():
                v = str(v)
                if len(v) > 0:
                    f.write(f"<{k}:{len(v)}>{v} ")
            f.write(f"<eor>\n")
        cbm = log_dict['call'] + "_" + log_dict['band'] + "_FT8"
        tm = time.time()
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
    def __init__(self, hamlib_port = 4532):
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

    def ptt_on(self):
        self._send_tcp(f"T 1")

    def ptt_off(self):
        self._send_tcp(f"T 0")
        

class Settings:
    def __init__(self):
        self.root = tk.Tk()
        self.root.protocol("WM_DELETE_WINDOW", self.quit)
        self.initialise()
        self._load()
        self.root.iconify()

    def show(self):          
        self.root.deiconify()
        self.root.mainloop()

    def initialise(self):
        config_folder = os.path.expanduser("~")
        self.cfg_file = f"{config_folder}/PyFT8m.cfg"
        cfg_items = ['My call', 'My grid', 'Sound out keywords', 'Sound in keywords']
        self.cfg = {}
        for cfg_item in cfg_items:
            frm = tk.Frame(self.root)
            var = tk.StringVar()
            ent = tk.Entry(frm, textvariable = var)
            lbl = tk.Label(frm, text = cfg_item)
            lbl.pack(side = 'left')
            ent.pack(side = 'left')
            self.cfg[cfg_item] = var
            frm.pack(side = 'top')
        self.cfg['config_folder'] = tk.StringVar()
        self.cfg['config_folder'].set(config_folder)

    def get(self, cfg_item):
        return self.cfg[cfg_item].get()

    def allOK(self):
        return all([self.cfg[s].get() for s in ['My call', 'My grid', 'Sound out keywords', 'Sound in keywords']])
    
    def quit(self):
        self._save()
        if self.allOK():
            self.root.iconify()
            self.root.quit()

    def _save(self):
        cfg_dict = {k: v.get() for k, v in self.cfg.items()}
        with open(self.cfg_file, 'w') as f:
            json.dump(cfg_dict, f)

    def _load(self):
        if os.path.exists(self.cfg_file):
            with open(self.cfg_file, 'r') as f:
                cfg_dict = json.load(f)
            for k in cfg_dict:
                if k in self.cfg:
                    self.cfg[k].set(cfg_dict[k])

PORTS = {'gui_to_rx': 2121, 'rx_to_gui':2122, 'gui_to_tx': 2123, 'tx_to_gui': 2124}
MAX_CYCLETIME_TX_START = 3
class Gui:
    def __init__(self, Receiver = None, Transmitter = None):
        self.rig = None
        self.running = True
        self.pskr_upload = None
        self.root = None
        self.component_status = {}
        self.udp_q = queue.Queue()
        self.root = tk.Tk()
        self.root.protocol("WM_DELETE_WINDOW", lambda: self._shutdown_all())
        
        self.settings = Settings()
        if not self.settings.allOK():
            self.settings.show()
            
        self._start_components()
        self._make_layout()

        if self.running:
            self.first_decode = False
            self.pskr_upload = PSKR_upload(self.settings.get('My call'), self.settings.get('My grid'), "PyFT8m")
            self.adif_log = ADIF(f"{self.settings.get('config_folder')}/PyFT8m.adi")
            self.rig = Rig()
            self.init_qso_vars()
            self.update_waterfall()
            self.update_pskr_uploader()
            self.dial_freq_Hz = 14074000
            self.pending_start_tx_stream = None
            self.pending_ptt_on = None
            self.call_hashes = {}
            self.root.mainloop()

    def _monitor_udp(self):
        if not self.udp_q.empty():
            self._handle_udp_message(self.udp_q.get())
        if self.running:
            self.root.after(100, self._monitor_udp)

    def _start_components(self):
        self.udp_comms_rcvr = UdpComms(ports = {'listen':PORTS['rx_to_gui'], 'send':PORTS['gui_to_rx']}, rx_callback = lambda msg_dict: self.udp_q.put(msg_dict))
        self.udp_comms_txr = UdpComms(ports = {'listen':PORTS['tx_to_gui'], 'send':PORTS['gui_to_tx']}, rx_callback = lambda msg_dict: self.udp_q.put(msg_dict))
        self._monitor_udp()
        rx = Receiver(ports = {'listen':PORTS['gui_to_rx'], 'send':PORTS['rx_to_gui']}, input_keywords = self.settings.get('Sound in keywords'))
        tx = Transmitter(ports = {'listen':PORTS['gui_to_tx'], 'send':PORTS['tx_to_gui']}, output_keywords = self.settings.get('Sound out keywords'))
        self.root.after(2000, self._check_components)

    def _check_components(self):
        if not all([comp in self.component_status for comp in ['receiver','transmitter']]):
            self._shutdown_all()
        if not all([self.component_status[comp] == 'OK' for comp in self.component_status]):
            self._shutdown_all()
            
    def _shutdown_all(self):
        if self.rig:
            self.rig.ptt_off()
            self.rig.shutdown()
        if self.pskr_upload:
            self.pskr_upload.shutdown()
        if self.running:
            self.running = False
            self.udp_comms_rcvr.udp_send_dict({'mtype':'shutdown'})
            self.udp_comms_txr.udp_send_dict({'mtype':'shutdown'})
            self.udp_comms_rcvr.shutdown()
            self.udp_comms_txr.shutdown()
            time.sleep(0.5)
        self.root.destroy()
        sys.exit()

    def _make_layout(self):        
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
        self.buttons.append(tk.Button(bc, width = 10, text = 'Settings', command = self.settings.show))
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

    def _handle_udp_message(self, msg_dict):
        display_text = ''
        if msg_dict['mtype'] == 'STATUS':
            self.component_status[msg_dict['from']] = msg_dict['value']
        if msg_dict['mtype'] == 'decode':
            self.pskr_upload.add_report(self.dial_freq_Hz, msg_dict)
            their_snr, fHz, dt, msg_tuple = msg_dict['their_snr'], f"{float(msg_dict['fHz']):07.2f}", msg_dict['dt'], msg_dict['msg_tuple']
            idx = 1 * msg_tuple[0].startswith("CQ") + 2* msg_tuple[0].startswith(self.my_call) + 3 * (msg_tuple[1] == self.my_call)
            display_type = ['norm','cq','to_me','from_me', 'from_me'][idx]
            display_text = f"{their_snr:4s} {dt:5s} {fHz:7s} ~ {' '.join(msg_tuple)}"
            if msg_tuple[1] == self.their_call:
                my_reply = self.determine_reply(' '.join(msg_tuple))
                self.queue_transmit(my_reply)
                if len(msg_tuple) == 3:
                    if any([m for m in ['+','-'] if m in msg_tuple[2]]): # grid_rpt == rpt
                        self.my_snr = msg_tuple[2]
                    if not any([m for m in ['+','-','RR','73'] if m in msg_tuple[2]]): # grid_rpt == grid
                        self.their_grid = msg_tuple[2]
                if "73" in my_reply:
                    self.adif_log.log(self.my_call, self.my_grid, self.their_call, self.their_grid,
                                      self.their_snr, self.my_snr, self.dial_freq_Hz + float(fHz))
                    self.their_call = ''
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
        if self.running:
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
        if self.running:
            self.waterfall_canvas.after(250, self.update_waterfall)

    def init_qso_vars(self):
        self.their_grid = ''
        self.their_call = ''
        self.their_snr = -30
        self.my_snr = -30
        self.my_call = self.settings.get('My call')
        self.my_grid = self.settings.get('My grid')

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
        def generate_tx_audio(tx_message):
            self.udp_comms_txr.udp_send_dict({'mtype':'generate_tx_audio', 'message':tx_message})
        def start_tx_audio():
            self.udp_comms_txr.udp_send_dict({'mtype':'send_tx_audio'})        
        if message:
            generate_tx_audio(message)
            ct = (time.time() - TX_T0) % T_CYC
            delay =  0 if ct < MAX_CYCLETIME_TX_START else T_CYC - ct
            self.pending_start_tx_stream = self.root.after(int(delay * 1000), start_tx_audio)
            self.pending_ptt_on = self.root.after(int(delay * 1000), self.rig.ptt_on)
            self.root.after(int(delay * 1000 + 13000), self.rig.ptt_off)

    def stop_transmit(self):
        self.udp_comms_txr.udp_send_dict({'mtype':'stop_tx_audio'})      
        self.rig.ptt_off()
        if self.pending_ptt_on:
            try:
                self.root.after_cancel(self.pending_ptt_on)
            except:
                pass
        if self.pending_start_tx_stream:
            try:
                self.root.after_cancel(self.pending_start_tx_stream)
            except:
                pass
        print("Transmit cancelled")

if __name__ == "__main__":
    gui = Gui(Receiver = Receiver, Transmitter = Transmitter)

