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

    def add_report(self, band_tuple, msg_dict):
        if band_tuple is not None:
            band, fHz_dial = band_tuple
            their_snr, freq_hz = int(msg_dict['their_snr']), fHz_dial + float(msg_dict['fHz'])
            dxcall, mode = msg_dict['msg_tuple'][1], "FT8"
            source, tt = 1, int(time.time())
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
              
    def log(self, band_tuple, qso_dict):
        gmt = time.gmtime()
        qd = qso_dict
        if band_tuple is not None:
            band, fHz_dial = band_tuple
            freq = f"{ (float(fHz_dial) + float(qd['fHz']))/1e6 : .3f}"
            log_dict = {'operator':qd['call'], 'station_callsign':qd['call'], 'my_gridsquare':qd['grid'], 'mode':'FT8',
                        'time_on': time.strftime("%H%M%S", gmt), 'qso_date':time.strftime("%Y%m%d", gmt),
                        'band':band, 'freq': freq,
                        'call':qd['their_call'], 'gridsquare': qd['their_grid'], 'rst_sent':qd['their_snr'], 'rst_rcvd':qd['my_snr']}
            with open(self.adif_log_file,'a') as f:
                for k, v in log_dict.items():
                    v = str(v)
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
        self.hamlib_port = hamlib_port
        self.sock_hamlib = socket.create_connection(('localhost', self.hamlib_port))
        self._send_tcp("M PKTUSB 0")

    def shutdown(self):
        self.sock_hamlib.close()
        print("Rig control is shut down")

    def _send_tcp(self, cmd):
        self.sock_hamlib.sendall((cmd + "\n").encode())
        return self.sock_hamlib.recvfrom(25)[0].decode()

    def ptt_on(self):
        self._send_tcp(f"T 1")

    def ptt_off(self):
        self._send_tcp(f"T 0")

    def set_band(self, band_tuple):
        band, fHz = band_tuple
        self._send_tcp(f"F {fHz}")

    def get_fHz(self):
        fHz = int(self._send_tcp("f"))
        return(fHz)

class Settings:
    def __init__(self):
        self.root = tk.Tk()
        #self.root.protocol("WM_DELETE_WINDOW", self.quit)
        self.root.iconify()

    def show(self):          
        self.root.deiconify()
        self.root.mainloop()

PORTS = {'gui_to_rx': 2121, 'rx_to_gui':2122, 'gui_to_tx': 2123, 'tx_to_gui': 2124}
MAX_CYCLETIME_TX_START = 3
class Gui:
    def __init__(self, Receiver = None, Transmitter = None, config = None, band_tuples = None):
        self.config = config
        self.band_tuples = band_tuples
        self.band_tuple = None
        self.running = True
        self.root = tk.Tk()
        self.root.protocol("WM_DELETE_WINDOW", lambda: self._shutdown_all())
        self.root.title("PyFT8m by G1OJS")
        self._start_components()

        self.pskr_upload = None
        if self.config['pskreporter']['upload'] == 'Y':
            self.pskr_upload = PSKR_upload(self.config['station']['call'], self.config['station']['grid'], "PyFT8m")

        self.rig = None
        if self.config.has_section('hamlib_rig'):
            self._ensure_hamlib_running()
            self.rig = Rig()
            fHz = self.rig.get_fHz()
            band_tuples = [bt for bt in self.band_tuples if (bt[1] - fHz) < 10000]
            if len(band_tuples) == 1:
                self.band_tuple = band_tuples[0]

        self.adif = None
        if self.config.has_section('logging'):
            self.adif_log = ADIF(self.config['logging']['logfile'])

        self.settings = Settings()
        self._make_layout()
        self.update_clock()
        self.update_waterfall()
        self.update_pskr_uploader()
        self.qso_dict = {'cyclestart_string': '', 'fHz':'0', 'dt':'0', 'decode_info':'', 'msg_tuple':('','',''),
                         'call':self.config['station']['call'], 'grid':self.config['station']['grid'], 'my_snr':'-30',  
                         'their_call':'', 'their_grid':'', 'their_snr':'-30'}
        self.pending_start_tx_stream = None
        self.pending_ptt_on = None
        self.last_rx_time = 0
        self.root.mainloop()

    def _ensure_hamlib_running(self):
        if not any(['rigctld' in i.name() for i in psutil.process_iter()]):
            hamlib_cfg_keys = ['rigctld', 'model', 'port', 'baud_rate']
            vals = [self.config['hamlib_rig'][key] for key in hamlib_cfg_keys]
            cmd = f"{vals[0]} -m {vals[1]} -r {vals[2]} -s {vals[3]}"
            threading.Thread(target = subprocess.run, args = (cmd,)).start() 
        
    def _monitor_udp(self):
        if not self.udp_q.empty():
            self._handle_udp_message(self.udp_q.get())
        if self.running:
            self.root.after(100, self._monitor_udp)

    def _start_components(self):
        self.component_status = {}
        self.udp_q = queue.Queue()
        self.udp_comms_rcvr = UdpComms(ports = {'listen':PORTS['rx_to_gui'], 'send':PORTS['gui_to_rx']}, rx_callback = lambda msg_dict: self.udp_q.put(msg_dict))
        self.udp_comms_txr = UdpComms(ports = {'listen':PORTS['tx_to_gui'], 'send':PORTS['gui_to_tx']}, rx_callback = lambda msg_dict: self.udp_q.put(msg_dict))
        self._monitor_udp()
        rx = Receiver(ports = {'listen':PORTS['gui_to_rx'], 'send':PORTS['rx_to_gui']}, input_keywords = self.config['soundcard']['receive'])
        tx = Transmitter(ports = {'listen':PORTS['gui_to_tx'], 'send':PORTS['tx_to_gui']}, output_keywords = self.config['soundcard']['transmit'])
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

        self.clock_frame = tk.Frame(self.sidebar_container, width = 10)
        self.clock_frame.pack(side = 'top')
        self.clock_lbl = tk.Label(self.clock_frame, font=('calibri', 14, 'bold'), bg='purple', fg='white')
        self.clock_lbl.pack(anchor='center')

        self.buttons = []
        bc = self.sidebar_container
        #self.buttons.append(tk.Button(bc, width = 10, text = 'Settings', command = self.settings.show))
        self.buttons.append(tk.Button(bc, width = 10, text = 'CQ', command = self.call_cq))
        self.buttons.append(tk.Button(bc, width = 10, text = 'Repeat last', command = self.queue_transmit))
        self.buttons.append(tk.Button(bc, width = 10, text = 'STOP TX', command = self.stop_transmit))
        for btn in self.buttons:
            btn.pack(side = 'top', anchor = 'n', pady = 1)

        for band_tuple in self.band_tuples:
            band, _ = band_tuple
            btn = tk.Button(bc, width = 10, text = band, command = lambda band_tuple = band_tuple: self.set_band(band_tuple))
            btn.pack(side = 'top', anchor = 'n', pady = 1)
            self.buttons.append(btn)
            
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
        if msg_dict['mtype'] == 'STATUS':
            self.component_status[msg_dict['from']] = msg_dict['value']
        if msg_dict['mtype'] == 'decode':
            self.pskr_upload.add_report(self.band_tuple, msg_dict)
            self.process_decode(msg_dict)
        elif msg_dict['mtype'] == 'waterfall':
            self.waterfall_new_vals = [int(v) for v in msg_dict['data'].split(',')]

    def process_decode(self, msg_dict):
        new_cycle = msg_dict['cyclestart_string'] != self.qso_dict['cyclestart_string']
        their_snr, dt, fHz, msg_tuple = msg_dict['their_snr'], msg_dict['dt'], msg_dict['fHz'], msg_dict['msg_tuple']
        display_text = f" {their_snr}  {dt}  {fHz}  ~  {' '.join(msg_tuple)}"
        idx = 1 * msg_tuple[0].startswith("CQ") + 2* msg_tuple[0].startswith(self.qso_dict['call']) + 3 * (msg_tuple[1] == self.qso_dict['call'])
        display_type = ['norm','cq','to_me','from_me', 'from_me'][idx]
        tnow = time.time()
        if new_cycle or tnow > self.last_rx_time + 15:
            self.text_widget.delete(1.0, tk.END)
            self.qso_dict.update({'cyclestart_string': msg_dict['cyclestart_string']})
        self.last_rx_time = tnow
        self.text_widget.insert(tk.END, f"{display_text}\n", display_type)
        self.text_widget.see('end')
        if msg_tuple[0] == self.qso_dict['call']:
            self.progress_qso(msg_tuple)

    def row_click(self, e):
        curr = e.widget.index("current").split('.')[0]
        row_txt = e.widget.get(f"{curr}.0", f"{curr}.end")
        if "~" in row_txt:
            rx_msg = row_txt.split('~')[1][1:]
            msg_tuple = rx_msg.split(' ')
            self.qso_dict.update({'their_call':msg_tuple[1], 'their_snr': row_txt[:3]})
            self.progress_qso(msg_tuple)
                    
    def progress_qso(self, msg_tuple):
        if len(msg_tuple) == 3:
            hail, their_call, grid_rpt = msg_tuple
            reply = ''
            tcmc = f"{self.qso_dict['their_call']} {self.qso_dict['call']}"
            if hail.startswith("CQ"):
                reply = f"{tcmc} {self.qso_dict['grid'][:4]}"
            if hail.startswith(self.qso_dict['call']):
                reply = f"{tcmc} {self.qso_dict['their_snr']}"
                if any([m for m in ['+','-'] if m in grid_rpt]):
                    reply = f"{tcmc} R{self.qso_dict['their_snr']}"
                if any([m for m in ['R+','R-','RRR'] if m in grid_rpt]):
                    reply = f"{tcmc} RR73"
                if grid_rpt == 'RR73':
                    reply = f"{tcmc} 73"
            self.queue_transmit(reply)
            if any([m for m in ['+','-'] if m in grid_rpt]): # grid_rpt == rpt
                self.qso_dict['my_snr'] = grid_rpt
            if not any([m for m in ['+','-','RR','73'] if m in grid_rpt]): # grid_rpt == grid
                self.qso_dict['their_grid'] = grid_rpt
            if "73" in reply:
                self.adif_log.log(self.band_tuple, self.qso_dict)

    def set_band(self, band_tuple):
        self.band_tuple = band_tuple
        self.rig.set_band(band_tuple)
        self.text_widget.delete(1.0, tk.END)

    def update_clock(self):
        self.clock_lbl.config(text = time.strftime('%H:%M:%S'))
        self.clock_lbl.after(1000, self.update_clock)

    def update_pskr_uploader(self):
        if self.pskr_upload and self.running:
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
        if self.running:
            self.waterfall_canvas.after(250, self.update_waterfall)

    def call_cq(self):
        self.queue_transmit(f"CQ {self.config['station']['call']} {self.config['station']['grid']}")

    def queue_transmit(self, message = None):
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
    import configparser

    def get_config(config_folder):
        config = configparser.ConfigParser()
        ini_file = f"{config_folder}/PyFT8m.ini"
        if not os.path.exists(ini_file):
            resp = input(f"No config file found at {ini_file}\nWould you like to create one (Y/N)? ")
            if resp.upper() !="Y":
                print("Exiting program")
                sys.exit()
            station_callsign = input(f"Please enter your callsign: ")
            station_grid = ''
            while len(station_grid) < 4:
                station_grid = input(f"Please enter your Maidenhead locator (at least 4 characters, you can edit this later): ")
            config['station'] = {'call':station_callsign, 'grid':station_grid}
            config['bands'] = {'20m':14.074}
            config['gui'] = {'loc':'km_deg', 'wb':'Y'}
            config['hamlib_rig'] = {'rigctld':'C:/WSJT/wsjtx/bin/rigctld-wsjtx', 'port': 'COM4', 'baud_rate':9600, 'model':3070}
            config['soundcard'] = {'transmit':'Speak, CODEC', 'receive':'Mic, CODEC'}
            config['pskreporter'] = {'upload':'Y'}
            config['logging'] = {'logfile': f"{config_folder}/PyFT8m.adi"}
            with open(ini_file, 'w') as f:
                config.write(f)
            print(f"Wrote default config to {ini_file}. Please open and edit to add bands, frequencies and preferences and re-launch PyFT8m.")
            sys.exit()
        print(f"Reading config from {ini_file}")
        config.read(ini_file)
        band_tuples = []
        if config.has_section('bands'):
            for band in config['bands']:
                fHz = 1000000 * float(config['bands'][band])
                band_tuples.append((band, fHz))
        return config, band_tuples

    config_folder = os.path.expanduser("~").replace('\\', '/')
    config, band_tuples = get_config(config_folder)
    gui = Gui(Receiver = Receiver, Transmitter = Transmitter, config = config, band_tuples = band_tuples)

