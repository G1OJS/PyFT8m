import win32api,win32process
win32process.SetPriorityClass(win32api.GetCurrentProcess(), win32process.HIGH_PRIORITY_CLASS)

import numpy as np
import time, pickle, threading, pyaudio, sys, queue, os, socket, json
from PyFT8m.receiver import Receiver

finished_audio = False

class SoundcardOut:
    def __init__(self, outputcard_keywords, wav_files):
        self.wav_files = wav_files
        self.output_device_index = None
        self.pya = pyaudio.PyAudio()
        if outputcard_keywords:
            for dev_idx in range(self.pya.get_device_count()):
                name = self.pya.get_device_info_by_index(dev_idx)['name']
                match = True
                for pattern in outputcard_keywords.replace(' ','').split(','):
                    if (not pattern in name): match = False
                if(match):
                    self.output_device_index = dev_idx
                    break
            if not self.output_device_index:
                print(f"[Audio Out] No output audio device found matching {outputcard_keywords}", verbose = True)
                sys.exit(1)

    def start_wavs(self):
        threading.Thread(target = self.play_wavs, daemon = True).start()

    def play_wavs(self, sr=12000):
        global finished_audio
        import wave
        for i, w in enumerate(self.wav_files):
            print(f"Start playing wav file {w}")
            wv = wave.open(w, 'rb')
            audio_bytes = wv.readframes(sr*16)
            audio_bytes = audio_bytes[-int(sr*(15-0.6/4))*2:]

            stream = self.pya.open(format=pyaudio.paInt16, channels=1, rate = sr, output=True,
                              output_device_index = self.output_device_index)
            stream.write(audio_bytes)
            stream.stop_stream()
            stream.close()
        time.sleep(5)
        finished_audio = True

class Wsjtx_all_tailer:
    
    def __init__(self, on_decode, all_file = "C:/Users/drala/AppData/Local/WSJT-X/ALL.txt", silent = True):
        self.all_file = all_file
        self.on_decode = on_decode
        self.silent = silent
        threading.Thread(target = self.run).start()

    def run(self):
        print("WSJT-x ALL Tailer running")
        def follow():
            with open(self.all_file, "r") as f:
                f.seek(0, 2)
                while True:
                    line = f.readline()
                    if not line:
                        time.sleep(0.2)
                        continue
                    yield line.strip()
        for line in follow():
            ls = line.split()
            msg = ' '.join(ls[7:])
            self.on_decode({'decode_completed':time.time(),  'ws_msg':msg, 'ws_cycle':ls[0]})

py_q = queue.Queue()
ws_q = queue.Queue()

def on_wsjtx_decode(m):
    global ws_q
    ws_q.put(m)

def monitor_udp():
    global py_q
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('', 2121))
    
    while True:
        time.sleep(0.1)
        rx_bytes, addres = sock.recvfrom(1024)
        if rx_bytes:
            msg_dict = json.loads(rx_bytes.decode('utf-8'))
            if msg_dict['mtype'] == 'decode':
                py_q.put(msg_dict)
            else:
                print(msg_dict)

def monitor_decodes():
    while not finished_audio:
        time.sleep(0.1)
        
        while not py_q.empty():
            time.sleep(0)
            m = py_q.get()
            if m['cyclestart_string'] != py_cycle[0]:
                baseline_decode_count = baseline_counts[py_cycle[1]] if py_cycle[1] < len(baseline_counts) else 0
                py_cycle[0] = m['cyclestart_string']
                py_cycle[1] += 1
            py_times.append(float(m['t_decode']) - t_start)
            decode_count = len(py_times)
            diff = decode_count - baseline_decode_count
            txt = f"{m['decode_info']:15s} dec:{float(m['t_decode']) % 15:5.2f} {m['msg_tuple']}"
            py_info  = f"{decode_count:03d}({diff:+03d}) {py_cycle[1]:03d} {py_times[-1]:7.2f} {txt}"
            with open(output_files[0], 'a') as f:
                f.write(f"{py_info}\n")
            print(py_info)
            
        while not ws_q.empty():
            time.sleep(0)
            m = ws_q.get()
            wst = float(m['decode_completed']) - t_start
            if m['ws_cycle'] != ws_cycle[0]:
                ws_cycle[0] = m['ws_cycle']
                ws_cycle[1] += 1
            if len(py_times):
                if wst > py_times[0] - 5:
                    ws_times.append(wst)
                    decode_count = len(ws_times)
                    ws_info  = f"{decode_count:03d} {ws_cycle[1]:3d} {ws_times[-1]:7.2f} ~ {m['ws_msg']}"
                    with open(output_files[1], 'a') as f:
                        f.write(f"{ws_info}\n")
                   # print(ws_info)


def do_test(input_device_keywords, wav_range = None):
    global t_start, gui, ws_cycle, py_cycle
    global baseline_counts, py_times, ws_times
    ws_cycle = ['', 0]
    py_cycle = ['', 0]

    for fn in output_files:
        with open(fn,'w') as f:
            f.write('')
    baseline_counts = []
    
    if os.path.exists(baseline_file):
        with open(baseline_file, 'r') as f:
            lines = f.readlines()
        cycle_prev = lines[0].split()[1]
        for i, l in enumerate(lines):
            cycle = l.split()[1]
            if cycle != cycle_prev:
                cycle_prev = cycle
                baseline_counts.append(i)
        baseline_counts.append(i)
        print(f"Loaded {len(baseline_counts)} cycle decode counts from {baseline_file}")

    py_times, ws_times = [], []
    
    wav_files = []
    if wav_range:
        for idx in range(*wav_range):
            wav_files.append(f"{wav_folder}/test_{idx:02d}.wav")

    wsjtx_all_tailer = Wsjtx_all_tailer(on_wsjtx_decode, silent = False)
    rx = Receiver(mic_keywords = input_device_keywords)
    threading.Thread(target = monitor_decodes, daemon = True).start()
    threading.Thread(target = monitor_udp, daemon = True).start()
    soundout = SoundcardOut("CABLE, Input", wav_files)

    wav_file_time_offset = -1
    t = (wav_file_time_offset - time.time()) %15
    print(f"Waiting to play first wav file {t:6.2f}s")
    time.sleep(t)
    t_start = time.time()
    soundout.start_wavs()

wav_folder = "C:/Users/drala/Documents/Projects/GitHub/ft8_lib/test/wav/20m_busy"
baseline_file = 'PyFT8m_baseline.txt'
output_files = ['PyFT8m.txt', 'wsjtx.txt']

#do_test("Mic, CODEC")
do_test("CABLE, Output", [8,28])
