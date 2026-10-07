from PyFT8m.receiver import Receiver
from PyFT8m.transmitter import Transmitter
from PyFT8m.tk_gui import Gui

rx = Receiver(mic_keywords = 'Mic, CODEC', max_freq = 2900, latest_decode = 2, sock_rcvr_out = 2121)
tx = Transmitter(outputcard_keywords = 'Speak, CODEC', max_tx_cycletime_start = 3, sock_gui_cmd = 2122)
tx.init_hamlib(com_rig = 'COM4', com_baud = 9600, rigctld = 'C:/WSJT/wsjtx/bin/rigctld-wsjtx', rig_code = 3070, hamlib_port = 4532)
gui = Gui(sock_gui_cmd = 2122, sock_rcvr_out = 2121)
