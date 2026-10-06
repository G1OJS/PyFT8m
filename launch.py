from PyFT8m.receiver import Receiver
from PyFT8m.transmitter import Transmitter
from PyFT8m.tk_gui import Gui

rx = Receiver(mic_keywords = ['Mic', 'CODEC'], max_freq = 2900)
tx = Transmitter()
gui = Gui()
