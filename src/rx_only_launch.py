from PyFT8m import Receiver

rx = Receiver(max_freq = 2900, output_type = 'print')
rx.start(input_keywords = 'Mic, CODEC')
