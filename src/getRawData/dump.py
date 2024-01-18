import logging
import os

class Dumper():
	
	def __init__(self):
		self.mlogger = logging.getLogger(f'getRawData')

	def dump(self, filePath):
		dst_file_path = filePath + '.txt'
		self.mlogger.info(f'Dumping file: {dst_file_path}')
		command = f'bgpdump -m {filePath}'
		bgpdump_output = os.popen(command)
		f = open(dst_file_path, 'w')
		while True:
			line = bgpdump_output.readline()
			if not line:
				break
			f.write(line)
		f.close()
		self.mlogger.info(f'file {dst_file_path} dump finished')
        
if __name__ == '__main__':
    d = Dumper()