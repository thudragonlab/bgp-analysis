import os
abs_path = os.path.abspath(__file__)
par_path = os.path.split(abs_path)[0]
parpar_path = os.path.split(par_path)[0]
parparpar_path = os.path.split(parpar_path)[0]
par_path += '/'
parpar_path += '/'
parparpar_path += '/'
import sys
sys.path.append(par_path)

import os
import logging
from datetime import datetime
import time
import requests
from tqdm import tqdm


class Downloader():
	
	def __init__(self):
		self.mlogger = logging.getLogger(f'getRawData')

	def _get_fileName(self, url):
		# url format : 
		# RIPE RIS : f'https://data.ris.ripe.net/rrc{suffix}/{year}.{month}/bview.{year}{month}{day}.0000.gz'
		# CGTF : f'https://bgp.cgtf.net/ribs/{year}/{month}/rib.{year}{month}{day}.mrt.bz2'
		# RouteViews : f'http://archive.routeviews.org/{mid_string}/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.0000.bz2'
		# the specific year,month,etc. information in urls in this function has already been given
		fileName = url.split('/')[-1]
		return fileName

	def download(self, url, path, vp_code, loop_times=0):
		# path : save path, vp_code : vantage point code, like rrc00, routeviews1, etc.
		self.mlogger.info(f'[DOWNLOAD] {url} save to {path}, loop times -> {loop_times}')
		if loop_times > 25:
			self.loop_times_exceed_handler(url, loop_times)
			return False
		try:
			fileName = f'{vp_code}_{self._get_fileName(url)}'
			filePath = os.path.join(path, fileName)
			file_content = requests.get(url, stream=True, verify=False,timeout=10)
			data_count = int(file_content.headers.get('content-length', 0))
			local_file_size = 0
			if os.path.exists(filePath):
				local_file_size = os.path.getsize(filePath)
			if data_count != local_file_size:
				print(f'Download new RIB file => {fileName}')
				with open(filePath, 'wb') as file, tqdm(
						desc=fileName,
						total=data_count,
						unit='iB',
						unit_scale=True,
						unit_divisor=1024,
				) as bar:
					for data in file_content.iter_content(chunk_size=1 * 1024 * 1024):
						size = file.write(data)
						bar.update(size)
			file_content.close()
			if os.path.exists(filePath):
				local_file_size = os.path.getsize(filePath)
				if data_count != local_file_size:
					self.mlogger.warning(f'[DOWNLOAD] file {fileName} header_data_count != local_file_size')
					time.sleep(1.5)
					self.download(url, path, vp_code, loop_times + 1)
			else:
				self.mlogger.warning(f'[DOWNLOAD] file {fileName} filePath doesn\'t exists, retrying...')
				time.sleep(1.5)
				self.download(url, path, vp_code, loop_times + 1)
		except Exception as e:
			self.exception_when_downloading(e, url, loop_times)
			time.sleep(3)
			self.download(url, path, vp_code, loop_times + 1)

		return filePath

	def loop_times_exceed_handler(self, url, loop_times):
		with open(os.path.join(parparpar_path, 'logs/download_fail.log'),'a+') as dfl:
			dfl.write(f'[{datetime.utcfromtimestamp(float(datetime.now().timestamp())).strftime("%Y-%m-%d %H:%M:%S")}] Donwload {url} failed retry {loop_times} times\n')

	def exception_when_downloading(self, e, url, loop_times, max_limit = 3):
		with open(os.path.join(parparpar_path, 'logs/error_list.log'), 'a+') as error_r:
			error_r.write(f'\n------------------------------------------\n')
			if loop_times <= max_limit:
				error_r.write(f'Donwload {url} failed, retrying {loop_times + 1} times\n')
			else:
				error_r.write(f'URL: Donwload {url} failed\n')
			error_r.write(f'Exception Date : {datetime.now()}\n')
			error_r.write(f'Exception Name : {e}\n')
			error_r.write(f'Exception Args : {e.args}\n')
			error_r.write(f'\n------------------------------------------\n')
			error_r.flush()

if __name__ == '__main__':
	d = Downloader(['https://archive.routeviews.org/bgpdata/2023.07/RIBS/rib.20230717.0000.bz2'], './', 'test')
	d.download('https://archive.routeviews.org/bgpdata/2023.07/RIBS/rib.20230717.0000.bz2', './', 'routeviews')
