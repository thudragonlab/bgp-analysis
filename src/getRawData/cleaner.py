import logging
import os
import shutil

class Cleaner():
        def __init__(self):
            self.mlogger = logging.getLogger(f'getRawData')

        def clean_all(self, dir):
            self.mlogger.info(f'Cleaning all of {dir}')
            try:
                shutil.rmtree(dir)
            except OSError as e:
                self.mlogger.warning(f'Error: {e.filename} - {e.strerror}')
            os.makedirs(dir, exist_ok=True)
