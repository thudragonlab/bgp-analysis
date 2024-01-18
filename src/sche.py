import time
import schedule as sch
from main import download, copy_exe

if __name__ == '__main__':
    sch.every().day.at("00:30").do(download)

    while True:
        sch.run_pending()
        time.sleep(60)