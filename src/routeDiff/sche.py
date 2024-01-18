import time
import schedule as sch
from main import routeDiff_execute_schedule as rd_exe

if __name__ == '__main__':
    rd_exe()
    sch.every().day.at("02:00").do(rd_exe)

    while True:
        sch.run_pending()
        time.sleep(60)