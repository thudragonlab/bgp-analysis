import time
import schedule as sch
from main import treeHash_execute_schedule as th_exe

if __name__ == '__main__':
    th_exe()
    sch.every().day.at("02:00").do(th_exe)

    while True:
        sch.run_pending()
        time.sleep(60)