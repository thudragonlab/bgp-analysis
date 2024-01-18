import time
import schedule as sch
from main import routeMoniter_execute_schedule as rm_exe

if __name__ == '__main__':
    rm_exe()
    sch.every().day.at("02:00").do(rm_exe)

    while True:
        sch.run_pending()
        time.sleep(60)