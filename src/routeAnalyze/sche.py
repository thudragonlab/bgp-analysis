import time
import schedule as sch
from main import routeAnalyze_execute_schedule as ra_exe

if __name__ == '__main__':
    ra_exe()
    sch.every().day.at("02:00").do(ra_exe)

    while True:
        sch.run_pending()
        time.sleep(60)