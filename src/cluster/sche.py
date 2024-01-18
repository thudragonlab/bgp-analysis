import time
import schedule as sch
from cluster import main_execute as cluster_main
from datetime import datetime

if __name__ == '__main__':
    # cluster_main()
    sch.every().day.at("04:10").do(cluster_main)

    while True:
        sch.run_pending()
        time.sleep(60)