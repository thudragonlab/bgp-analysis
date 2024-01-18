import os
from getRawData.main import grd_main
from treeHash.main import treeHash_execute as th_exe
from routeAnalyze.main import routeAnalyze_execute as ra_exe
from routeDiff.main import routeDiff_execute as rd_exe
from routeMoniter.main import routeMoniter_execute as rm_exe
from datetime import datetime
import multiprocessing

def download():
    SAVE_DIR = grd_main()

def main():
    SAVE_DIR = grd_main()
    # SAVE_DIR = f'/home/taoyx/donly-bgp-routing/bgpwatch/srcData/20230815'
    current_date = datetime.now().strftime("%Y_%m_%d")
    # th_exe(SAVE_DIR, current_date)
    #ra_exe(SAVE_DIR, current_date)
    #rd_exe(SAVE_DIR, current_date)
    #rm_exe(SAVE_DIR, current_date)
    pool = multiprocessing.Pool(4)
    pool.apply_async(th_exe, (SAVE_DIR, current_date, ))
    pool.apply_async(ra_exe, (SAVE_DIR, current_date, ))
    pool.apply_async(rd_exe, (SAVE_DIR, current_date, ))
    pool.apply_async(rm_exe, (SAVE_DIR, current_date, ))
    pool.close()
    pool.join()


if __name__ == '__main__':
    download()