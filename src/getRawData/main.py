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

import logging
import json
from datetime import datetime
from logging.handlers import RotatingFileHandler
from cleaner import Cleaner
from downloader import Downloader
from dump import Dumper
import multiprocessing

handler = RotatingFileHandler(os.path.join(parparpar_path, 'logs/getRawData-running.log'), encoding='UTF-8', maxBytes=5*1024*1024, backupCount=10)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s")
handler.setFormatter(formatter)
mlogger = logging.getLogger(f'getRawData')
mlogger.setLevel(logging.INFO)
mlogger.addHandler(handler)

RIB_URL_LIST = [
    #("https://bgp.cgtf.net/ribs/{year}/{month}/rib.{year}{month}{day}.{hour}{minute}.mrt.bz2", "cgtf"),
    #("https://bgp.cgtf.net/collector2/ribs/{year}/{month}/rib.{year}{month}{day}.{hour}{minute}.mrt.bz2", "cgtf2"),
    #("https://bgp.cgtf.net/collector1/ribs/{year}/{month}/rib.{year}{month}{day}.{hour}{minute}.mrt.bz2", "cgtf1"),
    ("http://archive.routeviews.org/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "_"),
    ("http://archive.routeviews.org/route-views3/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "3"),
    ("http://archive.routeviews.org/route-views4/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "4"),
    ("http://archive.routeviews.org/route-views5/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "5"),
    ("http://archive.routeviews.org/route-views6/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "6"),
    ("http://archive.routeviews.org/route-views.amsix/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "amsix"),
    ("http://archive.routeviews.org/route-views.chicago/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "chicago"),
    ("http://archive.routeviews.org/route-views.chile/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "chile"),
    ("http://archive.routeviews.org/route-views.eqix/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "eqix"),
    ("http://archive.routeviews.org/route-views.flix/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "flix"),
    ("http://archive.routeviews.org/route-views.gorex/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "gorex"),
    ("http://archive.routeviews.org/route-views.kixp/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "kixp"),
    # ("http://archive.routeviews.org/route-views.isc/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "isc"),
    ("http://archive.routeviews.org/route-views.napafrica/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "napafrica"),
    ("http://archive.routeviews.org/route-views.nwax/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "nwax"),
    ("http://archive.routeviews.org/route-views.phoix/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "phoix"),
    ("http://archive.routeviews.org/route-views.telxatl/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "telxatl"),
    ("http://archive.routeviews.org/route-views.wide/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "wide"),
    ("http://archive.routeviews.org/route-views.sydney/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "sydney"),
    # ("http://archive.routeviews.org/route-views.saopaulo/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "saopaulo"),
    ("http://archive.routeviews.org/route-views2.saopaulo/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "saopaulo2"),
    ("http://archive.routeviews.org/route-views.sg/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "sg"),
    ("http://archive.routeviews.org/route-views.perth/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "perth"),
    ("http://archive.routeviews.org/route-views.peru/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "peru"),
    ("http://archive.routeviews.org/route-views.sfmix/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "sfmix"),
    # ("http://archive.routeviews.org/route-views.siex/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "siex"),
    ("http://archive.routeviews.org/route-views.soxrs/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "soxrs"),
    ("http://archive.routeviews.org/route-views.mwix/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "mwix"),
    ("http://archive.routeviews.org/route-views.rio/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "rio"),
    ("http://archive.routeviews.org/route-views.fortaleza/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "fortaleza"),
    ("http://archive.routeviews.org/route-views.gixa/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "gixa"),
    ("http://archive.routeviews.org/route-views.bdix/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "bdix"),
    ("http://archive.routeviews.org/route-views.bknix/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "bknix"),
    ("http://archive.routeviews.org/route-views.uaeix/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "uaeix"),
    ("http://archive.routeviews.org/route-views.ny/bgpdata/{year}.{month}/RIBS/rib.{year}{month}{day}.{hour}{minute}.bz2", "ny"),
    ("https://data.ris.ripe.net/rrc00/{year}.{month}/bview.{year}{month}{day}.{hour}{minute}.gz", "rrc00"),
    ("https://data.ris.ripe.net/rrc01/{year}.{month}/bview.{year}{month}{day}.{hour}{minute}.gz", "rrc01"),
    ("https://data.ris.ripe.net/rrc03/{year}.{month}/bview.{year}{month}{day}.{hour}{minute}.gz", "rrc03"),
    ("https://data.ris.ripe.net/rrc04/{year}.{month}/bview.{year}{month}{day}.{hour}{minute}.gz", "rrc04"),
    ("https://data.ris.ripe.net/rrc05/{year}.{month}/bview.{year}{month}{day}.{hour}{minute}.gz", "rrc05"),
    ("https://data.ris.ripe.net/rrc06/{year}.{month}/bview.{year}{month}{day}.{hour}{minute}.gz", "rrc06"),
    ("https://data.ris.ripe.net/rrc07/{year}.{month}/bview.{year}{month}{day}.{hour}{minute}.gz", "rrc07"),
    ("https://data.ris.ripe.net/rrc10/{year}.{month}/bview.{year}{month}{day}.{hour}{minute}.gz", "rrc10"),
    ("https://data.ris.ripe.net/rrc11/{year}.{month}/bview.{year}{month}{day}.{hour}{minute}.gz", "rrc11"),
    ("https://data.ris.ripe.net/rrc12/{year}.{month}/bview.{year}{month}{day}.{hour}{minute}.gz", "rrc12"),
    ("https://data.ris.ripe.net/rrc13/{year}.{month}/bview.{year}{month}{day}.{hour}{minute}.gz", "rrc13"),
    ("https://data.ris.ripe.net/rrc14/{year}.{month}/bview.{year}{month}{day}.{hour}{minute}.gz", "rrc14"),
    ("https://data.ris.ripe.net/rrc15/{year}.{month}/bview.{year}{month}{day}.{hour}{minute}.gz", "rrc15"),
    ("https://data.ris.ripe.net/rrc16/{year}.{month}/bview.{year}{month}{day}.{hour}{minute}.gz", "rrc16"),
    ("https://data.ris.ripe.net/rrc18/{year}.{month}/bview.{year}{month}{day}.{hour}{minute}.gz", "rrc18"),
    ("https://data.ris.ripe.net/rrc19/{year}.{month}/bview.{year}{month}{day}.{hour}{minute}.gz", "rrc19"),
    ("https://data.ris.ripe.net/rrc20/{year}.{month}/bview.{year}{month}{day}.{hour}{minute}.gz", "rrc20"),
    ("https://data.ris.ripe.net/rrc21/{year}.{month}/bview.{year}{month}{day}.{hour}{minute}.gz", "rrc21"),
    ("https://data.ris.ripe.net/rrc22/{year}.{month}/bview.{year}{month}{day}.{hour}{minute}.gz", "rrc22"),
    ("https://data.ris.ripe.net/rrc23/{year}.{month}/bview.{year}{month}{day}.{hour}{minute}.gz", "rrc23"),
    ("https://data.ris.ripe.net/rrc24/{year}.{month}/bview.{year}{month}{day}.{hour}{minute}.gz", "rrc24"),
    ("https://data.ris.ripe.net/rrc25/{year}.{month}/bview.{year}{month}{day}.{hour}{minute}.gz", "rrc25"),
    ("https://data.ris.ripe.net/rrc26/{year}.{month}/bview.{year}{month}{day}.{hour}{minute}.gz", "rrc26")
    ]

SAVE_DIR = os.path.join(parparpar_path, 'bgpwatch/srcData/')
POOL_SIZE = 16


# Set up objects
rib_downloader = Downloader()
download_dir_cleaner = Cleaner()
txt_dumper = Dumper()

# Set up status tracker
status = {"last_update_time": ""}


def download_rib_and_dump(url, path, vp_code):
    filePath = rib_downloader.download(url, path, vp_code)
    txt_dumper.dump(filePath)
    return


def getRawData():
    global SAVE_DIR
    current_datetime = datetime.now()
    day = current_datetime.strftime("%d")
    month = current_datetime.strftime("%m")
    year = current_datetime.strftime("%Y")
    SAVE_DIR = os.path.join(parparpar_path, f'bgpwatch/srcData/{year}{month}{day}')
    # create SAVE_DIR if not exist
    if not os.path.exists(SAVE_DIR):
        mlogger.info(f'Creating directory: {SAVE_DIR}')
        os.makedirs(SAVE_DIR)
    # create processing pool and download
    pool = multiprocessing.Pool(POOL_SIZE)
    download_dir_cleaner.clean_all(SAVE_DIR)
    mlogger.info("Start getting rawData")
    for url_with_placeholder, vp_code in RIB_URL_LIST:
        url = url_with_placeholder.replace("{day}", day).replace("{month}", month).replace("{year}", year).replace("{hour}", "00").replace("{minute}", "00")
        pool.apply_async(download_rib_and_dump, (url, SAVE_DIR, vp_code,))
    pool.close()
    pool.join()
    for file in os.listdir(SAVE_DIR):
        if file.startswith('cgtf'):
            txt_dumper.dump(os.path.join(SAVE_DIR, file))
    status['last_update_time'] = current_datetime.strftime("%Y%m%d%H%M")
    with open(SAVE_DIR + '/' + 'status.json', "w") as f:
        json.dump(status, f)
    return SAVE_DIR

# for testing
def grd_main():
    return getRawData()