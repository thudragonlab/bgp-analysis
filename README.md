# About bgp analysis

bgp analysis collects daily RIB files from all RIPE RIS, RouteViews and cgtf collectors and performs various approaches to get different types of statistics.

## Data Storing

Source data and difference data are stored in {rootdir}/bgpwatch/srcData and {rootdir}/bgpwatch/diffData respectively. Statistics are stored in mongodb. The settings of mongodb should be put in {rootdir}/configs/rconfig.py. The logs of each module will be stored in {rootdir}/logs.

## Functions of each modules(with requirements)

**cluster:** Code stored in {rootdir}/src/cluster. Performs node clustering, peer counting, renamning collections and other works after all other modules have finished their works.(Requires pymongo,IPy in Python). A scheduled task is set in {rootdir}/src/cluster/sche.py, please run it with supervisorctl.

**getRawData:** Code stored in {rootdir}/src/getRawData. Downloads RIB files from all RIPE RIS, RouteViews and cgtf collectors and then dump them to txt files.(Requires pymongo in Python, bgpdump tool in Linux). A scheduled task is set in {rootdir}/src/sche.py, please run it with supervisorctl.

**routeAnalyze:** Code stored in {rootdir}/src/routeAnalyze. Traverses all dumped files, collects origin_prefix information of ASes (including number of ipv4 prefix, number of ipv6 prefix, number of /24 ipv4 sub-prefix and number of /48 ipv6 sub-prefix) and upload them to mongodb.(Requires pymongo,IPy in Python). A scheduled task is set in {rootdir}/src/routeAnalyze/sche.py, please run it with supervisorctl.

**routeDiff:** Code stored in {rootdir}/src/routeDiff. Traverses all dumped files, collects the difference information of ASes, prefixes from the day before(For example, if a prefix is announced yesterday but not today in the RIB files, it will be logged in this module.) and upload them to mongodb.(Requires pymongo,IPy in Python). A scheduled task is set in {rootdir}/src/routeDiff/sche.py, please run it with supervisorctl.

**routeMoniter:** Code stored in {rootdir}/src/routeMoniter. Traverses all dumped files, collects prefixes of ASes and prefixes import/export information of different ASes. Then upload them to mongodb.(Requires pymongo,IPy in Python). A scheduled task is set in {rootdir}/src/routeMoniter/sche.py, please run it with supervisorctl.

**treeHash:** Code stored in {rootdir}/src/treeHash. Traverses all dumped files, collects AS paths and form route trees. Collects Hashes of route trees and map prefixes to these Hashes. Then upload them to mongodb.(Requires pymongo,IPy in Python). A scheduled task is set in {rootdir}/src/treeHash/sche.py, please run it with supervisorctl.

## usage

Create supervisorctl service for each sche.py mentioned above, then the scheduled task will run once per day. By default, **getRawData** will be processed on 00:30 UTC, **routeAnalyze/routeDiff/routeMoniter/treeHash** will be processed on 02:00 UTC, **cluster** will be processed on 04:10 UTC. Note that upon starting the services, **routeAnalyze/routeDiff/routeMoniter/treeHash** will immediately run for once, so please make sure you have the processed raw data. An example of .conf file of supervisorctl is below.

```
[program:routeAnalyze]
stopasgroup=true
user=<your user name>
environment=PYTHONPATH="$PYTHONPATH:/usr/local/lib" (replace with your python path)
directory=/home/<your user name>
command=python3 -u /home/<your user name>/<rootdir>/src/routeAnalyze/sche.py
redirect_stderr=false
stdout_logfile=/home/<your user name>/<rootdir>/logs/routeAnalyze-access.log
stderr_logfile=/home/<your user name>/<rootdir>/logs/routeAnalyze-error.log
stdout_logfile_maxbytes=20MB
stderr_logfile_maxbytes=20MB
stdout_logfile_backups=15
stderr_logfile_backups=15
```

If you want to run some of the modules, please make sure the task of **getRawData** is complete before running **routeAnalyze/routeDiff/routeMoniter/treeHash**. Then you can run the main.py in each directory and run **cluster** after. Also note that you may need to modify the task of cluster to make sure no exception will be thrown.

## TIPS

The source data of these tasks if huge, please remember to clear it periodically.
