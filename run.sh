#!/bin/bash
. /root/source/python/venv/bin/activate 
python thermobeacon.py --beacons "living=6f:15:00:00:00:42,bed=6f:15:00:00:0c:b1,bath=8e:72:00:00:03:25,fridge=23:1b:00:00:04:76,outside=8e:d6:00:00:06:ca" --interval 5 --loglevel debug --logger console
