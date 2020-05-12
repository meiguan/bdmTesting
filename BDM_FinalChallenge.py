import os
import subprocess

cmd = 'hdfs dfs -ls hdfs:///tmp/bdm/nyc_parking_violations/'
files = subprocess.check_output(cmd, shell=True).strip().split('\n')
print(files)