import re
import csv
import itertools
import sys
import numpy as np
import datetime as datetime
from pyspark import SparkContext

# define helper function
def tocsv(data):
    return ','.join(str(d) for d in data)

# recycle functions from bdm class - lab 7
def createIndex(shapefile):
    '''
    This function takes in a shapefile path, and return:
    (1) index: an R-Tree based on the geometry data in the file
    (2) zones: the original data of the shapefile
    
    Note that the ID used in the R-tree 'index' is the same as
    the order of the object in zones.
    '''
    import rtree
    import fiona.crs
    import geopandas as gpd
    zones = gpd.read_file(shapefile).to_crs(fiona.crs.from_epsg(2263))
    index = rtree.Rtree()
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

def findZone(p, index, zones):
    '''
    findZone returned the ID of the shape (stored in 'zones' with
    'index') that contains the given point 'p'. If there's no match,
    None will be returned.
    '''
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if zones.geometry[idx].contains(p):
            return idx
    return None

def extractFull(pid, records):
    import pyproj
    import shapely.geometry as geom
    proj = pyproj.Proj(init="epsg:5570", preserve_units=True)
    index, zones = createIndex("/tmp/bdm/500cities_tracts.geojson")
    pattern = re.compile('^[a-zA-Z]+')
    drugwords = {word for word in drugwords_bc.value if " " not in word} # individual words
    drugphrases = {phrase for phrase in drugwords_bc.value if " " in phrase} # individual phrases
    
    counts = {}
    for record in records:
        flag = 0
        row = record.strip().split("|")
        tweetwords = set(row[-1].split(" "))
        tweet = row[-2].lower()
        if len(tweetwords & drugwords) > 0: # does any of the individual words in the 
            flag = 1
        else:  
            words = pattern.findall(tweet)
            length = len(words)
            if length > 1:
                phrases = set()
                for i in range(2, min(9, length + 1)): # make the set of phrases
                    for j in range(len(words) - i + 1):
                        phrases.add(" ".join(words[j:j + i]))
                if len(phrases & drug_pha) > 0: # does the phrase exist
                    flag = 1
        if flag == 1:
            p = geom.Point(proj(float(row[2]), float(row[1])))
            try:
                zone_id, zone_pop = findZone(p, index, zones)
            except:
                continue
            if zone_id and zone_pop > 0:
                counts[zone_id] = counts.get(zone_id, 0.0) + 1.0 / zone_pop
    return counts.items()

    
if __name__=='__main__':
    start = datetime.datetime.now()
    fn = sys.argv[1]
    sc = SparkContext()
    
    drugwords1 = [line.strip() for line in open('/tmp/bdm/drug_illegal.txt')]
    drugwords2 = [line.strip() for line in open('/tmp/bdm/drug_sched2.txt')]
    drugwords = [drugwords1, drugwords2]
    drugwords_bc = sc.broadcast(set().union(*drugwords))

    sc.textFile(fn)\
        .mapPartitionsWithIndex(extractFull)\
        .map(tocsv)\
        .saveAsTextFile(sys.argv[2])

    end = datetime.datetime.now()
    print(end - start)
