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
    import re
    import pyproj
    import shapely.geometry as geom
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
    index, zones = createIndex("500cities_tracts.geojson")
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
                if len(phrases & drugphrases) > 0: # does the phrase exist
                    flag = 1
        if flag == 1:
            tweetpoint = geom.Point(proj(float(row[2]), float(row[1])))
            try:
                censustract, censustractpop = findZone(tweetpoint, index, zones) 
            except:
                continue
            if censustract and censustractpop > 0:
                counts[censustract] = counts.get(censustractpop, 0.0) + 1.0 / censustractpop
    return counts.items()

    
if __name__=='__main__':
    start = datetime.datetime.now()
    fn = sys.argv[1]
    sc = SparkContext()
    
    drugwords1 = sc.textFile('/tmp/bdm/drug_illegal.txt').collect()
    drugwords2 = sc.textFile('/tmp/bdm/drug_sched2.txt').collect()
    drugwords = [drugwords1, drugwords2]
    drugwords_bc = sc.broadcast(set().union(*drugwords))

    sc.textFile(fn)\
        .mapPartitionsWithIndex(extractFull)\
        .saveAsTextFile(sys.argv[2])

    end = datetime.datetime.now()
    print(end - start)
