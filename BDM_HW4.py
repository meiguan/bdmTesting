import sys
from pyspark import SparkContext
from heapq import nlargest
import csv

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

def processTrips(pid, records):
    '''
    Our aggregation function that iterates through records in each
    partition, checking whether we could find a zone that contain
    the pickup location.
    '''
    import csv
    import pyproj
    import shapely.geometry as geom
    
    # Create an R-tree index
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)    
    index, zones = createIndex('neighborhoods.geojson')    
    
    # Skip the header
    if pid==0:
        next(records)
    reader = csv.reader(records)
    counts = {}
    
    for row in reader:
        try:
            pickup = geom.Point(proj(float(row[5]), float(row[6])))
            dropoff = geom.Point(proj(float(row[9]), float(row[10])))
        except:
            continue
        b = findZone(pickup, index, zones)
        n = findZone(dropoff, index, zones)
        if (b is not None) and (n is not None):
            yield ((zones['borough'][b],zones['neighborhood'][n]), 1)
    return counts.items()

def tocsv(data):
    return ','.join(str(d) for d in data)

if __name__=='__main__':
    fn = '/tmp/bdm/yellow_tripdata_2011-05.csv' if len(sys.argv)<2 else sys.argv[1]
    sc = SparkContext()
    rdd = sc.textFile(fn)
    rdd.mapPartitionsWithIndex(processTrips) \
        .reduceByKey(lambda x,y: x+y) \
        .sortBy(lambda x: -x[1]) \
        .map(lambda x: (x[0][0], x[0][1], x[1])) \
        .groupBy(lambda x: x[0]) \
        .flatMap(lambda g: nlargest(3, g[1], key=lambda x: x[2])) \
        .map(lambda x: (x[0], (x[1], x[2]))) \
        .reduceByKey(lambda x,y: x+y)\
        .sortByKey() \
        .map(tocsv) \
        .saveAsTextFile('output')
    
