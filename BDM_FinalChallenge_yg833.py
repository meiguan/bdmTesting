import re
import csv
import itertools
import sys
import numpy as np
import datetime as datetime
from pyspark import SparkContext

# helper functions
def tocsv(data):
    return ','.join(str(d) for d in data)

def getInt(data):
    try:
        return int(data)
    except:
        return 0

def getBoro(key):
    try:
        return boro_bc.value[key]
    except:
        return None

# create index from the street
def createStreetIndex(pid, rows):
    if pid==0:
        next(rows)
    buffer, lines = itertools.tee(rows)
    reader = csv.reader(lines)
    prevLine = 0
    for record in reader:
        steps = reader.line_num - prevLine
        prevLine = reader.line_num
        #check length of line
        if (len(record)==32):
            # check for the numerics on the lower bound
            if re.search('^([0-9-]+)$', record[2]):
                streetId = record[0]
                borocode = getInt(record[13])
                fullstreet = record[28].lower().strip()
                stname = record[29].lower().strip()
                streetNumBeginOdd = tuple(map(int, filter(None, record[2].split('-'))))
                streetNumEndOdd = tuple(map(int, filter(None, record[3].split('-'))))
                streetNumBeginEven = tuple(map(int, filter(None, record[4].split('-'))))
                streetNumEndEven = tuple(map(int, filter(None, record[5].split('-'))))
                yield ((borocode, fullstreet), 
                       ((streetNumBeginOdd, streetNumEndOdd, streetNumBeginEven, streetNumEndEven),(streetId)))
                next(itertools.islice(buffer, steps, steps), None)
        else:
            for no, line in enumerate(itertools.islice(buffer, steps), prevLine):
                yield (None, (((pid,no), line),))

# process the violation tickets initially
def extractFull(pid, rows):
    if pid==0:
        next(rows)
    buffer, lines = itertools.tee(rows)
    reader = csv.reader(lines)
    prevLine = 0
    for record in reader:
        steps = reader.line_num - prevLine
        prevLine = reader.line_num
        if (record[21] is not None and record[23] is not None and record[24] is not None and 
            record[4] is not None and record[4] != ''):
            # get only records with numbers or -
            if re.search('^([0-9-]+)$', record[23]):
                boroCode = getBoro(record[21])
                streetNum = tuple(map(int, filter(None, record[23].split('-'))))
                violationStreetName = record[24].lower().strip()
                year = getInt(record[4][-4:])
                if year > 2014 and year < 2020:
                    yield ((boroCode, violationStreetName),(streetNum,year))
                    next(itertools.islice(buffer, steps, steps), None)
        else:
            for no, line in enumerate(itertools.islice(buffer, steps), prevLine):
                yield (None, (((pid,no), line),))

# compare the street numbers
def compareStreet(row):
    if len(row[1][0][0]) == 1:
        if row[1][0][0][0] % 2 == 0: # even numbers
            if row[1][0][0] >= row[1][1][0][2] and row[1][0][0] <= row[1][1][0][3]:
                return True
            else:
                return False
        if row[1][0][0][0] % 2 == 1: # odd numbers
            if row[1][0][0] >= row[1][1][0][0] and row[1][0][0] <= row[1][1][0][1]:
                return True
            else:
                return False
    if len(row[1][0][0]) == 2:
        if row[1][0][0][1] % 2 == 0: # even numbers
            if row[1][0][0] >= row[1][1][0][2] and row[1][0][0] <= row[1][1][0][3]:
                return True
            else:
                return False
        if row[1][0][0][1] % 2 == 1: # odd numbers
            if row[1][0][0] >= row[1][1][0][0] and row[1][0][0] <= row[1][1][0][1]:
                return True
            else:
                return False
    else:
        return False

# get the counts by year
def getYearCounts(rawCounts):
    size = len(rawCounts)
    yearCounts = {2015:0, 2016:0, 2017:0, 2018:0, 2019:0}
    for i in range(size):
        if rawCounts[i] == 2015:
            i +=1
            yearCounts[2015] = rawCounts[i]
        if rawCounts[i] == 2016:
            i +=1
            yearCounts[2016] = rawCounts[i]
        if rawCounts[i] == 2017:
            i+=1
            yearCounts[2017] = rawCounts[i]
        if rawCounts[i] == 2018:
            i+=1
            yearCounts[2018] = rawCounts[i]
        if rawCounts[i] == 2019:
            i+=1
            yearCounts[2019] = rawCounts[i]
    yearCountsTuple = tuple(yearCounts.values())
    return(yearCountsTuple)

# get coef by hand
def getCoef(row):
    """
    function to calc b in y = a+bx
    """
    x = np.array([2015, 2016, 2017, 2018, 2019])
    y = np.array(list(row))
    n = 5
    xs = sum(x)
    ys = sum(y)
    x2 = sum(x**2)
    y2 = sum(y**2)
    xy = sum(x*y)
    b = ((n*xy) - (xs*ys))/((n*x2)-(xs)**2)
    ans = (row, round(b,2))
    return(ans)

    
if __name__=='__main__':
    start = datetime.datetime.now()
    fn = sys.argv[1]
    sc = SparkContext()
    
    boroDictionary = {'MAN':1,'MH':1,'MN':1,'NEWY':1,'NEW Y':1,'NY':1, 
                  'BRONX':2, 'BX':2,
                  'BK':3,'K':3,'KING':3,'KINGS':3, 
                  'Q':4,'QN':4,'QNS':4,'QU':4,'QUEEN':4,
                  'R':5,'RICHMOND':5}
    boro_bc = sc.broadcast(boroDictionary)
    
    dictionary = sc.textFile('/tmp/bdm/nyc_cscl.csv')\
        .mapPartitionsWithIndex(createStreetIndex)
    
    sc.textFile(fn)\
        .mapPartitionsWithIndex(extractFull)\
        .join(dictionary)\
        .filter(lambda x: compareStreet(x))\
        .map(lambda x: ((x[1][1][1], x[1][0][1]), 1))\
        .reduceByKey(lambda x, y: x+y)\
        .sortByKey()\
        .map(lambda x: (x[0][0], (x[0][1], x[1])))\
        .reduceByKey(lambda x, y: x + y)\
        .mapValues(lambda x: getYearCounts(x))\
        .mapValues(lambda x: getCoef(x))\
        .map(tocsv)\
        .saveAsTextFile(sys.argv[2])

    end = datetime.datetime.now()
    print(end - start)
