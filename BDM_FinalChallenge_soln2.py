import re
import csv
import itertools
import sys
import numpy as np
import datetime
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
        return 0

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
        streetId = record[0]
        borocode = int(record[13])
        fullstreet = record[28].lower().strip()
        stname = record[29].lower().strip()
        streetNumBeginOdd = tuple(map(int, filter(None, record[2].split('-'))))
        streetNumEndOdd = tuple(map(int, filter(None, record[3].split('-'))))
        streetNumBeginEven = tuple(map(int, filter(None, record[4].split('-'))))
        streetNumEndEven = tuple(map(int, filter(None, record[5].split('-'))))
        yield ((borocode, fullstreet),
               (stname, streetId, 
                streetNumBeginOdd, streetNumEndOdd, streetNumBeginEven, streetNumEndEven))

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
        if (record[21] is not None and record[23] is not None and record[24] is not None and record[4] is not None):
            # get only records with numbers or -
            if re.search('^([0-9-]+)$', record[23]):
                boroCode = getBoro(record[21])
                streetNum = tuple(map(int, filter(None, record[23].split('-'))))
                violationStreetName = record[24].lower().strip()
                year = getInt(record[4][-4:])
                if year > 2014 and year < 2020 and boroCode > 0 and violationStreetName != '':
                    yield ((year, boroCode, violationStreetName,streetNum), 1)
                    next(itertools.islice(buffer, steps, steps), None)
        else:
            for no, line in enumerate(itertools.islice(buffer, steps), prevLine):
                yield (None, (((pid,no), line),))

def getYearCounts(record):
    rawCounts = record[1]
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
    return((record[0], yearCountsTuple))

def fillBlanks(record):
    try:
        streetInfo = record[0]
        violations = record[1]
        if violations is None:
            violations = ((), (0, 0, 0, 0, 0))
        newRecords = (streetInfo, violations)
    except:
        newRecords = record
    return newRecords

# get coef by hand
def getCoef(row):
    """
    function to calc b in y = a+bx
    """
    x = np.array([0, 1, 2, 3, 4]) # years to correspond to 2015 - 2019
    y = np.array(list(row))
    n = 5
    xs = sum(x)
    ys = sum(y)
    x2 = sum(x**2)
    y2 = sum(y**2)
    xy = sum(x*y)
    b = ((n*xy) - (xs*ys))/((n*x2)-(xs)**2)
    ans = (row[0], row[1], row[2], row[3], row[4], round(b,2))
    return(ans)

def compareHouseNumbers(record):
    #break up record parts
    streetId = record[0][0]
    yearCounts = record[1][1]
    violationHouseNum = record[1][0]
    oddBegin = record[0][1][0]
    oddEnd = record[0][1][1]
    evenBegin =record[0][1][2]
    evenEnd = record[0][1][3]
    # check records
    if not violationHouseNum:
        newRecord = (streetId, yearCounts)
    else:
        lenViolation = len(violationHouseNum)
        if lenViolation == 2:
            if violationHouseNum[1] % 2 == 1:
                if violationHouseNum >= oddBegin and violationHouseNum <= oddEnd:
                    newRecord = (streetId, yearCounts)
                else:
                    newRecord = (streetId, (0, 0, 0, 0, 0))
            if violationHouseNum[1] % 2 == 0:
                if violationHouseNum >= evenBegin and violationHouseNum <= evenEnd:
                    newRecord = (streetId, yearCounts)
                else:
                    newRecord = (streetId, (0, 0, 0, 0, 0))
        if lenViolation == 1:
            if violationHouseNum[0] % 2 == 1:
                if violationHouseNum >= oddBegin and violationHouseNum <= oddEnd:
                    newRecord = (streetId, yearCounts)
                else:
                    newRecord = (streetId, (0, 0, 0, 0, 0))
            if violationHouseNum[0] % 2 == 0:
                if violationHouseNum >= evenBegin and violationHouseNum <= evenEnd:
                    newRecord = (streetId, yearCounts)
                else:
                    newRecord = (streetId, (0, 0, 0, 0, 0))
    return(newRecord)
    
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
    
    parking = sc.textFile(fn)\
        .mapPartitionsWithIndex(extractFull)\
        .reduceByKey(lambda x, y: x + y)\
        .sortByKey()\
        .map(lambda x: ((x[0][1], x[0][2], x[0][3]), (x[0][0], x[1])))\
        .reduceByKey(lambda x, y: x + y)\
        .map(lambda x: ((x[0][0], x[0][1]), (x[0][2], x[1])))\
        .mapValues(lambda x : getYearCounts(x))

    fullStreet = sc.textFile('/tmp/bdm/nyc_cscl.csv')\
        .mapPartitionsWithIndex(createStreetIndex)\
        .mapValues(lambda x:( x[1], x[2], x[3], x[4], x[5]))
    
    street = sc.textFile('tmp/bdm/nyc_cscl.csv')\
        .mapPartitionsWithIndex(createStreetIndex)\
        .map(lambda x: ((x[0][0], x[1][0]), x[1][1:]))\

    sc.union([street, fullStreet])\
        .map(lambda x: ((x[0][0], x[0][1], x[1][0]), (x[1][1:])))\
        .distinct()\
        .map(lambda x: ((x[0][0], x[0][1]), (x[0][2], x[1])))\
        .leftOuterJoin(parking)\
        .mapValues(lambda x: fillBlanks(x))\
        .mapValues(lambda x: compareHouseNumbers(x))\
        .map(lambda x: ((x[1][0]), (x[1][1])))\
        .reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1], x[2]+y[2], x[3]+y[3], x[4]+y[4]))\
        .mapValues(lambda x: getCoef(x))\
        .map(lambda x: (x[0], x[1][0], x[1][1], x[1][2], x[1][3], x[1][4], x[1][5]))\
        .map(lambda x: tocsv(x))\
        .saveAsTextFile(sys.argv[2])
    
    end = datetime.datetime.now()
    print(end - start)
