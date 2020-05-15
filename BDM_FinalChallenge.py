import re
import csv
import itertools
import sys
from pyspark import SparkContext

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
                yield ((boroCode, violationStreetName),(streetNum,year))
                next(itertools.islice(buffer, steps, steps), None)
        else:
            for no, line in enumerate(itertools.islice(buffer, steps), prevLine):
                yield (None, (((pid,no), line),))


if __name__=='__main__':
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
        .filter(lambda x: x[1][1] > 2014 and  x[1][1] < 2020)\
        .join(dictionary)\
        .filter(lambda x: compareStreet(x))\
        .map(lambda x: ((x[1][1][1], x[1][0][1]), 1))\
        .reduceByKey(lambda x, y: x+y)\
        .map(lambda x: (x[0][0], (x[0][1], x[1])))\
        .reduceByKey(lambda x, y: x+y)\
        .map(lambda x: (x[0], x[1][1], x[1][3], x[1][5], x[1][7]))\
        .map(tocsv)\
        .saveAsTextFile(sys.argv[2])
    