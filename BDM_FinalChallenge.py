import re
import csv
import itertools
import sys
from pyspark import SparkContext

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
            if re.search('^([0-9-]+)$', record[0]):
                streetId = record[2]
                borocode = int(record[13])
                fullstreet = record[28].lower().strip()
                stname = record[29].lower().strip()
                streetNumBeginOdd = tuple(map(int, filter(None, record[0].split('-'))))
                streetNumEndOdd = tuple(map(int, filter(None, record[1].split('-'))))
                streetNumBeginEven = tuple(map(int, filter(None, record[4].split('-'))))
                streetNumEndEven = tuple(map(int, filter(None, record[5].split('-'))))
                yield (streetId, (borocode, fullstreet, stname, 
                                  streetNumBeginOdd, streetNumEndOdd, streetNumBeginEven, streetNumEndEven))
                next(itertools.islice(buffer, steps, steps), None)
        else:
            for no, line in enumerate(itertools.islice(buffer, steps), prevLine):
                yield (None, (((pid,no), line),))
def getKey(val, myDict): 
    for key, value in myDict.items(): 
        if val in value:
            return key
    return(0)

def getStreetId(boro, oddOrEven, housenum, street, streetDictionary): 
    for key, value in streetDictionary.items(): 
        if (boro==value[0] and (street == value[1] or street==value[2])):
            if((oddOrEven == 1) and (housenum >= value[3] and housenum <= value[4])):
                return key
            if((oddOrEven == 0) and (housenum >= value[5] and housenum <= value[6])):
                return key
    return(None)

def extractFull(pid, rows):
    # boro lookups
    boro = {1: ['MAN','MH','MN','NEWY','NEW Y','NY'], 
            2: ['BRONX', 'BX'], 
            3: ['BK', 'K', 'KING', 'KINGS'], 
            4: ['Q', 'QN', 'QNS', 'QU', 'QUEEN'],
            5: ['R', 'RICHMOND']}
    if pid==0:
        next(rows)
    buffer, lines = itertools.tee(rows)
    reader = csv.reader(lines)
    prevLine = 0
    for record in reader:
        steps = reader.line_num - prevLine
        prevLine = reader.line_num
        if (record[21] is not None and record[23] is not None and record[24] is not None):
            # get only records with numbers or -
            if re.search('^([0-9-]+)$', record[23]):
                boroCode = getKey(record[21], boro)
                streetNum = tuple(map(int, filter(None, record[23].split('-'))))
                violationStreetName = record[24].lower().strip()
                year = int(record[4][-4:])
                # determine if odd or even
                if len(streetNum)==2:
                    oddEven = streetNum[1] % 2
                else:
                    oddEven = streetNum[0] % 2
                # get the streetid
                streetid = getStreetId(boroCode, oddEven, streetNum, violationStreetName, dictionary_bc.value)
                if streetid is not None:
                    yield ((year, streetid), 1)
                    next(itertools.islice(buffer, steps, steps), None)
        else:
            for no, line in enumerate(itertools.islice(buffer, steps), prevLine):
                yield (None, (((pid,no), line),))
                
def tocsv(data):
    return ','.join(str(d) for d in data)

if __name__=='__main__':
    fn = '/tmp/bdm/nyc_parking_violation/*.csv'
    sc = SparkContext()
    
    # create dictionary of streets
    dictionary = sc.textFile('/tmp/bdm/nyc_cscl.csv')\
        .mapPartitionsWithIndex(createStreetIndex)\
        .collectAsMap()
    dictionary_bc = sc.broadcast(dictionary)
    
    sc.textFile(fn)\
        .mapPartitionsWithIndex(extractFull)\
        .filter(lambda x: int(x[0][0]) > 2014 and int(x[0][0]<2020))\
        .reduceByKey(lambda x, y : x+y)\
        .map(tocsv) \
        .saveAsTextFile('output')
    