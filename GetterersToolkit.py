import json, csv
#import random
import datetime,time
import os, shutil
import unicodedata
#import tweepy
import smtplib
import cPickle

import gDocsImport as gd

#from email.MIMEMultipart import MIMEMultipart
#from email.MIMEBase import MIMEBase
#from email.MIMEText import MIMEText
#from email import Encoders

from copy import deepcopy, copy
from geopy.distance import great_circle
from geopy import geocoders
#from dateutil import parser
#from math import pi, cos, ceil
#from multiprocessing import Process, Queue, cpu_count, Manager

forecastioValues = "time,summary,icon,sunriseTime,sunsetTime,moonPhase,nearestStormDistance,nearestStormBearing,precipIntensity,precipIntesityMax,precipIntensityMaxTime,precipProbability,precipType,precipAccumulation,temperature,temperatureMin,temperatureMinTime,temperatureMax,temperatureMaxTime,apparentTemperature,apparentTemperatureMin,apparentTemperatureMinTime,apparentTemperatureMax,apparentTemperatureMaxTime,dewPoint,windSpeed,windBearing,cloudCover,humidity,pressure,visibility,ozone"

allValues = {'forecastio':forecastioValues.split(',')}


weatherEmail = 'Subscriber Email,CC Email'
weatherParams = 'Param Name,Param Value'
weatherLists = 'Locations'
pickleName = "GeoPickle.txt"

def updateGeoPickle(dictionary,fileRef):
    """Updates file & memory version of geoPickle"""
    loadedLength = length1 = len(dictionary.keys())
    pickleExists = os.path.isfile(fileRef)
    if pickleExists:
        pickleIn = openWhenReady(fileRef, "rb")
        pickleLoaded = cPickle.load(pickleIn)
        length1 = len(pickleLoaded.keys())
        pickleIn.close()
        if dictionary.keys() != pickleLoaded.keys():
            dictionary.update(pickleLoaded)
            needsWrite = True
        else:
            needsWrite = False      
    else:
        needsWrite = True
    
    length2 = len(dictionary.keys())
    if needsWrite and loadedLength != 0:
        print "Updating master geoPickle,", length2-length1,"new locations added with",length2,"total in cache"
        pickleOut = openWhenReady(fileRef,"wb")
        cPickle.dump(dictionary, pickleOut)
        pickleOut.close()
        time.sleep(.5)




def patientGeoCoder(request,cfg):
    """Patient geocoder, will wait if API rate limit hit"""
    gCoder = geocoders.GoogleV3()
    tries = 0
    limit = 1
    delay = 5
    if "Cores" in cfg.keys():
        delay *= cfg['Cores']
    while True:
        try:
            return gCoder.geocode(request)
        except:
            tries +=1
            if tries == limit+1 or not cfg['PatientGeocoding']:
                if 'Cores' not in cfg.keys():
                    print "\nUnable to geoCode", request, '\n'
                return "timeOut", ('NaN','NaN')
            time.sleep(delay)
            
            
            

def getLocation(cfg,geoCache,cacheRef):
    """Returns coordinates from query"""
    #http://code.google.com/p/geopy/wiki/GettingStarted
    #gCoder = geocoders.GoogleV3()
    hasCoords = False
    hasPlace = False
    coordsWork = False
    fromFile = False
    place = 'NaN'


    if cacheRef in geoCache.keys():
        loaded = geoCache[cacheRef]
        #print "DEBOOO: Found in geocache"        
        if loaded['lat'] != 'NaN' and loaded['lon'] != 'NaN' and loaded['place'] != 'NaN':
            print "GEOCACHE: Inboxed from memory", cacheRef
            return loaded
 
    print "GEOCACHE: Looking up", cacheRef
        
    if not hasCoords and not coordsWork:
        #lookup coords by location name
        try:
            place, (lat, lng) = patientGeoCoder(cacheRef,cfg)
            time.sleep(.15); coordinates = [lng,lat] 
            hasPlace = True
            hasCoords = True
        except:
            #print "DEBOO CANT GET COORDS"
            output = {'inBox':False,'text':'NoCoords','place':'NaN','lat':'NaN','lon':'NaN','trueLoc':coordsWork}
            geoCache[cacheRef] = output
            return output
   
    if not hasCoords:
        #print "DEBOO HAS NO COORDS"
        output = {'inBox':False,'text':'NoCoords','place':'NaN','lat':'NaN','lon':'NaN','trueLoc':coordsWork}
        geoCache[cacheRef] = output
        return output
    else:
        if not hasPlace:
            try:
                place, (lat, lng) = patientGeoCoder(str(coordinates[1])+','+str(coordinates[0]),cfg)
                time.sleep(.15)
            except:
                None

    if place == "timeOut":
        #print "DEBOO TIMEOUT"
        return {'inBox':False,'text':'NoCoords','place':'NaN','lat':'NaN','lon':'NaN','trueLoc':coordsWork}

    output =  {'inBox':True,'text':'InBox','place':place,'lat':coordinates[1],'lon':coordinates[0],'trueLoc':coordsWork}
    geoCache[cacheRef] = output
    return output
    
    
    

def checkBool(text):
    if type(text) is str:
        if text.lower() == 'false':
            return False
        elif text.lower() == 'true':
            return True
    return text
    
    
def checkType(text):
    if type(text) is str:
        if text.lower() == 'false':
            return False
        elif text.lower() == 'true':
            return True
        else:
            try:
                if float(text) == int(text):
                    return int(text)
                else:
                    return float(text)
            except:
                return text
        
                
def getTrackers(config):
    "Pull set of experiments from GDI file"
    experiments = set()
    paramsOut = dict()
    runKeys = ['source','delay','method','key','file','timing','getAll',
                'daysAhead','daysBehind','merge','checkMissing','checkLimit',
                'daysBack','deboo','keepOld']
    for line in config:
        experiments.add(line[0].split('.')[0])
    experiments = list(experiments)
    print "Pulling values for experiments:", experiments
    for item in experiments:
        dictOut = {'method':'max daily','file':'weatherOut.csv',
                    'source':'forecastio','delay':0,'timing':'currently()',
                    'getAll':False,'values':dict(), 'daysAhead':3,'daysBehind':0,
                    'merge':'null','checkMissing':500,'checkLimit':50,
                    'daysBack':1000,'keepOld':False,'jump':1}
        for line in config:
            temp = line[0].split('.')
            if temp[0] == item:
                if temp[1] in runKeys:
                    if temp[1] == 'merge' or temp[1] == 'key':
                        dictOut[temp[1]] = [entry for entry in line[1:] if len(entry)>3]
                    else:
                        dictOut[temp[1]] = checkType(line[1])
                else:
                    dictOut['values'][temp[1]] = checkType(line[1])
        if dictOut['getAll']:
            temp = allValues[dictOut['source']]
            for key in temp:
                dictOut['values'][key] = key
        paramsOut[item] = dictOut
    return(paramsOut)
    
    
    
    
def getLogin(directory, fileNames):
    """gets login parameters from list & directory passed on by config file"""
    params = {'description':'null'}
    logins = []
    
    """if ' ' in fileName:
        fileNames = fileName.split(' ')
        multiLogin = True
    else:
        fileNames = [fileName]
        multiLogin = False"""
        
    if directory == "null":
        directory = ''
        
    
    for fileName in fileNames:
        print "Loading login file:", directory + fileName
        try:
            try: 
                fileIn = open(directory+'/logins/' + fileName)
            except:
                fileIn = open(directory+fileName)
            content = fileIn.readlines()
            for item in content:
                if ' = ' in item:
                    while '  ' in item:
                        item = item.replace('  ',' ')
                    while '\n' in item:
                        item = item.replace('\n','')
                    line = item.split(' = ')
                    try:
                        line[1] = float(line[1])
                        if line[1] == int(line[1]):
                            line[1] = int(line[1])
                    except:
                        None
                    params[line[0]] = line[1]
            #for key,item in params.iteritems():
            #    print '\t*', key,':', item
            logins.append(deepcopy(params)) 
        except:
            print "\tlogin file not found"
        print params           
    return logins  
                


                    
def openWhenReady(directory, mode):
    """Trys to open a file, if unable, waits five seconds and tries again"""
    attempts = 0
    while True:
        try:
            fileOut = open(directory,mode)
            break
        except:
            time.sleep(5)
            attempts += 1
            if attempts == 1000:
                print "Error: Unable to open", directory, "for 5000 seconds, quiting now"
                quit()
    return fileOut                                        
                                                                                
    


def getLocations(directory,locations):
    geoCache = dict()
    cfg = dict()
    updateGeoPickle(geoCache,directory+pickleName)
    places = dict()
    count=0
    for line in locations:
        location = getLocation(cfg,geoCache,line)
	temp = stripUnicode(location['place'])
        places[temp] = {'lat':location['lat'],'lon':location['lon'],'place':temp,'index':count,'query':line}
	count += 1
    updateGeoPickle(geoCache,directory+pickleName)
    return {'place':places,'geoCache':geoCache}
    
        


def getDelay(tracker,numLocations):
    secPerDay = 86400
    apiLimits = {'forecastio':1000}
    limit = apiLimits[tracker['source'].lower()]
    delay = (secPerDay*numLocations)/limit
    return delay
    
    
def getRate(tracker,numLocations):
    secPerDay = 86400
    apiLimits = {'forecastio':1000}
    limit = apiLimits[tracker['source'].lower()]
    rate = float(secPerDay)/(limit*len(tracker['login']))
    return rate
         
        


def weatherGDILoad(gDocURL,directory):
    """Updates user config & lists for GDI seeker"""
    gdi = {}
    cfg = {}
    account = {}
    public = True
    
    print "Loading params from linked GDI sheet"
    
    if not public:
        config = gd.getScript(account['userName'], account['password'], account['fileName'], weatherParams, weatherLists, "default", False, [])
    else:
        config = gd.getScript('null', 'null', gDocURL, weatherParams, weatherLists, "default", False, [])
        
    trackers = getTrackers(config)
    
    print "Loading logins"
    
    for key,item in trackers.iteritems():
        trackers[key]['login'] = getLogin(directory,item['key'])

    print "Loading word list"        

    if not public:
        uglyLists = gd.getScript(account['userName'], account['password'], account['fileName'], weatherLists, -1, "default", False, [])
    else:
        uglyLists = gd.getScript('null', 'null', gDocURL, weatherLists, -1, "default", False, [])

    
    locations = []
    for pos in range(len(uglyLists)):
        row = uglyLists[pos]
        if len(str(row[0])) > 3 and row not in locations:
            locations.append(row[0])
        
    temp = getLocations(directory, locations) 
    geocoded = temp['place']
    geoCache = temp['geoCache']
    
    print; print  
    for key,tracker in trackers.iteritems():
        trackers[key]['runDelay'] = getDelay(tracker,len(geocoded.keys()))

    return {'trackers':trackers,'locations':geocoded,'geoCache':geoCache}
    
    


def stripUnicode(text):
    """Strips unicode special characters for text storage (smileys, etc)"""
    if text == None:
        return "NaN"
    else:
        if type(text) == unicode:
            return str(unicodedata.normalize('NFKD', text).encode('ascii', 'ignore'))
        else:
            return text


def writeCSV(directory, tracker, collectedContent, prefix, append):
    allKeys = set()
    dataOut = []
    
    if type(collectedContent) is list:
        temp = dict()
        for line in collectedContent:
            temp[line['place']+str(line['time'])]=line
        collectedContent = temp
            
    for key in collectedContent.keys():
        allKeys.update(collectedContent[key].keys())
            
    orderedKeys = ['place','time'] + sorted([word for word in list(allKeys) if word != 'place' and word != 'time'])
    
    outName = prefix + tracker['file']
                
    for pos in sorted(collectedContent.keys()):
        for key in orderedKeys:
            if key not in collectedContent[pos].keys():
                collectedContent[pos][key] = 'NaN'
            else:
                collectedContent[pos][key] = stripUnicode(collectedContent[pos][key])
        dataOut.append(collectedContent[pos])
    
    print "Writing collected weather data to "+outName  
    
    if not os.path.exists(directory):
        os.makedirs(directory) 
        
    if append and os.path.exists(directory+outName):
        outFile = open(directory+outName, "a+b") 
        csvOut = csv.DictWriter(outFile,orderedKeys)
    else:     
        outFile = open(directory+outName, "w") 
        csvOut = csv.DictWriter(outFile,orderedKeys)
        csvOut.writer.writerow(orderedKeys)

    csvOut.writerows(dataOut)
    outFile.close()
