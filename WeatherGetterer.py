import json, csv
import datetime, time
import sys, os
#import shutil
#import unicodedata
#import smtplib
import forecastio

#from copy import deepcopy, copy
#from geopy import geocoders
#from dateutil import parser
from multiprocessing import Process, Queue, cpu_count, Manager

import gDocsImport as gd
from GetterersToolkit import *




def pullOne(tracker,location,timing):
    readValues = dict()
    values = tracker['values']
    row = {'place':location['place'],'lat':location['lat'],'lon':location['lon']}
    
    tries = 0
    done = False
    maxTries = 10
    
    while not done and tries != maxTries:
        try:
            if timing != 'null':
                weatherIn = forecastio.load_forecast(tracker['login']['key'], location['lat'], location['lon'], timing)  
            else:
                weatherIn = forecastio.load_forecast(tracker['login']['key'], location['lat'], location['lon'])
            
            pulledWeather = eval("weatherIn."+tracker['timing']) 
            
            for valKey,value in values.iteritems():
                try:
                    attribute = eval("pulledWeather."+value)
                    readValues[valKey] = attribute
                except:
                    None
                    
            done = True
            
        except:
            print "Weather pull", tries, "failed, will sleep for 5 minutes and try again"
            tries += 1
            time.sleep(300)
    
    if tries == maxTries:
        weatherIn = 'null'
        print "Error: could not pull weather for", location['place'],time
           
    return weatherIn , dict(row.items()+readValues.items()) 
    
    
    

def pullWeather(tracker,locations):
    collectedContent = []
    for locKey,location in locations.iteritems():
        null,readValues = pullOne(tracker,location,'null')
        collectedContent.append(readValues)
    return collectedContent




def morningDone(timeData):
    for key in timeData.keys():
        if not timeData[key]['ranMorning']:
            return False
    return True




def afternoonDone(timeData):
    for key in timeData.keys():
        if not timeData[key]['ranAfternoon']:
            return False
    return True
    
    
def locationsListed(locations):
	listed = [0]*len(locations.keys())
	for key,item in locations.iteritems():
		listed[item['index']] = stripUnicode(item['place'])
	return listed



def bleedData(directory,tracker,locations,q):
    print "Data-bleed initiated..."
    startDay = datetime.datetime.now().replace(hour=12,minute=0,second=0)
    rate = getRate(tracker,len(locations))
    print "Starting on", startDay.strftime("%A %d"), 'at 1 query every', rate, 'seconds...'
    listed = locationsListed(locations)
    for pos in listed:
	locKey = pos
	location = locations[pos]
	print locations.keys()
        count = 0
        for day in range(1000):
            if count%5 == 0:
                fileOut = open('bleedTracker.txt','a+b')
                fileOut.write('Place: '+location['place']+'\tCount: '+str(count))
            pullday = startDay-datetime.timedelta(days=day)
            print "Pulling data for", location['place'], 'on', pullday.strftime("%A %d")
            weather,block = pullOne(tracker,location,startDay-datetime.timedelta(days=day))
            print block
            writeCSV(directory+'bled/',tracker,{'locKey':block},'',True)
            time.sleep(rate)
        
        




def noonForecast(directory,tracker,locations,q):
    while True:
        timeData = dict()
        morningStreams = dict()
        afternoonStreams = dict()
        morningBlocks = dict()
        afternoonBlocks = dict()
        wroteMorning = False
        
        startDay = datetime.datetime.now().strftime('%A')
        
        for locKey,location in locations.iteritems():
            timeData[locKey] = {'ranMorning':False,'ranAfternoon':False,'offset':False,'runDay':'frunday spectacular','tillNoon':0,'observed':0}
        while True:    
            while not afternoonDone(timeData):
                if not morningDone(timeData):
                    for locKey,location in locations.iteritems():
                        if not timeData[locKey]['ranMorning']:

                            morningStreams[locKey], morningBlocks[locKey] = pullOne(tracker,location,'null')
                            
                            if morningStreams[locKey] != 'null': 
                                currentTime = morningStreams[locKey].currently().time
                                #print currentTime
                                noonTime = currentTime.replace(hour= 12, minute=0, second=0)
    
                                morningStreams[locKey], morningBlocks[locKey] = pullOne(tracker,location,noonTime)
                                
                                if currentTime > noonTime:
                                    afternoonBlocks[locKey] = morningBlocks[locKey]
                                    timeData[locKey]['ranAfternoon'] = True
                                
                                timeData[locKey]['tillNoon']= noonTime-currentTime
                                timeData[locKey]['observed']= datetime.datetime.now()
                            else:
                                print "Error, could not pull weather"
                                None
                            timeData[locKey]['ranMorning'] = True
                            
                if not wroteMorning:
                    writeCSV(directory+'morningforecast/',tracker,morningBlocks,'Morn',False)
                    del morningBlocks
                    wroteMorning = True
                    
                for locKey,location in locations.iteritems():
                    if (datetime.datetime.now() - timeData[locKey]['observed']) >= timeData[locKey]['tillNoon'] and not timeData[locKey]['ranAfternoon']:
                        currentTime = morningStreams[locKey].currently().time
                        del morningStreams[locKey]
                        noonTime = currentTime.replace(hour= 12, minute=0, second=0)

                        afternoonStreams[locKey], afternoonBlocks[locKey] = pullOne(tracker,location,noonTime)
                        timeData[locKey]['ranAfternoon'] = True
                
                if not afternoonDone(timeData):
                    time.sleep(600)
                
            writeCSV(directory+'historic/',tracker,afternoonBlocks,'',True)
            
            print "Sleeping until next day"
            while datetime.datetime.now().strftime('%A') == startDay:
                time.sleep(3600)
            print "New day started:", datetime.datetime.now().strftime('%A')
            #time.sleep(15000)

            
def runOnce(directory,tracker,locations,q):
    collectedContent = pullWeather(tracker,locations)
    writeCSV(directory,tracker,collectedContent,'',True)
    q.put('null')
        
        
    

def main():
    directory = os.getcwd() + '/'
    temp = weatherGDILoad(sys.argv[1],directory)
    trackers = temp['trackers']
    locations = temp['locations']
    queue = Queue()
    running = dict()
    while True:
	print
        for trackKey,tracker in trackers.iteritems():
            if tracker['method'] == 'noon forecast':
                print "STARTING STREAM TYPE: NOON FORECAST"
                running[trackKey] = Process(target = noonForecast, args=(directory,tracker,locations,queue))
            elif tracker['method'] == 'run once':
                print "STARTING STREAM TYPE: RUN ONCE"
                running[trackKey] = Process(target = runOnce, args=(directory,tracker,locations,queue))
            elif tracker['method'] == 'bleed data':
                print "STARTING STREAM TYPE: BLEED DATA"
                running[trackKey] = Process(target = bleedData, args=(directory,tracker,locations,queue))
	print	
	for trackKey in trackers.keys():
		print "Starting tracker:",trackKey
		running[trackKey].start()
        for trackKey in trackers.keys():
		print "Waiting for tracker (may not terminate):",trackKey
		running[trackKey].join()
        quit()
            
            
            
main()