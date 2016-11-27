import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
from time import time, sleep
import numpy as np
import pandas as pd
import os.path
import datetime
import collections
from math import radians, cos, sin, asin, sqrt

stopData = ['stop_id', 'stop_lon', 'stop_lat']
stopTimeData = ['stop_id', 'arrival_time', 'trip_id', 'stop_sequence']
tripData = ['service_id', 'trip_id']
calendarData = ['service_id', 'tuesday']

width_height = 80000
img_wh = 35
dpi = 72

walk_kph = 5.7
adj_amount = img_wh * dpi / width_height

walk_meters_per_min = walk_kph * 1000 / 60

min_time = "17:00:00"
max_time = "18:00:00"



def haversine_meters(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r * 1000

def readStops(folder):
    return pd.read_csv('../' + folder + '/stops.txt')[stopData]

def readBusStops(folder):
    busStops = pd.read_csv('../' + folder + '/stops.txt')[stopData]
    busStopTimes = pd.read_csv('../' + folder + '/stop_times.txt')[stopTimeData]
    busTrips = pd.read_csv('../' + folder + '/trips.txt')[tripData]
    busCalendar = pd.read_csv('../' + folder + '/calendar.txt')[calendarData]
    return busStops, busStopTimes, busTrips, busCalendar

def plotSubwayStops(m, stops, time, color):
    # Make cute little stop circles for the stops.
    size = walk_meters_per_min * time * adj_amount
    for row in stops.itertuples():
        m.plot(row.stop_lon, row.stop_lat, marker='o', markersize=size, markeredgecolor=color, markerfacecolor=color,latlon=True)

def plotBusStops(m, stops, time, color):
    # Make cute little stop circles for the stops.
    adj_size = walk_meters_per_min * adj_amount
    for row in stops.itertuples():
        if row.time_from_subway < time:
            m.plot(row.stop_lon, row.stop_lat, marker='o', markersize=(time - row.time_from_subway) * adj_size, markeredgecolor=color, markerfacecolor=color,latlon=True)

def adjBusTimes(busStops, busStopTimes, busTrips, busCalendar):
    busTripsOnTuesday = pd.merge(busTrips, busCalendar, on='service_id', how='inner')
    busTripsOnTuesday = busTripsOnTuesday[busTripsOnTuesday['tuesday'] == 1]

    onTuesday = busStopTimes['trip_id'].isin(busTripsOnTuesday['trip_id'])
    notTooEarly = busStopTimes['arrival_time'] >= min_time
    busStopTimesInRange = busStopTimes[onTuesday & notTooEarly].sort_values(['arrival_time'])

    busStops['closest_stop_to_subway'] = busStops['stop_id']
    busStops['earliest_arrival_time'] = max_time

    stopsProcessed = 0
    time_begin = time()

    for stop in busStops.itertuples():
        if time() - time_begin > 180:
            break
        tripsStoppingHere = busStopTimesInRange[busStopTimesInRange['stop_id'] == stop.stop_id]
        for trip in tripsStoppingHere.itertuples():
            if time() - time_begin > 180:
                break
            onThisTrip = busStopTimesInRange['trip_id'] == trip.trip_id
            afterThisStop = busStopTimesInRange['stop_sequence'] > trip.stop_sequence
            nextStops = busStops[busStops['stop_id'].isin(busStopTimesInRange[onThisTrip & afterThisStop]['stop_id'])]

            for n in nextStops.itertuples():
                if time() - time_begin > 180:
                    break
                stopToCheck = busStopTimesInRange[onThisTrip & afterThisStop & (busStopTimesInRange['stop_id'] == n.stop_id)].reset_index()
                timeValue = stopToCheck.get_value(0, 'arrival_time')
                if n.earliest_arrival_time > timeValue:
                    busStops.set_value(n.Index, 'earliest_arrival_time', timeValue)
                    busStops.set_value(n.Index, 'closest_stop_to_subway', stop.stop_id)
                    busStops.set_value(n.Index, 'distance_from_subway', stop.distance_from_subway)
        stopsProcessed +=1

    print("Stops processed: " + str(stopsProcessed))
    return busStops

def makeIsochromeMap(fileName, busFolderList, lat, lon):
    # Create a map of New York City centered on Manhattan.
    plt.figure(figsize=(img_wh, img_wh), dpi=dpi)
    plt.title('1 - New York Transit Frequency')
    m = Basemap(resolution="h", projection="stere", width=width_height, height=width_height, lon_0=lon, lat_0=lat)
    numTrips = pd.DataFrame()

    numProcessed = 0

    subwayStops = readStops('Subway Data')
    busStops, busStopTimes, busTrips, busCalendar = readBusStops('Bronx Data')

    busStops['distance_from_subway'] = 80000

    # Do all pairs algorithm for bus stops to see which subway stop it is closest to.
    for s in subwayStops.itertuples():
        for b in busStops.itertuples():
            distance_haversine = haversine_meters(s.stop_lon, s.stop_lat, b.stop_lon, b.stop_lat)
            if distance_haversine < b.distance_from_subway:
                busStops.set_value(b.Index, 'distance_from_subway', distance_haversine)

    busStops['time_from_subway'] = busStops['distance_from_subway'] / walk_meters_per_min

    #print(busStops.sort_values(['distance_from_subway']))

    t0 = time()
    busStops = adjBusTimes(busStops.sort_values(['distance_from_subway']), busStopTimes, busTrips, busCalendar)
    return
    print(time()-t0)

    busStops.sort_values(['earliest_arrival_time'], inplace=True)

    #plotSubwayStops(m, subwayStops, 10, 'red')
    plotBusStops(m, busStops, 10, 'red')
    plt.savefig("img/" + fileName, facecolor='white',edgecolor='white')
    #plt.show()

makeIsochromeMap('new_york.png', [], 40.730610, -73.935242)