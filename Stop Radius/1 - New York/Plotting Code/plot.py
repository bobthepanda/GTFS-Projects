import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
from time import time, sleep, strptime
import numpy as np
import pandas as pd
import os.path
import datetime
import collections
from math import radians, cos, sin, asin, sqrt
from datetime import timedelta, datetime

stopData = ['stop_id', 'stop_lon', 'stop_lat']
stopTimeData = ['stop_id', 'arrival_time', 'trip_id', 'stop_sequence']
tripData = ['route_id', 'service_id', 'trip_id']
calendarData = ['service_id', 'tuesday']

walk_kph = 5.7
adj_amount = 36 * 72 / 80000

walk_meters_per_min = walk_kph * 1000 / 60

min_time = "17:00:00"
max_time = "23:59:59"
time_format = "%H:%M:%S"
min_time_dt = datetime.strptime(min_time, time_format)


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

def readBusStops(folderList):
    busStops = pd.DataFrame()
    busStopTimes = pd.DataFrame()
    busTrips = pd.DataFrame()
    busCalendar = pd.DataFrame()

    for folder in folderList:
        busStops = busStops.append(pd.read_csv('../' + folder + '/stops.txt')[stopData])
        busStopTimes = busStopTimes.append(pd.read_csv('../' + folder + '/stop_times.txt')[stopTimeData])
        busTrips = busTrips.append(pd.read_csv('../' + folder + '/trips.txt')[tripData])
        busCalendar = busCalendar.append(pd.read_csv('../' + folder + '/calendar.txt')[calendarData])

    busTrips['express'] = busTrips['route_id'].apply(lambda x: True in [prefix in x[:len(prefix)] for prefix in ['QM', 'BM', 'BxM', 'SM', 'X']])
    busTrips = busTrips[~busTrips['express']]
    return busStops.drop_duplicates().reset_index(), busStopTimes.drop_duplicates().reset_index(), busTrips.drop_duplicates().reset_index(), busCalendar.drop_duplicates().reset_index()

def plotSubwayStops(m, stops, time, color, alpha):
    # Make cute little stop circles for the stops.
    size = walk_meters_per_min * time * adj_amount
    for row in stops.itertuples():
        m.plot(row.stop_lon, row.stop_lat, marker='o', markersize=size, markeredgecolor='none', markerfacecolor=color, alpha=alpha, latlon=True)

def plotBusStops(m, stops, time, color, alpha):
    # Make cute little stop circles for the stops.
    adj_size = walk_meters_per_min * adj_amount
    for row in stops.itertuples():
        traveled_time = row.total_travel_time
        if traveled_time <= time:
            m.plot(row.stop_lon, row.stop_lat, marker='o', markersize=(time - traveled_time) * adj_size, markeredgecolor='none', markerfacecolor=color, alpha=alpha, latlon=True)

def adjBusTimes(busStops, busStopTimes, busTrips, busCalendar):
    busTripsOnTuesday = pd.merge(busTrips, busCalendar, on='service_id', how='inner')
    busTripsOnTuesday = busTripsOnTuesday[busTripsOnTuesday['tuesday'] == 1]

    notTooEarly = busStopTimes['arrival_time'] >= min_time
    notTooLate = busStopTimes['arrival_time'] <= max_time
    busStopTimesInRange = busStopTimes[notTooEarly & notTooLate].sort_values(['arrival_time'])

    # Keep a record of all arrivals at bus stops on Tuesdays within the given time range.
    busStopTimesOnTuesday = pd.merge(busStopTimesInRange, busTripsOnTuesday, on='trip_id', how='inner').drop(['index_x', 'index_y', 'route_id', 'service_id', 'express', 'tuesday'], 1)
    validBusStops = pd.merge(busStopTimesOnTuesday, busStops, on='stop_id', how='inner').drop(['index_x', 'index_y'], axis=1)

    # Grab the closest stop to the subway for each trip.
    closestBusStops = validBusStops.sort_values('distance_from_subway').groupby('trip_id', as_index=False).first()
    closestBusStops = closestBusStops.drop(['stop_lon', 'stop_lat', 'distance_from_subway'], axis=1)
    closestBusStops.columns = ['trip_id', 'closest_stop_id', 'closest_arrival_time', 'closest_stop_sequence', 'time_to_closest']
    validBusStops = pd.merge(validBusStops, closestBusStops, on='trip_id', how='inner')

    # Omit stops that precede a bus arriving at the closest stop to the subway on the trip.
    validBusStops = validBusStops[validBusStops['closest_stop_sequence'] <= validBusStops['stop_sequence']]
    
    # Calculate the total travel time once you account for the bus, and grab the fastest travel time.
    validBusStops['bus_travel_time'] = validBusStops.apply(lambda x: (datetime.strptime(x['arrival_time'], time_format) - datetime.strptime(x['closest_arrival_time'], time_format)).total_seconds() / 60, axis=1)
    validBusStops['total_travel_time'] = validBusStops['time_to_closest'] + validBusStops['bus_travel_time']
    validBusStops = validBusStops.sort_values(['total_travel_time']).groupby('stop_id', as_index=False).first()
    validBusStops.sort_values(['total_travel_time'])

    return validBusStops

def makeIsochromeMap(fileName, busFolderList, width_height, lat, lon):
    # Create a map of New York City centered on Manhattan.
    plt.figure(figsize=(36, 36), dpi=72)
    plt.title('1 - New York Transit Frequency')
    m = Basemap(resolution="h", projection="stere", width=width_height, height=width_height, lon_0=lon, lat_0=lat)
    numTrips = pd.DataFrame()

    numProcessed = 0

    subwayStops = readStops('Subway Data')

    adjustedBusStops = []

    for borough in ['Queens Data', 'Bronx Data', 'Brooklyn Data', 'SI Data', 'Manhattan Data', 'MTA Bus Data']:
        busStops, busStopTimes, busTrips, busCalendar = readBusStops([borough])

        busStops['distance_from_subway'] = 80000

        for s in subwayStops.itertuples():
            for b in busStops.itertuples():
                distance_haversine = haversine_meters(s.stop_lon, s.stop_lat, b.stop_lon, b.stop_lat)
                if distance_haversine < b.distance_from_subway:
                    busStops.set_value(b.Index, 'distance_from_subway', distance_haversine)

        busStops['time_from_subway'] = busStops['distance_from_subway'] / walk_meters_per_min

        t0 = time()
        adjustedBusStops.append(adjBusTimes(busStops.sort_values(['distance_from_subway']), busStopTimes, busTrips, busCalendar).copy())
        print(time()-t0)

    aggregateBusStops = pd.DataFrame()

    for boroughBusStops in adjustedBusStops:
        aggregateBusStops = aggregateBusStops.append(boroughBusStops)

    aggregateBusStops = aggregateBusStops.sort_values(['total_travel_time']).groupby('stop_id').first()

    dist = [15, 10, 5]
    color = ['#b3cde3', '#8c96c6', '#88419d']

    for i in range(3):
        colorNum = (0, 0, ((1 / (3 ** 3)) * ((3 ** 3)-(i**3))))
        plotSubwayStops(m, subwayStops, dist[i], color[i], 1)
        plotBusStops(m, aggregateBusStops, dist[i], color[i], 1)
    plt.savefig("img/" + fileName, facecolor='white',edgecolor='white')
    #plt.show()

makeIsochromeMap('new_york.png', [], 80000, 40.730610, -73.935242)