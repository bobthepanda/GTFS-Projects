import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
from time import time, sleep
import numpy as np
import pandas as pd
import os.path
import datetime
import collections

lists = collections.namedtuple('List',['shapes','trips','stopTimes','calendar'])

# This data allows you to configure what the bar for frequency is.
base_directions = 2

# The minimum and maximum hour range of trips to include in 24h time, where 0 is midnight.
base_minhour = 9
base_maxhour = 12 + 7

# The tiers of headways you want to represent.
base_maxheadway = 10
second_maxheadway = 15
third_maxheadway = 30

# The days of the week to include. Must be in Mon-Sun order. Start date must be a Monday;
# for best results, use a Monday on a week with no holidays or special service conditions.
start_date = 20161002
daysOfTheWeek = ['monday','tuesday','wednesday','thursday','friday', 'saturday', 'sunday']

base_minhour = base_minhour % 24
base_maxhour = base_maxhour % 24
while base_maxhour <= base_minhour:
    base_maxhour += 24

base_hourspan = base_maxhour - base_minhour - 1

base_trips = len(daysOfTheWeek) * (base_hourspan) * (60 / base_maxheadway)

min_draw_size = 8

shapeData = ['shape_id','shape_pt_lon','shape_pt_lat']
routeData = ['route_id', 'service_id', 'shape_id', 'trip_id']
timeData = ['arrival_time', 'departure_time', 'trip_id']
stopData = ['stop_lon', 'stop_lat']
calendarData = ['service_id'] + daysOfTheWeek
calendarDateData = ['service_id', 'date']

def getData(folder, shapes, trips, stopTimes, calendar):
    print('Adding data from ' + folder + '.')

    # Read the files from the data.
    readShapes = pd.read_csv('../' + folder + '/shapes.txt')[shapeData]
    readTrips = pd.read_csv('../' + folder + '/trips.txt')[routeData]
    readStopTimes = pd.read_csv('../' + folder + '/stop_times.txt')[timeData]
    readCalendar = pd.DataFrame()
    if os.path.isfile('../' + folder + '/calendar.txt'):
        readCalendar = pd.read_csv('../' + folder + '/calendar.txt')[calendarData]
    else:
        readCalendarDates = pd.read_csv('../' + folder + '/calendar_dates.txt')[calendarDateData]
        calendarDates = readCalendarDates[(readCalendarDates.date >= start_date) & (readCalendarDates.date <= (start_date + 6))]
        readCalendar = calendarDates.groupby(['service_id']).first().reset_index()[['service_id']]
        for day in daysOfTheWeek:
            readCalendar['day'] = 0

        dayNum = 0
        while dayNum < len(daysOfTheWeek):
            currentDay = calendarDates[(calendarDates.date == (start_date + dayNum))]
            for row in readCalendar.itertuples():
                readCalendar.set_value(row.Index, daysOfTheWeek[dayNum], len(currentDay[(currentDay.service_id == row.service_id)].index))
            dayNum +=1

    # Append it to the existing data.
    shapes = pd.concat([shapes, readShapes])
    trips = pd.concat([trips, readTrips])
    stopTimes = pd.concat([stopTimes, readStopTimes])
    calendar = pd.concat([calendar, readCalendar])

     # Calculate the number of missing shapes.
    num_shapes = trips.groupby('route_id').size()
    num_validshapes = trips[trips.shape_id.isin(shapes.shape_id)].groupby('route_id').size()
    num_missingshapes = num_shapes - num_validshapes
    percent_missingshapes = num_missingshapes / num_shapes * 100
    print('Missing data from ' + folder + ':')
    num_missingshapesList = num_missingshapes[num_missingshapes != 0]
    if num_missingshapes.empty:
        print(num_missingshapes[num_missingshapes != 0])
        print(percent_missingshapes[percent_missingshapes != 0])
    else:
        print('No data missing.\n')

    return lists(shapes, trips, stopTimes, calendar)

def getNumTrips(folder, trips, stopTimes, calendar):
    validFreq = pd.DataFrame()

    # Only grab the first stop for every trip.
    smallStopTimes = stopTimes.sort_values(['arrival_time']).groupby(['trip_id']).first().reset_index()[['trip_id', 'arrival_time']]
    invalidTrips = pd.DataFrame()

    # Grab the trips that are made outside the min and max times.
    tooEarly = smallStopTimes['arrival_time'] < '{0:0>2}:00:00'.format(base_minhour)
    tooLate = smallStopTimes['arrival_time'] > '{0:0>2}:00:00'.format(base_maxhour)
    if base_minhour < base_maxhour:
        invalidTrips = smallStopTimes[(tooEarly | tooLate)]
    else:
        invalidTrips = smallStopTimes[(tooEarly & tooLate)]

    # Filter out the invalid trips.
    in_validTrips = trips['trip_id'].isin(invalidTrips['trip_id'])
    validTrips = trips[~in_validTrips]

    # Get the valid trips and their times and service days.
    validTripTimes = pd.merge(pd.merge(validTrips, calendar, on='service_id', how='inner'), smallStopTimes, on='trip_id', how='inner')

    # Start a DataFrame with all the shape_ids.
    numTrips = validTripTimes.groupby(['shape_id']).first().reset_index()[['route_id', 'shape_id']]
    numTrips['max_headway'] = 121
    numTrips['max_route_headway'] = 121
    numTrips['max_route_weekday_headway'] = 121

    last_route_id = "null"
    max_route_headway = -1.0
    max_route_weekday_headway = -1.0
    for row in numTrips.itertuples():
        currentTrips = validTripTimes[(validTripTimes.shape_id == row.shape_id)]
        currentRouteTrips = validTripTimes[(validTripTimes.route_id == row.route_id)]
        # print('Row data for ' + row.shape_id + ', ' + row.route_id)
        # print(currentRouteTrips.sort_values(['sunday', 'arrival_time']))
        max_headway = -1

        for currentHour in range(base_minhour, base_maxhour):
            beginHourString = '{0:0>2}:00:00'.format(currentHour % 24)
            endHourString = '{0:0>2}:00:00'.format((currentHour % 24) +1)

            currentTripsHour = currentTrips[(currentTrips.arrival_time >= beginHourString) & (currentTrips.arrival_time < endHourString)]

            if currentHour > 23:
                beginHourString = '{0:0>2}:00:00'.format(currentHour)
                endHourString = '{0:0>2}:00:00'.format((currentHour) +1)

                currentTripsHour = currentTripsHour.append(currentTrips[(currentTrips.arrival_time >= beginHourString) & (currentTrips.arrival_time < endHourString)])

            for day in daysOfTheWeek:
                currentTripsNum = currentTripsHour[[day, 'arrival_time']].iloc[:,0].sum()
                if currentTripsNum != 0:
                    headway = (60 * base_directions) / currentTripsNum
                    if headway > max_headway:
                        max_headway = headway
                else:
                    max_headway = 121

        if row.route_id != last_route_id:
            max_route_headway = -1.0
            max_route_weekday_headway = -1.0
            for currentHour in range(base_minhour, base_maxhour):
                beginHourString = '{0:0>2}:00:00'.format(currentHour % 24)
                endHourString = '{0:0>2}:00:00'.format((currentHour % 24) +1)

                currentRouteTripsHour = currentRouteTrips[(currentRouteTrips.arrival_time >= beginHourString) & (currentRouteTrips.arrival_time < endHourString)]

                if currentHour > 23:
                    beginHourString = '{0:0>2}:00:00'.format(currentHour)
                    endHourString = '{0:0>2}:00:00'.format((currentHour) +1)

                    currentRouteTripsHour = currentRouteTripsHour.append(currentRouteTrips[(currentRouteTrips.arrival_time >= beginHourString) & (currentRouteTrips.arrival_time < endHourString)])

                for day in daysOfTheWeek:
                    currentRouteTripsNum = currentRouteTripsHour[[day, 'arrival_time']].iloc[:,0].sum()
                    if currentRouteTripsNum != 0:
                        route_headway = (60 * base_directions) / currentRouteTripsNum
                        if route_headway > max_route_headway:
                            max_route_headway = route_headway
                        if day in ['monday', 'tuesday', 'wednesday', 'thursday', 'friday'] and route_headway > max_route_weekday_headway:
                            max_route_weekday_headway = route_headway
                    elif max_route_headway != 121:
                        max_route_headway = 121
                        if day in ['monday', 'tuesday', 'wednesday', 'thursday', 'friday']:
                            max_route_weekday_headway = 121
                    else:
                        max_route_headway = 121
                        if day in ['monday', 'tuesday', 'wednesday', 'thursday', 'friday']:
                            max_route_weekday_headway = 121

        numTrips.set_value(row.Index, 'max_headway', max_headway)
        numTrips.set_value(row.Index, 'max_route_headway', max_route_headway)
        numTrips.set_value(row.Index, 'max_route_weekday_headway', max_route_weekday_headway)
        last_route_id = row.route_id

    return numTrips

def plotData(m, folder, minsize):
    shapes = pd.DataFrame()
    trips = pd.DataFrame()
    stopTimes = pd.DataFrame()
    calendar = pd.DataFrame()

    shapes, trips, stopTimes, calendar = getData(folder, shapes, trips, stopTimes, calendar)

    numTrips = getNumTrips(folder, trips, stopTimes, calendar).sort_values(['max_headway'])

    numTrips.sort_values(['route_id']).to_json(path_or_buf=(folder.replace(" ", "") + 'TripData.json'), orient='records')

    numTrips_csv = numTrips[['route_id', 'max_route_headway', 'max_route_weekday_headway']]
    numTrips_csv.groupby('route_id').first().reset_index().sort_values(['max_route_headway','max_route_weekday_headway', 'route_id']).to_csv(path_or_buf=(folder.replace(" ", "") + 'RouteData.csv'))

    plotDataOnMap(m, shapes, numTrips, minsize)

def plotDataOnMap(m, shapes, numTrips, min_draw_size):
    # Map the routes, with transparency dependent on frequency.
    for row in numTrips.itertuples():
        shape_id = row.shape_id
        headway = row.max_route_headway
        route_headway = row.max_route_headway
        currentShape = shapes[shapes['shape_id'] == shape_id]

        transp = base_maxheadway / headway
        colorNum = base_maxheadway / route_headway
        color = 'grey'
        width = 1

        if (transp >= 1):
            transp = 1
            width = 1
        elif (transp >= base_maxheadway / second_maxheadway):
            width = .5
        elif (transp >= base_maxheadway / third_maxheadway):
            width = .25
        else:
            width = .125

        if (colorNum >= 1):
            color = 'red'
        elif (colorNum >= base_maxheadway / second_maxheadway):
            color = 'yellow'
        elif (colorNum >= base_maxheadway / third_maxheadway):
            color = 'blue'

        m.plot(currentShape['shape_pt_lon'].values, currentShape['shape_pt_lat'].values, color=color, latlon=True, linewidth=min_draw_size * width, alpha=transp)

def plotStops(m, folder, min_draw_size):
    stops = pd.read_csv('../Subway Data/stops.txt')[stopData]

    # Make cute little stop circles for the stops.
    for index, row in stops.iterrows():
        m.plot(row['stop_lon'], row['stop_lat'], marker='o', markersize=min_draw_size * .5, markeredgecolor='white', markerfacecolor='black', markeredgewidth=min_draw_size * .25, latlon=True)


def makeFrequentMap(fileName, railFolderList, busFolderList, width_height, lon, lat):
    # Create a map of New York City centered on Manhattan.
    plt.figure(figsize=(36, 36), dpi=72)
    plt.title('1 - New York Transit Frequency')
    map = Basemap(resolution="h", projection="stere", width=width_height, height=width_height, lon_0=lon, lat_0=lat)

    # Plot trains.
    for folder in railFolderList:
        plotData(map, folder, min_draw_size * 2)
        plotStops(map, folder, min_draw_size * 2)

    # Plot bus routes.
    for folder in busFolderList:
        plotData(map, folder, min_draw_size)

    plt.savefig(fileName, facecolor='white',edgecolor='white')
    #plt.show()

makeFrequentMap('new_york.png', ['PATH Data', 'Subway Data', 'LIRR Data', 'Metro North Data', 'NJT Rail Data'], ['Bronx Data', 'Queens Data', 'Brooklyn Data', 'Manhattan Data', 'SI Data', 'MTA Bus Data', 'Westchester Data', 'Nassau Data', 'NJT Bus Data', 'SI Ferry Data'], 80000, -73.935242, 40.730610)

makeFrequentMap('bronx.png', ['Subway Data', 'Metro North Data'], ['Bronx Data', 'Queens Data', 'Manhattan Data', 'MTA Bus Data', 'Westchester Data', 'NJT Bus Data'], 20000, -73.886111, 40.837222)
makeFrequentMap('queens.png', ['Subway Data', 'LIRR Data'], ['Bronx Data', 'Queens Data', 'Brooklyn Data', 'Manhattan Data', 'MTA Bus Data', 'Nassau Data'], 30000, -73.866667, 40.75)
makeFrequentMap('manhattan.png', ['PATH Data', 'Subway Data', 'LIRR Data', 'Metro North Data', 'NJT Rail Data'], ['Bronx Data', 'Queens Data', 'Brooklyn Data', 'Manhattan Data', 'SI Data', 'MTA Bus Data', 'NJT Bus Data', 'SI Ferry Data'], 25000, -73.979167, 40.758611)
makeFrequentMap('brooklyn.png', ['Subway Data', 'LIRR Data'], ['Queens Data', 'Brooklyn Data', 'Manhattan Data', 'SI Data', 'MTA Bus Data', 'SI Ferry Data'], 30000, -73.952222, 40.624722)
makeFrequentMap('si.png', ['Subway Data', 'NJT Rail Data'], ['SI Data', 'MTA Bus Data', 'NJT Bus Data', 'SI Ferry Data', 'NJT Bus Data'], 25000, -74.144839, 40.576281)