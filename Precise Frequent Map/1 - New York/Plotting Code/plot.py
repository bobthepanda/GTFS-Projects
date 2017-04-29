import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
import json
from descartes import PolygonPatch
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
start_date = 20170123
daysOfTheWeek = ['monday','tuesday','wednesday','thursday','friday', 'saturday', 'sunday']

base_minhour = base_minhour % 24
base_maxhour = base_maxhour % 24
while base_maxhour <= base_minhour:
    base_maxhour += 24

base_hourspan = base_maxhour - base_minhour - 1

base_trips = len(daysOfTheWeek) * (base_hourspan) * (60 / base_maxheadway)

shapeData = ['shape_id','shape_pt_lon','shape_pt_lat']
routeData = ['route_id', 'service_id', 'shape_id', 'trip_id']
timeData = ['arrival_time', 'departure_time', 'trip_id', 'stop_sequence']
stopData = ['stop_lon', 'stop_lat']
calendarData = ['service_id'] + daysOfTheWeek
calendarDateData = ['service_id', 'date']

storedShapes = {}
storedTrips = {}
storedStopTimes = {}
storedCalendar = {}
storedNumTrips = {}

def getCalendar(folder):
    ''' Process the calendar for a dataset in a given folder.
    '''

    readCalendar = pd.DataFrame()

    # Check if calendar.txt is specified.
    if os.path.isfile('../' + folder + '/calendar.txt'):
        readCalendar = pd.read_csv('../' + folder + '/calendar.txt')
        daysInWeek = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']

        # Account for extra/no service specified in calendar dates.
        if os.path.isfile('../' + folder + '/calendar_dates.txt'):
            readCalendarDates = pd.read_csv('../' + folder + '/calendar_dates.txt')
            # print(readCalendarDates['date'].sort_values().unique())
            calendarDates = readCalendarDates[(readCalendarDates.date >= start_date) & (readCalendarDates.date <= (start_date + 6))]
            dayNum = 0
            while dayNum < len(daysInWeek):
                currentDay = calendarDates[(calendarDates.date == (start_date + dayNum))]
                readCalendar.loc[readCalendar['service_id'].isin(currentDay['service_id']), daysInWeek[dayNum]] = 0
                dayNum +=1

        # Only count service during the specified timeframe.
        dayNum = 0
        while dayNum < len(daysInWeek):
            readCalendar.loc[(readCalendar['start_date'] > start_date + dayNum) | (readCalendar['end_date'] < start_date + dayNum), daysInWeek[dayNum]] = 0
            dayNum += 1

        readCalendar = readCalendar[(readCalendar['monday'] != 0) | (readCalendar['tuesday'] != 0) | (readCalendar['wednesday'] != 0) | (readCalendar['thursday'] != 0) | (readCalendar['friday'] != 0) | (readCalendar['saturday'] != 0) | (readCalendar['sunday'] != 0)]

    # If it is not specified, calendar_dates.txt is the source of truth.
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

    

    return readCalendar[calendarData]

def getData(folder, shapes, trips, stopTimes, calendar):
    ''' Get raw data from dataset in the specified folder.
    '''

    if folder in storedShapes:
        print('Retrieving stored data for ' + folder + '.\n')
        return lists(storedShapes[folder], storedTrips[folder], storedStopTimes[folder], storedCalendar[folder])
    print('Adding data from ' + folder + '.')

    # Read the files from the data.
    readShapes = pd.read_csv('../' + folder + '/shapes.txt')[shapeData]
    readTrips = pd.read_csv('../' + folder + '/trips.txt')[routeData]
    readStopTimes = pd.read_csv('../' + folder + '/stop_times.txt')
    if 'pickup_type' in readStopTimes.columns.values.tolist():
        readStopTimes = readStopTimes[readStopTimes['pickup_type'] == 0]
    readStopTimes = readStopTimes[timeData]
    readCalendar = getCalendar(folder)

    # Append it to the existing data.
    shapes = pd.concat([shapes, readShapes])
    trips = pd.concat([trips, readTrips])
    stopTimes = pd.concat([stopTimes, readStopTimes])
    calendar = pd.concat([calendar, readCalendar])

     # Calculate the number of missing shapes.
    num_shapes = trips.groupby('route_id').size()
    num_validshapes = trips[~trips.shape_id.isin(shapes.shape_id)].groupby('route_id').size()
    if not num_validshapes.empty:
        print(num_validshapes)
        print()
    else:
        print('No data missing.\n')

    storedShapes[folder] = shapes
    storedTrips[folder] = trips
    storedStopTimes[folder] = stopTimes
    storedCalendar[folder] = calendar

    return lists(shapes, trips, stopTimes, calendar)

def getNumWeekdays():
    ''' Returns the amount of weekdays specified in the configuration settings.
    '''
    days = 0
    for day in ['monday', 'tuesday', 'wednesday', 'thursday', 'friday']:
        if day in daysOfTheWeek:
            days += 1
    return days

def getNumTrips(folder, trips, stopTimes, calendar):
    ''' Calculate the number of trips for each shape and route.
    '''

    shape_ids = pd.Series(trips['shape_id'].unique())
    route_ids = pd.Series(trips['route_id'].unique())

    shape_w_route_ids = trips.groupby(['route_id', 'shape_id'], as_index = False).first()[['route_id', 'shape_id']]

    if folder in storedNumTrips:
        return storedNumTrips[folder]
    validFreq = pd.DataFrame()

    # Only grab the first stop for every trip.
    first_stop_times = stopTimes.sort_values(['stop_sequence']).groupby(['trip_id']).first().reset_index()[['trip_id', 'arrival_time']]
    invalidTrips = pd.DataFrame()

    # Grab the trips that are made outside the min and max times.
    tooEarly = first_stop_times['arrival_time'] < '{0:0>2}:00:00'.format(base_minhour)
    tooLate = first_stop_times['arrival_time'] > '{0:0>2}:00:00'.format(base_maxhour)
    if base_minhour < base_maxhour:
        invalidTrips = first_stop_times[(tooEarly | tooLate)]
    else:
        invalidTrips = first_stop_times[(tooEarly & tooLate)]

    # Filter out the invalid trips.
    in_validTrips = trips['trip_id'].isin(invalidTrips['trip_id'])
    validTrips = trips[~in_validTrips]

    # Get the valid trips and their times and service days.
    validTripTimes = pd.merge(pd.merge(validTrips, calendar, on='service_id', how='inner'), first_stop_times, on='trip_id', how='inner')

    # Start a DataFrame with all the shape_ids.
    numTripsShape = pd.DataFrame()
    numTripsRoute = pd.DataFrame()

    # Grab the selection of trips during every hour timeframe during the days.
    for currentHour in range(base_minhour, base_maxhour - 1):
        beginHourString = '{0:0>2}:00:00'.format(currentHour % 24)
        endHourString = '{0:0>2}:00:00'.format((currentHour % 24) +1)

        currentTripsHour = validTripTimes[(validTripTimes.arrival_time >= beginHourString) & (validTripTimes.arrival_time < endHourString)]

        # GTFS allows trips that go over 24:00:00, so account for those.
        if currentHour > 23:
            beginHourStringExt = '{0:0>2}:00:00'.format(currentHour)
            endHourStringExt = '{0:0>2}:00:00'.format((currentHour) +1)

            currentTripsHour = currentTripsHour.append(validTripTimes[(validTripTimes.arrival_time >= beginHourStringExt) & (validTripTimes.arrival_time < endHourStringExt)])

        for day in daysOfTheWeek:
            currentTripsToday = currentTripsHour[currentTripsHour[day] == 1]

            # Count how many trips are within the current timeframe.
            currentTripsTodayShape = currentTripsToday.groupby('shape_id').size().to_frame('size').reset_index()
            currentTripsTodayRoute = currentTripsToday.groupby('route_id').size().to_frame('route_size').reset_index()

            currentTripsTodayShape['day'] = day
            currentTripsTodayShape['start_time'] = beginHourString

            currentTripsTodayRoute['route_day'] = day
            currentTripsTodayRoute['route_start_time'] = beginHourString

            # Add it to the list of known trip numbers.
            numTripsShape = numTripsShape.append(currentTripsTodayShape.copy())
            numTripsRoute = numTripsRoute.append(currentTripsTodayRoute.copy())

            # See how many shapes and routes are not served at all during the current hour and day.
            partial_shapes = shape_ids[~shape_ids.isin(currentTripsTodayShape['shape_id'])]
            partial_routes = route_ids[~route_ids.isin(currentTripsTodayRoute['route_id'])]

            partial_shapes_df = pd.DataFrame(partial_shapes, columns=['shape_id'])
            partial_shapes_df['size'] = 0
            partial_shapes_df['day'] = day
            partial_shapes_df['start_time'] = beginHourString

            partial_routes_df = pd.DataFrame(partial_routes, columns=['route_id'])
            partial_routes_df['route_size'] = 0
            partial_routes_df['route_day'] = day
            partial_routes_df['route_start_time'] = beginHourString

            # Add null trips for these shapes and routes
            numTripsShape = numTripsShape.append(partial_shapes_df.copy())
            numTripsRoute = numTripsRoute.append(partial_routes_df.copy())

    # Grab only the route trip numbers for weekdays.
    numTripsRouteWeekday = numTripsRoute[numTripsRoute['route_day'].apply(lambda x: x in ['monday', 'tuesday', 'wednesday', 'thursday', 'friday'])]

    # Find the lowest number of trips run on a given shape or route during the given time frame.
    numTripsFinalShape = pd.merge(numTripsShape.sort_values('size').groupby('shape_id').first().reset_index(), shape_w_route_ids, on='shape_id', how='inner')
    numTripsFinalRoute = numTripsRoute.sort_values('route_size').groupby('route_id').first().reset_index()
    numTripsFinalRouteWeekday = numTripsRouteWeekday.sort_values('route_size').groupby('route_id').first().reset_index()

    numTripsFinalRouteWeekday.rename(columns={'route_size':'route_weekday_size'}, inplace=True)

    # Consolidate all information about shapes, routes, and routes on weekdays.
    numTrips = pd.merge(pd.merge(numTripsFinalShape, numTripsFinalRoute, on='route_id', how='inner'), numTripsFinalRouteWeekday, on='route_id', how='inner')

    # Calculate headways.
    numTrips['max_headway'] = numTrips['size'].apply(lambda x: (60 * base_directions) / x if x != 0 else 121)
    numTrips['max_route_headway'] = numTrips['route_size'].apply(lambda x: (60 * base_directions) / x if x != 0 else 121)
    numTrips['max_route_weekday_headway'] = numTrips['route_weekday_size'].apply(lambda x: (60 * base_directions) / x if x != 0 else 121)
    numTrips['headway_tier'] = numTrips['max_route_headway'].apply(lambda x: baseline_headway(x))
    numTrips['weekday_headway_tier'] = numTrips['max_route_weekday_headway'].apply(lambda x: baseline_headway(x))

    return numTrips

def baseline_headway(x):
    ''' Set the baseline headway,
    '''
    x = round(x)
    if x <= base_maxheadway:
        return base_maxheadway
    elif x <= second_maxheadway:
        return second_maxheadway
    elif x <= third_maxheadway:
        return third_maxheadway
    elif x != 121:
        return 60
    else:
        return x

def plotData(m, folder, minsize):
    ''' For the given folder, plot onto m using the specified minsize.
    '''

    shapes = pd.DataFrame()
    trips = pd.DataFrame()
    stopTimes = pd.DataFrame()
    calendar = pd.DataFrame()

    shapes, trips, stopTimes, calendar = getData(folder, shapes, trips, stopTimes, calendar)

    numTrips = getNumTrips(folder, trips, stopTimes, calendar)

    os.makedirs("json",exist_ok=True)
    numTrips.sort_values(['route_id']).to_json(path_or_buf=("json/" + folder.replace(" ", "") + 'TripData.json'), orient='records')

    os.makedirs("csv",exist_ok=True)
    numTrips_csv = numTrips[['route_id', 'headway_tier', 'weekday_headway_tier', 'max_route_headway', 'max_route_weekday_headway']].groupby('route_id').first().reset_index().round(2)
    numTrips_csv.sort_values(['headway_tier', 'weekday_headway_tier', 'route_id']).to_csv(path_or_buf=("csv/" + folder.replace(" ", "") + 'RouteData.csv'))
    plotDataOnMap(m, shapes, numTrips, minsize)

    return numTrips_csv

def plotDataOnMap(m, shapes, numTrips, min_draw_size):
    ''' Actually draw on the map using computed numTrips data.
    '''

    # Map the routes, with transparency dependent on frequency.
    for row in numTrips.itertuples():
        shape_id = row.shape_id
        headway = row.max_headway
        route_headway = row.max_route_headway
        currentShape = shapes[shapes['shape_id'] == shape_id]

        base_transp = 1
        transp = base_maxheadway / headway
        width = base_maxheadway / route_headway
        color = 'black'

        if (transp >= 1):
            transp = 1

        if width >= 1:
            width = 1
        elif (width >= base_maxheadway / second_maxheadway):
            width = .5
        elif (width >= base_maxheadway / third_maxheadway):
            width = .25
        elif (width >= base_maxheadway / 60):
            width = .125
        else:
            width = .01

        m.plot(currentShape['shape_pt_lon'].values, currentShape['shape_pt_lat'].values, color=color, latlon=True, linewidth=min_draw_size * width, alpha=base_transp * transp)

def plotStops(m, folder, min_draw_size):
    ''' Plot the stops from a dataset in a folder at the min_draw_size.
    '''
    stops = pd.read_csv('../' + folder + '/stops.txt')[stopData]

    # Make cute little stop circles for the stops.
    for index, row in stops.iterrows():
        m.plot(row['stop_lon'], row['stop_lat'], marker='o', markersize=min_draw_size * .5, markeredgecolor='white', markerfacecolor='black', markeredgewidth=min_draw_size * .125, alpha=.5,latlon=True)


def makeFrequentMap(fileName, min_draw_size, railFolderList, busFolderList, width_height, lat, lon):
    ''' Generate a frequent map. '''

    # Create a map of New York City centered on Manhattan.
    fig = plt.figure(figsize=(36, 36), dpi=72)
    plt.title('1 - New York Transit Frequency')
    m = Basemap(resolution="h", projection="stere", width=width_height, height=width_height, lon_0=lon, lat_0=lat)
    # m.drawcounties()
    numTrips = pd.DataFrame()
    m.readshapefile('../Shapefiles/output_wgs84', 'nybb')

    numProcessed = 0

    # Plot trains.
    for folder in railFolderList:
        if numProcessed > 0:
            print('-' * 50 + '\n')
        numTrips = numTrips.append(plotData(m, folder, min_draw_size * 2))
        numProcessed += 1

    # Plot bus routes.
    for folder in busFolderList:
        if numProcessed > 0:
            print('-' * 50 + '\n')
        numTrips = numTrips.append(plotData(m, folder, min_draw_size))
        numProcessed += 1

    for folder in railFolderList:
        plotStops(m, folder, min_draw_size * 2)

    os.makedirs("img",exist_ok=True)
    plt.savefig("img/" + fileName, facecolor='white',edgecolor='white')
    print('=' * 50 + '\n')
    #plt.show()

# makeFrequentMap('test.svg', 2, [], ['NJT Bus Data'], 80000, 40.730610, -73.935242)

makeFrequentMap('new_york.svg', 4, ['PATH Data', 'Subway Data', 'LIRR Data', 'Metro North Data', 'NJT Rail Data'], ['Bronx Data', 'Queens Data', 'Brooklyn Data', 'Manhattan Data', 'SI Data', 'MTA Bus Data', 'Westchester Data', 'Nassau Data', 'NJT Bus Data', 'SI Ferry Data'], 80000, 40.730610, -73.935242)

makeFrequentMap('bronx.svg', 8, ['Subway Data', 'Metro North Data'], ['Bronx Data', 'Queens Data', 'Manhattan Data', 'MTA Bus Data', 'Westchester Data'], 20000, 40.837222, -73.886111)
makeFrequentMap('queens.svg', 8, ['Subway Data', 'LIRR Data'], ['Bronx Data', 'Queens Data', 'Brooklyn Data', 'Manhattan Data', 'MTA Bus Data', 'Nassau Data'], 26000, 40.680459, -73.843703)
makeFrequentMap('manhattan.svg', 8, ['PATH Data', 'Subway Data', 'LIRR Data', 'Metro North Data'], ['Bronx Data', 'Queens Data', 'Brooklyn Data', 'Manhattan Data', 'SI Data', 'MTA Bus Data', 'SI Ferry Data'], 25000, 40.758611, -73.979167)
makeFrequentMap('brooklyn.svg', 8, ['Subway Data', 'LIRR Data'], ['Queens Data', 'Brooklyn Data', 'Manhattan Data', 'SI Data', 'MTA Bus Data', 'SI Ferry Data'], 15000, 40.631111, -73.9525)
makeFrequentMap('si.svg', 8, ['Subway Data'], ['SI Data', 'MTA Bus Data', 'SI Ferry Data'], 25000, 40.576281, -74.144839)