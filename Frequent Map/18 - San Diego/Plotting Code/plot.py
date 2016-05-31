import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
import numpy as np
import pandas as pd
import os.path
import datetime
import collections

lists = collections.namedtuple('List',['shapes','trips','stopTimes','calendar','frequencies'])

# Create a map of New York City centered on Manhattan.
plt.figure(figsize=(12, 12), dpi=72)
plt.title('18 - San Diego Transit Frequency')
map = Basemap(resolution="h", projection="stere", width=80000, height=80000, lon_0=-117.1611
, lat_0=32.7157)

# Calculate base trips.
base_directions = 2
base_minhour = 7
base_maxhour = 19
base_mintime = '07:00:00'
base_maxtime = '19:00:00'
base_maxheadway = 10
base_days = 7
base_trips = base_days * base_directions * (base_maxhour - base_minhour) * (60 / base_maxheadway)

min_draw_size = 1

shapeData = ['shape_id','shape_pt_lon','shape_pt_lat']
routeData = ['route_id', 'service_id', 'shape_id', 'trip_id']
timeData = ['arrival_time', 'departure_time', 'trip_id']
stopData = ['stop_lon', 'stop_lat']
calendarData = ['service_id','monday','tuesday','wednesday','thursday','friday','saturday','sunday']

def getData(folderList, shapes, trips, stopTimes, calendar, frequencies):
    for folder in folderList:
        print('Adding data from ' + folder + '.')

        # Read the files from the data.
        readShapes = pd.read_csv('../' + folder + '/shapes.txt')[shapeData]
        readTrips = pd.read_csv('../' + folder + '/trips.txt')[routeData]
        readStopTimes = pd.read_csv('../' + folder + '/stop_times.txt')[timeData]
        readCalendar = pd.read_csv('../' + folder + '/calendar.txt')[calendarData]

        # Append it to the existing data.
        shapes = pd.concat([shapes, readShapes])
        trips = pd.concat([trips, readTrips])
        stopTimes = pd.concat([stopTimes, readStopTimes])
        calendar = pd.concat([calendar, readCalendar])

        if os.path.isfile('../' + folder + '/frequencies.txt'):
            readFrequencies = pd.read_csv('../' + folder + '/frequencies.txt')
            frequencies = pd.concat([frequencies, readFrequencies])

         # Calculate the number of missing shapes.
        num_shapes = trips.groupby('route_id').size()
        num_validshapes = trips[trips.shape_id.isin(shapes.shape_id)].groupby('route_id').size()
        num_missingshapes = num_shapes - num_validshapes
        percent_missingshapes = num_missingshapes / num_shapes * 100
        print('Missing data from ' + ', '.join(folderList) + ':')
        num_missingshapesList = num_missingshapes[num_missingshapes != 0]
        if num_missingshapes.empty:
            print(num_missingshapes[num_missingshapes != 0])
            print(percent_missingshapes[percent_missingshapes != 0])
        else:
            print('No data missing.\n')

    return lists(shapes, trips, stopTimes, calendar, frequencies)

def getNumTrips(trips, stopTimes, calendar, frequencies):
    validFreq = pd.DataFrame()

    # Grab the number of trips made outside min and max hour.
    tooEarly = stopTimes['arrival_time'] < base_mintime
    tooLate = stopTimes['departure_time'] > base_maxtime
    invalidTrips = stopTimes[(tooEarly | tooLate)].groupby('trip_id').size()

    # Add frequency information.
    if not frequencies.empty:
        # Filter out frequencies that fall outside the given time.
        freq_tooEarly = frequencies['end_time'] < base_mintime
        freq_tooLate = frequencies['start_time'] > base_maxtime
        validFreq = frequencies[~(freq_tooEarly | freq_tooLate)]

        # Only consider valid timeframes.
        validFreq.loc[validFreq.start_time < base_mintime, 'start_time'] = base_mintime
        validFreq.loc[validFreq.end_time > base_maxtime, 'end_time'] = base_maxtime
        freq_noTime = validFreq['start_time'] == validFreq['end_time']
        validFreq = validFreq[~freq_noTime]

        # Calculate the amount of time left.
        validFreq['trips']=pd.to_timedelta(pd.to_datetime(validFreq['end_time'], format='%H:%M:%S')-pd.to_datetime(validFreq['start_time'], format='%H:%M:%S')).dt.seconds / validFreq['headway_secs']
        validFreq = validFreq[['trip_id','trips']].groupby('trip_id', as_index = False).sum()

        # Remove invalid trips that have valid entries in frequencies.txt.
        in_freq = invalidTrips.index.isin(validFreq.index)
        print(invalidTrips[in_freq])
        invalidTrips = invalidTrips[~in_freq]

    # Filter out the invalid trips.
    in_validTrips = ~trips.trip_id.isin(invalidTrips.index)
    validTrips = trips[in_validTrips]
    validTrips['trips']=1

    if not validFreq.empty:
        # Go through the items in validFreq and update validTrips.trips.
        for row in validFreq.itertuples():
            validTrips.loc[validTrips.trip_id == row.trip_id, 'trips'] = row.trips

    # Find the amount of days any given service pattern runs.
    calendar['numDays'] = calendar.drop('service_id', 1).sum(axis=1)
    calendar = calendar[['service_id', 'numDays']]

    # Add this information to each trip.
    numTrips = pd.merge(validTrips, calendar, on='service_id', how='inner')
    numTrips['totalTrips'] = numTrips['numDays'] * numTrips['trips']
    numTrips = numTrips.groupby('shape_id', as_index=False).sum()    
    return numTrips

def plotData(m, folderList, minsize):
    shapes = pd.DataFrame()
    trips = pd.DataFrame()
    stopTimes = pd.DataFrame()
    calendar = pd.DataFrame()
    frequencies = pd.DataFrame()

    shapes, trips, stopTimes, calendar, frequencies = getData(folderList, shapes, trips, stopTimes, calendar, frequencies)

    numTrips = getNumTrips(trips, stopTimes, calendar, frequencies)

    # Map the routes, with transparency dependent on frequency.
    for row in numTrips.itertuples():
        shape_id = row.shape_id
        num_trips = row.totalTrips
        currentShape = shapes[shapes['shape_id'] == shape_id]
        transp = (num_trips / base_trips) ** 2
        if (transp > 1):
            transp = 1
        m.plot(currentShape['shape_pt_lon'].values, currentShape['shape_pt_lat'].values, color='black', latlon=True, linewidth=minsize, alpha=transp)

plotData(map, ['NCTD Data'], min_draw_size)

# calendar.txt is empty
plotData(map, ['MTS Data'], min_draw_size)

plt.savefig('map18.png')
plt.show()