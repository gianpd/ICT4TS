import pymongo as pm
import pprint
from datetime import datetime
import time
"""
ICT in Transport System Laboratories
City assigned:

Torino
Torino enjoy
Portland
"""

# Mongo Client Init
client = pm.MongoClient(
    'bigdatadb.polito.it',
    ssl=True,
    authSource='carsharing',
    tlsAllowINvalidCertificates=True
)
db = client['carsharing']
db.authenticate('ictts', 'Ictts16!')

"""
=========================
PRELIMINARY DATA ANALYSIS
=========================
"""
# Collections init
active_bookings = db['ActiveBookings']
active_parkings = db['ActiveParkings']
permanent_bookings = db['PermanentBookings']
permanent_parkings = db['PermanentParkings']
enjoy_permanent_bookings = db['enjoy_PermanentBookings']
enjoy_permanent_parkings = db['enjoy_PermanentParkings']

#
# Find documents number per collection
#
print(f"Number of ActiveBookings: {active_bookings.estimated_document_count()}")
print(f"Number of ActiveParkings: {active_bookings.estimated_document_count()}")
print(f"Number of PermanentParkings: {permanent_bookings.estimated_document_count()}")
print(f"Number of PermanentParkings: {permanent_parkings.estimated_document_count()}")
print()

#
# Find cities in the system
# TODO Ask if the system is only the Car2Go one or also the Enjoy one
cities = active_bookings.distinct('city')
cities += [x for x in active_parkings.distinct('city') if x not in cities]
cities += [x for x in permanent_parkings.distinct('city') if x not in cities]
cities += [x for x in permanent_bookings.distinct('city') if x not in cities]
print(f"Cities in the system:\n{cities}")
print()

#
# Get starting and ending time
# TODO Ask if the ending time can be taken from 'init_time' or 'final_time'
start = permanent_parkings.find().sort('init_time',1)[0]['init_time']
start_date = datetime.fromtimestamp(start)
end = permanent_parkings.find().sort('init_time',-1)[0]['init_time']
end_date = datetime.fromtimestamp(end)
print(f"System started at:\n{start} Unix Timestamp -> {start_date} GMT+01:00")
print(f"System ended at:\n{end} Unix Timestamp -> {end_date} GMT+01:00")
print()

#
# Number of car per city
#
torino = permanent_parkings.find({'city':'Torino'})
torino_cars = torino.distinct('plate')
print(f"Torino, Car2Go: {len(torino_cars)} cars")

portland = permanent_parkings.find({'city':'Portland'})
portland_cars = portland.distinct('plate')
print(f"Portland, Car2Go: {len(portland_cars)} cars")

torino_e = enjoy_permanent_parkings.find({'city':'Torino'})
torino_e_cars = torino_e.distinct('plate')
print(f"Torino, Enjoy: {len(torino_e_cars)} cars")
print()

#
# Number of cars booked in December 2017 per city
# find 2017-12-1 <= date < 2018-1-1
start = time.mktime(time.strptime('2017-12-1','%Y-%m-%d'))
end = time.mktime(time.strptime('2018-1-1','%Y-%m-%d'))

torino = permanent_bookings.find({
    'city':'Torino',
    'init_time':{
        '$gte':start,
        '$lt':end
    }
})
torino_b_cars = torino.distinct('plate')
print(f"Torino, Car2Go: {len(torino_b_cars)} cars booked")

portland = permanent_bookings.find({
    'city':'Portland',
    'init_time':{
        '$gte':start,
        '$lt':end
    }
})
portland_b_cars = portland.distinct('plate')
print(f"Portland, Car2Go: {len(portland_b_cars)} cars booked")

torino_e = enjoy_permanent_bookings.find({
    'city':'Torino',
    'init_time':{
        '$gte':start,
        '$lt':end
    }
})
torino_eb_cars = torino_e.distinct('plate')
print(f"Torino, Enjoy: {len(torino_eb_cars)} cars booked")
print()

#
# Number of cars booked in December 2017 per city
# find 2017-12-1 <= date < 2018-1-1
start = time.mktime(time.strptime('2017-12-1','%Y-%m-%d'))
end = time.mktime(time.strptime('2018-1-1','%Y-%m-%d'))

torino = permanent_bookings.find({
    'city':'Torino',
    'init_time':{
        '$gte':start,
        '$lt':end
    },
    'public_transport':{
        'duration':{'$ne':-1}
    },
    'walking':{
        'duration':{'$ne':-1}
    }
})
torino_b_cars = torino.distinct('plate')
print(f"Torino, Car2Go: {len(torino_b_cars)} cars booked")

portland = permanent_bookings.find({
    'city':'Portland',
    'init_time':{
        '$gte':start,
        '$lt':end
    },
    'public_transport':{
        'duration':{'$ne':-1}
    },
    'walking':{
        'duration':{'$ne':-1}
    }
})
portland_b_cars = portland.distinct('plate')
print(f"Portland, Car2Go: {len(portland_b_cars)} cars booked")

torino_e = enjoy_permanent_bookings.find({
    'city':'Torino',
    'init_time':{
        '$gte':start,
        '$lt':end
    },
    'public_transport':{
        'duration':{'$ne':-1}
    },
    'walking':{
        'duration':{'$ne':-1}
    }
})
torino_eb_cars = torino_e.distinct('plate')
print(f"Torino, Enjoy: {len(torino_eb_cars)} cars booked")