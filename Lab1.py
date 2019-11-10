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
enjoy_active_bookings = db['enjoy_ActiveBookings']
enjoy_active_parkings = db['enjoy_ActiveParkings']
enjoy_permanent_bookings = db['enjoy_PermanentBookings']
enjoy_permanent_parkings = db['enjoy_PermanentParkings']

#
# Find documents number per collection
# Car2Go
print("CAR2GO\n==========")
print(f"Number of ActiveBookings: {active_bookings.estimated_document_count()}")
print(f"Number of ActiveParkings: {active_parkings.estimated_document_count()}")
print(f"Number of PermanentParkings: {permanent_bookings.estimated_document_count()}")
print(f"Number of PermanentParkings: {permanent_parkings.estimated_document_count()}")
print()
# Enjoy
print("ENJOY\n==========")
print(f"Number of ActiveBookings: {enjoy_active_bookings.estimated_document_count()}")
print(f"Number of ActiveParkings: {enjoy_active_parkings.estimated_document_count()}")
print(f"Number of PermanentParkings: {enjoy_permanent_bookings.estimated_document_count()}")
print(f"Number of PermanentParkings: {enjoy_permanent_parkings.estimated_document_count()}")
print()

#
# Find cities in the system
cities = permanent_parkings.distinct('city')
print(f"Cities in the Car2Go system:\n{cities}")
cities += [x for x in enjoy_permanent_parkings.distinct('city') if x not in cities]
print(f"Cities in the Enjoy system:\n{cities}")
print()

#
# Get starting and ending time
# TODO Ask if the ending time can be taken from 'init_time' or 'final_time'
# Car2Go
start = permanent_parkings.find().sort('init_time',1)[0]['init_time']
start_date = datetime.fromtimestamp(start)
end = permanent_parkings.find().sort('init_time',-1)[0]['init_time']
end_date = datetime.fromtimestamp(end)
print("CAR2GO\n==========")
print(f"System started at:\n{start} Unix Timestamp -> {start_date} GMT+01:00")
print(f"System ended at:\n{end} Unix Timestamp -> {end_date} GMT+01:00")
print()
# Enjoy
start = enjoy_permanent_parkings.find().sort('init_time',1)[0]['init_time']
start_date = datetime.fromtimestamp(start)
end = enjoy_permanent_parkings.find().sort('init_time',-1)[0]['init_time']
end_date = datetime.fromtimestamp(end)
print("ENJOY\n==========")
print(f"System started at:\n{start} Unix Timestamp -> {start_date} GMT+01:00")
print(f"System ended at:\n{end} Unix Timestamp -> {end_date} GMT+01:00")
print()

#
# Number of car per city
#
torino = active_parkings.find({'city':'Torino'})
torino_cars = torino.distinct('plate')
print(f"Torino, Car2Go: {len(torino_cars)} currently available cars")

torino = permanent_parkings.find({'city':'Torino'})
torino_cars = torino.distinct('plate')
print(f"Torino, Car2Go: {len(torino_cars)} total available cars")

portland = active_parkings.find({'city':'Portland'})
portland_cars = portland.distinct('plate')
print(f"Portland, Car2Go: {len(portland_cars)} currently available cars")

portland = permanent_parkings.find({'city':'Portland'})
portland_cars = portland.distinct('plate')
print(f"Portland, Car2Go: {len(portland_cars)} total available cars")

torino_e = enjoy_active_parkings.find({'city':'Torino'})
torino_e_cars = torino_e.distinct('plate')
print(f"Torino, Enjoy: {len(torino_e_cars)} currently available cars")

torino_e = enjoy_permanent_parkings.find({'city':'Torino'})
torino_e_cars = torino_e.distinct('plate')
print(f"Torino, Enjoy: {len(torino_e_cars)} total available cars")
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
print(f"Torino, Car2Go: {len(torino_b_cars)} cars booked in December 2017")

portland = permanent_bookings.find({
    'city':'Portland',
    'init_time':{
        '$gte':start,
        '$lt':end
    }
})
portland_b_cars = portland.distinct('plate')
print(f"Portland, Car2Go: {len(portland_b_cars)} cars booked in December 2017 ")

torino_e = enjoy_permanent_bookings.find({
    'city':'Torino',
    'init_time':{
        '$gte':start,
        '$lt':end
    }
})
torino_eb_cars = torino_e.distinct('plate')
print(f"Torino, Enjoy: {len(torino_eb_cars)} cars booked in December 2017 ")
print()

#
# Number of cars booked in December 2017 per city with alternative 
# transport modes
# find 2017-12-1 <= date < 2018-1-1
start = time.mktime(time.strptime('2017-12-1','%Y-%m-%d'))
end = time.mktime(time.strptime('2018-1-1','%Y-%m-%d'))

torino = permanent_bookings.find({
    'city':'Torino',
    'init_time':{
        '$gte':start,
        '$lt':end
    },
    'public_transport.duration':{'$ne':-1},
    'walking.duration':{'$ne':-1}
})
torino_public = permanent_bookings.find({
    'city':'Torino',
    'init_time':{
        '$gte':start,
        '$lt':end
    },
    'public_transport.duration':{'$ne':-1},
})
torino_walking = permanent_bookings.find({
    'city':'Torino',
    'init_time':{
        '$gte':start,
        '$lt':end
    },
    'walking.duration':{'$ne':-1}
})
torino_alt_cars = torino.distinct('plate')
torino_public_cars = torino_public.distinct('plate')
torino_walking_cars = torino_walking.distinct('plate')
print(f"Torino, Car2Go: {len(torino_alt_cars)} cars booked in December 2017 with alternative modes")
print(f"\t{len(torino_public_cars)} public transport alternative")
print(f"\t{len(torino_walking_cars)} walking alternative")

portland = permanent_bookings.find({
    'city':'Portland',
    'init_time':{
        '$gte':start,
        '$lt':end
    },
    'public_transport.duration':{'$ne':-1},
    'walking.duration':{'$ne':-1}
})
portland_public = permanent_bookings.find({
    'city':'Portland',
    'init_time':{
        '$gte':start,
        '$lt':end
    },
    'public_transport.duration':{'$ne':-1},
})
portland_walking = permanent_bookings.find({
    'city':'Portland',
    'init_time':{
        '$gte':start,
        '$lt':end
    },
    'walking.duration':{'$ne':-1}
})
portland_alt_cars = portland.distinct('plate')
portland_public_cars = portland_public.distinct('plate')
portland_walking_cars = portland_walking.distinct('plate')
print(f"Portland, Car2Go: {len(portland_alt_cars)} cars booked in December 2017 with alternative modes")
print(f"\t{len(portland_public_cars)} public transport alternative")
print(f"\t{len(portland_walking_cars)} walking alternative")

torino = enjoy_permanent_bookings.find({
    'city':'Torino',
    'init_time':{
        '$gte':start,
        '$lt':end
    },
    'public_transport.duration':{'$ne':-1},
    'walking.duration':{'$ne':-1}
})
torino_public = enjoy_permanent_bookings.find({
    'city':'Torino',
    'init_time':{
        '$gte':start,
        '$lt':end
    },
    'public_transport.duration':{'$ne':-1},
})
torino_walking = enjoy_permanent_bookings.find({
    'city':'Torino',
    'init_time':{
        '$gte':start,
        '$lt':end
    },
    'walking.duration':{'$ne':-1}
})
torino_alt_cars = torino.distinct('plate')
torino_public_cars = torino_public.distinct('plate')
torino_walking_cars = torino_walking.distinct('plate')
print(f"Torino, Enjoy: {len(torino_alt_cars)} cars booked in December 2017 with alternative modes")
print(f"\t{len(torino_public_cars)} public transport alternative")
print(f"\t{len(torino_walking_cars)} walking alternative")