
def pip_hmt(date_start, date_end, city='Torino'):
    """query for retrieving how many bookings/parkings were doing in a specific time"""

    pipeline = [
    {"$match": {"$and": [
    {"city": city}, {"init_date": {"$gte": date_start, "$lte": date_end} } ] }
    },
    {"$group": {
        "_id": {"city": "$city"},
        "plate": {"$addToSet": "$plate"}}
    },
     {
         "$unwind": "$plate"
     },
    {
        "$group": {"_id": "$_id", "Count": {"$sum": 1}}
    }

    ]

    return pipeline


def pip_d(city="Torino"):
    """pipeline for querying how much time bookings/Parkings take: duration contains the booking/parking time for each
    car"""

    pipeline = [
        {"$match": {"city": city}},
        {"$project": {
            "_id": 0,
            "init_date": 1,
            "duration": {"$subtract": ['$final_time', '$init_time']},
        }
        }
        #{"$sort": {"duration": 1}}, this operation takes too much time


    ]

    return pipeline


def pip_uot(start_timestamp, end_timestamp, city='Torino'):
    """pipeline Utilization Over Time: total contains the total number of booking/parking for each hour"""

    pipeline = [
        {
            '$match': {
                'city': city,
                'init_time': {
                    '$gte': start_timestamp,
                    '$lte': end_timestamp
                }
            }
        }, {
            "$project":
                {'_id': 1,
                 "dd": {"$dayOfWeek": "$init_date"},
                 "hh": {"$hour": "$init_date"},
                 "init_date": 1,
                 }
        },
        {
            '$group': {
                '_id': {'day': '$dd', 'hour': '$hh'},
                'total': {'$sum': 1},

            }
        },

        {
            '$sort': {'_id': 1}
        }
    ]

    return pipeline


def pip_uotF(start_timestamp, end_timestamp, city='Torino'):
    """pipeline Utilization Over Time with filter: total contains the total number of booking/parking for each hour,
    by taking into account only the cars that really were used."""

    pipeline = [
            {
            '$match': {
                'city': city,
                'init_time': {
                    '$gte': start_timestamp,
                    '$lte': end_timestamp
                }
            }
        }, {
            "$project":
                {'_id': 1,
                 "dd": {"$dayOfWeek": "$init_date"},
                 "hh": {"$hour": "$init_date"},
                 "init_date": 1,
                 "total_fuel": {'$subtract': ['$final_fuel', '$init_fuel']}
                }
        },
        {
            '$match': {'total_fuel': {'$ne': 0}}
        },
        {
            '$group': {
                '_id': {'day': '$dd', 'hour': '$hh'},
                'total': {'$sum': 1}

            }},


            {
                '$sort': {'_id': 1}
            }
        ]

    return pipeline



def get_series(data_ls):


    pass
