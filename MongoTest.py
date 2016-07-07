import pymongo
import unittest
#from test import testdata
#import base
#import sixfeet
#import datetime

MONGO_HOST = "172.19.131.110"
TIMEOUT_MS = 3000

class MongoDB():
    @classmethod
    def connect(self):
        client = pymongo.MongoClient(
            MONGO_HOST,
            serverSelectionTimeoutMS=TIMEOUT_MS,
            connectTimeoutMS=TIMEOUT_MS
        )
        client.server_info() # Force connection

        #self.db = self.client.get_database(mongo_config['tripsDBName'])
        return client

    @classmethod
    def insert_many(self, collection, items, ignore_duplicates=False):
        """
        Inserts the given items into the database

        :param items: List of items to insert
        :param ignore_duplicates: Whether to swallow errors from duplicate inserted items
        :return:
        """

        try:
            ret = collection.insert_many(items, ordered=False)
        except pymongo.errors.BulkWriteError as e:
            dupe_indices = set([err['index'] for err in e.details['writeErrors'] if err['code'] == 11000])
            if e.details['nInserted'] + len(dupe_indices) != len(items) or not ignore_duplicates:
                raise

            ret = pymongo.results.InsertManyResult([items[i]['_id'] for i in range(len(items)) if i not in dupe_indices], False)

        return ret


#
# Representation in code of the trip objects.
#

def get_aqi_data_point(mins_offset):
    import random
    return {
        'mins': int(mins_offset),
        'pm25': random.randrange(50, 500) / 10,
        'pm25cnt': random.randrange(20, 13000),
        'pm10': random.randrange(50, 500) / 10,
        'rhumid': random.randrange(20, 80),
        'temp': random.randrange(-3, 17)
    }

def get_aqi_data(format):
    dataPts = [get_aqi_data_point(x) for x in range(0, 1440, 5)]

    # First format is as suggested by Liujun
    if format == 1:
        data = {
            'pm25': {},
            'pm25cnt': {},
            'pm10': {},
            'rhumid': {},
            'temp': {}
        }

        for pt in dataPts:
            data['pm25'][str(pt['mins'])] = pt['pm25']
            data['pm25cnt'][str(pt['mins'])] = pt['pm25cnt']
            data['pm10'][str(pt['mins'])] = pt['pm10']
            data['rhumid'][str(pt['mins'])] = pt['rhumid']
            data['temp'][str(pt['mins'])] = pt['temp']

        return data

    elif format == 2:
        data = {
            'fields': ['mins', 'pm25', 'pm25cnt', 'pm10', 'rhumid', 'temp'],
            'nums': []
        }

        nums = []
        for pt in dataPts:
            nums.append([pt['mins'], pt['pm25'], pt['pm25cnt'], pt['pm10'], pt['rhumid'], pt['temp']])

        data['nums'] = nums
        return data

    # Like #2, except all values are stuffed into one giant array
    elif format == 3:
        data = {
            'fields': ['mins', 'pm25', 'pm25cnt', 'pm10', 'rhumid', 'temp'],
            'nums': []
        }

        nums = []
        for pt in dataPts:
            nums.extend([pt['mins'], pt['pm25'], pt['pm25cnt'], pt['pm10'], pt['rhumid'], pt['temp']])

        data['nums'] = nums
        return data

    # Each type of data gets its very own array
    elif format == 4:
        data = {
            'mins': [],
            'pm25': [],
            'pm25cnt': [],
            'pm10': [],
            'rhumid': [],
            'temp': []
        }

        nums = []
        for pt in dataPts:
            data['mins'].append(pt['mins'])
            data['pm25'].append(pt['pm25'])
            data['pm25cnt'].append(pt['pm25cnt'])
            data['pm10'].append(pt['pm10'])
            data['rhumid'].append(pt['rhumid'])
            data['temp'].append(pt['temp'])

        return data

    else:
        raise Exception("Don't know what format you're talking about")

def get_aqi_record(date, serial, format):
    import datetime

    return {
        '_id': serial + "#" + date.isoformat(),
        'serial': serial,
        'timestamp': date,
        'location': [42.43843, -121.38239],
        'data': get_aqi_data(format)
    }

def get_sensor_id(n):
    return "ser{0:06}".format(n + 1)

def get_aqi_records(sensor_count, days_count, format):
    import datetime

    START_DATE = datetime.datetime(2016, 7, 7)

    recs = []

    for d in range(days_count):
        myDate = START_DATE + datetime.timedelta(days=d)
        for n in range(sensor_count):
            ser = get_sensor_id(n)
            recs.append(get_aqi_record(myDate, ser, format))

    return recs


if __name__ == '__main__':
    client = MongoDB.connect()

    print("Connected.")

    SENSOR_COUNT = 10
    DAYS_COUNT = 10
    BATCH_SIZE = 500
    FORMAT_TYPE = 1

    db = client.get_database('storeTest')
    coll = db['sensorData_{0}'.format(FORMAT_TYPE)]

    data = get_aqi_records(SENSOR_COUNT, DAYS_COUNT, FORMAT_TYPE)

    print("Writing {0} records...".format(len(data)))

    #result = coll.bulk_write([pymongo.UpdateOne({'_id': 'ser000001#2016-07-07T00:00:00'}, {'$set': {'test2': [42.0]}})], ordered=False)
    result = coll.bulk_write([pymongo.UpdateOne({'_id': 'ser000001#2016-07-07T00:00:00'}, {'$inc': {'test2': [44.0]}})],
                             ordered=False)
    print(result.__dict__)

    # inserted_count = 0
    # for x in range(0, len(data), BATCH_SIZE):
    #     stop = min(len(data), x + BATCH_SIZE)
    #     result = MongoDB.insert_many(coll, data[x:stop], ignore_duplicates=True)
    #     inserted_count += len(result.inserted_ids)
    #
    # print("Inserted {0} new records".format(inserted_count))

