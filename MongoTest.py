import pymongo
import base
import unittest
#from test import testdata
#import base
#import sixfeet
#import datetime

MONGO_HOST = "10.62.192.113"
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

        __batch_size = 500
        inserted_ids = []

        for x in range(0, len(items), __batch_size):
            items_to_insert = items[x:min(len(items), x + __batch_size)]
            try:
                ret = collection.insert_many(items_to_insert, ordered=False)
                inserted_ids.extend(ret.inserted_ids)
            except pymongo.errors.BulkWriteError as e:
                dupe_indices = set([err['index'] for err in e.details['writeErrors'] if err['code'] == 11000])
                if e.details['nInserted'] + len(dupe_indices) != len(items_to_insert) or not ignore_duplicates:
                    raise

                inserted_ids.extend([items_to_insert[i]['_id'] for i in range(len(items_to_insert)) if i not in dupe_indices])

        return pymongo.results.InsertManyResult(inserted_ids, False)



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

def get_aqi_data(format, minute_range):
    dataPts = [get_aqi_data_point(x) for x in minute_range]

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

def get_aqi_record(date, serial, format, minute_range):
    import datetime

    return {
        '_id': serial + "#" + date.isoformat(),
        'serial': serial,
        'timestamp': date,
        'location': [42.43843, -121.38239],
        'data': get_aqi_data(format, minute_range)
    }

def get_sensor_id(n):
    return "ser{0:06}".format(n + 1)

def get_aqi_records(sensor_count, days_count, format, minute_range):
    import datetime

    START_DATE = datetime.datetime(2016, 7, 7)

    recs = []

    for d in range(days_count):
        myDate = START_DATE + datetime.timedelta(days=d)
        for n in range(sensor_count):
            ser = get_sensor_id(n)
            recs.append(get_aqi_record(myDate, ser, format, minute_range))

    return recs


#
#
#
DAILY_READING_MINS_RANGE = range(0, 1440, 5)


def populate_all_at_once(coll, sensor_count, days_count, format_type):


    data = get_aqi_records(sensor_count, days_count, format_type, DAILY_READING_MINS_RANGE)

    print("Writing {0} records...".format(len(data)))

    result = MongoDB.insert_many(coll, data, ignore_duplicates=True)

    print("Inserted {0} new records".format(len(result.inserted_ids)))

def populate_time_sequential(coll, sensor_count, days_count, format_type):
    # Simulate population of data as it trickles in, sample by sample

    if format_type != 1:
        raise Exception("This function only supports FORMAT_TYPE=1")

    print("Doing initial population...")

    # Do the initial population with the first piece of data
    data = get_aqi_records(sensor_count, days_count, format_type, range(0, 1))
    result = MongoDB.insert_many(coll, data, ignore_duplicates=True)

    print("Done.  Adding more data...")

    for min in DAILY_READING_MINS_RANGE:
        data = get_aqi_records(sensor_count, days_count, format_type, range(min, min + 1))

        updates = []
        for dt in data:
            updates.append(pymongo.UpdateOne(
                {'_id': dt['_id']},
                {'$set': {
                    'data.pm25.' + str(min): dt['data']['pm25'][str(min)],
                    'data.pm25cnt.' + str(min): dt['data']['pm25cnt'][str(min)],
                    'data.pm10.' + str(min): dt['data']['pm10'][str(min)],
                    'data.rhumid.' + str(min): dt['data']['rhumid'][str(min)],
                    'data.temp.' + str(min): dt['data']['temp'][str(min)]
                }}))

        result = coll.bulk_write(updates, ordered=False)
        print(".")

    print("Done.")


if __name__ == '__main__':
    base.InitBare()
    client = MongoDB.connect()
    db = client.get_database('storeTest')

    print("Connected.")

    FORMAT_TYPE = 2
    # coll = db['sensorData_{0}_seq'.format(FORMAT_TYPE)]
    # populate_time_sequential(coll, 10, 10, FORMAT_TYPE)

    coll = db['sensorData_{0}_bulk'.format(FORMAT_TYPE)]
    populate_all_at_once(coll, 10, 10, FORMAT_TYPE)