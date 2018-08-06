'''
This is file for options back testing.
Loads data from sql, calculates theoretical volatility, BS pricing and greeks

Strategy:
toss a coin. Buy either call or put.
Apply martingale
'''

# import stuff

import datetime
import random
import csv

from sqlalchemy import create_engine, and_, func
from sqlalchemy.orm import sessionmaker
# from sqlalchemy.types import Numeric, DateTime, Float
from sqlalchemy.sql.expression import extract

from volatility_functions import theoreticalVolatility, option_priceBS
from data_model import Volatility_storage, Base


# CONSTS
year = 2015
strike_step = 2500
CONST_DAYS = 365.
CSV_OUTPUT = 'output.csv'

# variables for expiration check
expa_dict = {
    '3.15' : datetime.datetime(year,3,16,18,45),
    '6.15' : datetime.datetime(year,6,15,18,45),
    '9.15' : datetime.datetime(year,9,15,18,45),
    '12.15': datetime.datetime(year,12,15,18,45),
}

opt_expa_dict = {
    '1': datetime.datetime(year,1,15,18,45),
    '2': datetime.datetime(year,2,16,18,45),
    '3': datetime.datetime(year,3,16,18,45),
    '4': datetime.datetime(year,4,15,18,45),
    '5': datetime.datetime(year,5,15,18,45),
    '6': datetime.datetime(year,6,15,18,45),
    '7': datetime.datetime(year,7,15,18,45),
    '8': datetime.datetime(year,8,17,18,45),
    '9': datetime.datetime(year,9,15,18,45),
    '10': datetime.datetime(year,10,15,18,45),
    '11': datetime.datetime(year,11,16,18,45),
    '12': datetime.datetime(year,12,15,18,45),
}


dbname = 'sqlite:///BR2015.db'


# Prepare SQL

engine = create_engine(dbname, echo=False)
Session = sessionmaker(bind=engine)
sql_session = Session()
# running for each month 
query_records = sql_session.query(Volatility_storage)
t_threshold = 30/365. # select only short term one m

time_enter = datetime.time(10, 02, 00)
time_close = datetime.time(18, 40, 00)
hour_enter_min = time_enter.hour
hour_enter_max = hour_enter_min + 2
hour_close_max = time_close.hour
hour_close_min = time_close.replace(hour=(18-1))
open_entries = []

def find_open_pos_record(records, hour, minute):
    '''
    Returns id of the record matching open or close time for position
    '''
    for record in records:
        # try to find the closest match with min at first
        # print record.timestamp.hour, record.timestamp.minute
        # check the date-time
        nearestContract = False
        if record.timestamp.hour == hour and record.timestamp.minute == minute:
            # check that the nearest contract (most liquid) is selected
            # cut contract expiration from name
            contract_expa = record.name.split('-')[1]
            # if expiration happens in dates defined in expa_dict then proceed
            if contract_expa in expa_dict.keys():
                # check that it is the nearest
                if (expa_dict[contract_expa] - record.timestamp).days <= 92:
                    nearestContract = True
            if nearestContract:
                # print record.id, record.timestamp
                record_id = record.id
                open_entries.append(record)
                print 'Added record with:'
                print record.id, record.name, record.small_name,record.timestamp
                return record_id
    return None
def find_close_pose_record(open_record):
    '''
    open_record = volatility_storage object.
    find sql entry for the same ticket on the same day at hour_close_max or earlier if exists
    returns matching volatility_storage object or None if not found
    '''
    record_found = False

    # Next lines are to simplify query = > not entirely necessary
    
    search_date = open_record.timestamp.date()
    search_name = open_record.name
    search_time = time_close
    while not record_found:
        close_record = query_records.filter(and_(Volatility_storage.name == open_record.name,
                            extract('day', Volatility_storage.timestamp) == search_date.day, 
                            extract('month', Volatility_storage.timestamp) == search_date.month, 
                            extract('hour', Volatility_storage.timestamp) == search_time.hour,
                            extract('minute', Volatility_storage.timestamp) == search_time.minute)).first()
        if close_record:
            record_found = True
            return close_record
        curr_min = search_time.minute        
        curr_hour = search_time.hour
        if curr_min >=1:
            curr_min -= 1
        else:
            curr_min = 59
            curr_hour = curr_hour - 1
        search_time = search_time.replace(hour=curr_hour, minute=(search_time.minute-1))
        if search_time < hour_close_min:
            # no closing record found
            return None
def find_high_low_pos(open_record):
    search_date = open_record.timestamp.date()
    search_name = open_record.name
    highest_record_id = sql_session.query(func.max(Volatility_storage.asset_price),
                        Volatility_storage.id).filter(and_(Volatility_storage.name == open_record.name,
                        extract('day', Volatility_storage.timestamp) == search_date.day, 
                        extract('month', Volatility_storage.timestamp) == search_date.month)).one()
    # print type(highest_record_id[1]), highest_record_id[1]
    highest_record = query_records.filter(Volatility_storage.id == highest_record_id[1]).one()
    lowest_record_id = sql_session.query(func.min(Volatility_storage.asset_price), Volatility_storage.id).\
                        filter(and_(Volatility_storage.name == open_record.name,
                        extract('day', Volatility_storage.timestamp) == search_date.day, 
                        extract('month', Volatility_storage.timestamp) == search_date.month)).one()
    lowest_record = query_records.filter(Volatility_storage.id == lowest_record_id[1]).one()
    return highest_record, lowest_record

def calculate_prices(options_list):
    '''
    Receives filtered options list in the format:
    [[data_open],[data_high],[data_low],[data_close]],[.....]]
    each data has the following structue:
    [record.name, record.small_name, record.timestamp, record.asset_price,
                              opttype, strike, theor_vola, opt_price, time_to_exp(option),delta, gamma, theta, vega]
    returns list with some data extracted from volatility_storage objects
    and adds     
    '''
    all_records = []
    for record_date in options_list:
        # temp list to append with daily open, high, low, close records.
        daily_records_list = []
        new_day = True # if new day, then generate new strike ATM if not strike should be the same
        for record in record_date:
            # Unpack object properties into variables passed to function
            asset_price,s,a = float(record.asset_price), float(record.s), float(record.a)
            b,c,d,e,t = float(record.b), float(record.c), float(record.d), float(record.e), float(record.t)
            # Calculate strike if it is a new day and option type.
            # If not new day then option type and strike will be preserved from the
            # previous record
            if new_day:
                if asset_price % strike_step > strike_step*0.5:
                    # if true then we need to round up to the highest strike
                    strike = strike_step - (asset_price % strike_step) + asset_price
                else:
                    # otherwise - step down one strike down
                    strike = asset_price - (asset_price % strike_step)
                random.seed()
                if random.random() > 0.5:
                    opttype = "call"
                else:
                    opttype = "put"
                new_day = False
            try:
                theor_vola = theoreticalVolatility(strike, asset_price, s, a, b, c, d, e, t)
            except:
                print ('Some bad stuff happened when calling calc Theor. volatility')
                print strike, asset_price, s, a, b, c, d, e, t
                print ('record with id ') + str(record.id)
                print record.timestamp
                for e1 in options_list:
                    for e in e1:
                        print e.id, e.name, e.small_name, e.timestamp, e.asset_price
                raise SystemExit(0)
            # find the nearest expiration date and obtain t for that
            time = record.timestamp
            time_to_exp = temp_time_to_exp = 0
            for value in opt_expa_dict.values():
                temp_time_to_exp = (value-time).days/CONST_DAYS
                if temp_time_to_exp == 0: # expiration is today convert to seconds to day
                    temp_time_to_exp = (value-time).seconds/60./60/24/365
                # print 'inside time funct'
                # print 'old temp var:' +  str(temp_time_to_exp)
                # print 'new temp var' + str(time_to_exp)
                if temp_time_to_exp > 0 and time_to_exp == 0:
                #   setup value for the first loop
                    time_to_exp = temp_time_to_exp
                if temp_time_to_exp > 0 and temp_time_to_exp<time_to_exp:
                # if true then value is between negative value and zero => expiration is
                # closer than previous stores.
                    time_to_exp = temp_time_to_exp
            try:
                opt_price, delta, gamma, theta, vega = option_priceBS(asset_price, strike, theor_vola, time_to_exp, opttype)
            except:
                print 'bad stuff occured when calling option_priceBS!'
                print ('id:') + str(record.id)
                print asset_price, strike, theor_vola, time_to_exp, opttype
                raise SystemExit(0)
            record_to_list = [record.name, record.small_name, record.timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                              record.asset_price, opttype, strike, theor_vola, opt_price, time_to_exp,
                              delta, gamma, theta, vega]
            daily_records_list.append(record_to_list)
        all_records.append(daily_records_list)
    return all_records


def generate_csv(output):
    header =  4 * ["record.name", "record.small_name", "record.timestamp", "record.asset_price",\
    "opttype", "strike", "theor_vola", "opt_price", "time_to_exp(opt)", "delta", "gamma", "theta", "vega"]
    with open(CSV_OUTPUT, 'w') as outputcsv:
        writer = csv.writer(outputcsv, dialect='excel',lineterminator='\n')
        writer.writerow(header)

# data stucture 
#[[day1[open][high][low][close],[day2][open],[high],[low],[close]],...]

        for row in output:
            merge_sublists = []
            for list_element in row:
            #     for index, element in enumerate(list_element):
            #         # rearrange python data structure to csv excel compatible format
            #         list_element[index] = str(element)
                merge_sublists = merge_sublists + list_element
            # print merge_sublists
            writer.writerow(merge_sublists)

for x in range(1,13):
    print 'Calculating for month:' + str(x)
    records_month = query_records.filter(extract('month',
        Volatility_storage.timestamp) == x).order_by(extract('day',
        Volatility_storage.timestamp)).all()
    date_begin = records_month[0].timestamp.day
    date_end = records_month[len(records_month)-1].timestamp.day
    for y in range(date_begin,date_end+1):
        # Get records for the specific day
        positionOpened = False
        positionLong = False
        records_date_open = query_records.filter(and_(extract('month',
            Volatility_storage.timestamp) == x,
            extract('day', Volatility_storage.timestamp) == y),
            extract('hour', Volatility_storage.timestamp) >= hour_enter_min,
            extract('hour', Volatility_storage.timestamp) <= hour_enter_max,
            Volatility_storage.t > 0).all()
        if len(records_date_open)>0:
            # we have data for entry find the earliest record and open position
            minute_enter_min = time_enter.minute
            openError = False
            while (not positionOpened) and (not openError):
                print 'PositionOpened ' + str(positionOpened)
                record_id = find_open_pos_record(records_date_open, hour_enter_min, minute_enter_min)
                if record_id:
                    positionOpened = True
                else:
                    minute_enter_min += 1
                    if minute_enter_min == 60:
                        minute_enter_min = 0
                        hour_enter_min += 1
                    if hour_enter_min > hour_enter_max:
                        # Was not able to open position
                        print 'error raised!!! Last record'
                        print record_id
                        openError = True
# after the code above is executed we have list of volatility_storage objects in open_entries list
all_entries = []
# last_entry = len(open_entries)-1
# print open_entries[last_entry].id
# print open_entries[last_entry].name
# print open_entries[last_entry].small_name
# print open_entries[last_entry].timestamp
# raise SystemExit(0)

for open_pos in open_entries:
    # browse through entries and find record on the same day closing on
    # time_close or earlier.
    # print ('find close  record for ') + str(open_pos.timestamp)
    close_pos = find_close_pose_record(open_pos)
    # returns volatility with highews and lowest asset_price
    highest_pos, lowest_pos = find_high_low_pos(open_pos)
    entry = [open_pos, highest_pos, lowest_pos,close_pos]
    all_entries.append(entry)

# Now we have a full list of matching entries in the format
# [[volatility_storage opened position],[volatility_storage matching closed position]]

# Next we call function with list which calculates theoretical volatility and BS price
# Call or put is decided by random generator

# print calculate_prices(all_entries) - it returns all required info
# so we can print it or export to CSV

output = calculate_prices(all_entries)
generate_csv(output)

     # Hey Dad what does your code say?





     # Oh,never mind.


     # Hey Denis, it doesn't say anything special. Just some not funny stuff