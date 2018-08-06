import datetime
import csv


from volatility_functions import theoreticalVolatility, option_priceBS
from strategy_logic import openposition, closeposition
from data_model import Volatility_storage, Base

from sqlalchemy import create_engine, and_, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import extract

# trade data
year = 2017
ticker = 'RTS'
v = Volatility_storage
reward_to_risk = 1.8

#sql alchemy session
dbname = 'sqlite:///RTS2017.db'
engine = create_engine(dbname, echo=True)
Session = sessionmaker(bind=engine)
sql_session = Session()

expa_dict = {
    '3.17' : datetime.datetime(year,3,16,18,45),
    '6.17' : datetime.datetime(year,6,15,18,45),
    '9.17' : datetime.datetime(year,9,21,18,45),
    '12.17': datetime.datetime(year,12,21,18,45),
}
weekly_opt_expa_dict = {
    6: datetime.datetime(year, 2, 9, 18, 35),
    7: datetime.datetime(year, 2, 16, 18, 35),
    8: datetime.datetime(year, 2, 22, 18, 35),
    9: datetime.datetime(year, 3, 2, 18, 35),
    10: datetime.datetime(year, 3, 9, 18, 35),
    11: datetime.datetime(year, 3, 16, 18, 35),
    12: datetime.datetime(year, 3, 23, 18, 35),
    13: datetime.datetime(year, 3, 30, 18, 35),
    14: datetime.datetime(year, 4, 6, 18, 35),
    15: datetime.datetime(year, 4, 13, 18, 35),
    16: datetime.datetime(year, 4, 20, 18, 35),
    17: datetime.datetime(year, 4, 27, 18, 35),
    18: datetime.datetime(year, 5, 4, 18, 35),
    19: datetime.datetime(year, 5, 11, 18, 35),
    20: datetime.datetime(year, 5, 18, 18, 35),
    21: datetime.datetime(year, 5, 25, 18, 35),
    23: datetime.datetime(year, 6, 1, 18, 35),
    24: datetime.datetime(year, 6,8, 18, 35),
    25: datetime.datetime(year, 6,15, 18, 35),
    26: datetime.datetime(year, 6,22, 18, 35),
    27: datetime.datetime(year, 6, 29, 19, 35),
    28: datetime.datetime(year, 7, 6, 18, 35),
    29: datetime.datetime(year, 7, 13, 18, 35),
    30: datetime.datetime(year, 7, 20, 18, 35),
    31: datetime.datetime(year, 7, 27, 18, 35),
    32: datetime.datetime(year, 8, 3, 18, 35),
    33: datetime.datetime(year, 8, 10, 18, 35),
    34: datetime.datetime(year, 8, 17, 18, 35),
    35: datetime.datetime(year, 8, 24, 18, 35),
    36: datetime.datetime(year, 8, 31, 18, 35),
    37: datetime.datetime(year, 9, 7, 18, 35),
    38: datetime.datetime(year, 9, 14, 18, 35),
    39: datetime.datetime(year, 9, 21, 18, 35),
    40: datetime.datetime(year, 9, 28, 18, 35),
    41: datetime.datetime(year, 10, 5, 18, 35),
    42: datetime.datetime(year, 10, 12, 18, 35),
    43: datetime.datetime(year, 10, 19, 18, 35),
    44: datetime.datetime(year, 10, 26, 18, 35),
    45: datetime.datetime(year, 11, 2, 18, 35),
    46: datetime.datetime(year, 11, 9, 18, 35),
    47: datetime.datetime(year, 11, 16, 18, 35),
    48: datetime.datetime(year, 11, 23, 18, 35),
    49: datetime.datetime(year, 11, 30, 18, 35),
    50: datetime.datetime(year, 12, 7, 18, 35),
    51: datetime.datetime(year, 12, 14, 18, 35),
    52: datetime.datetime(year, 12, 21, 18, 35),
    53: datetime.datetime(year, 11, 28, 18, 35)
}

def find_asset(datetimeobject,asset_dict,expirydelta):
    ''' Iterate through asset_dict and return the first value where
        datetimeobject - asset_dict item < expirydelta
    '''
    for index,value in asset_dict.iteritems():
        delta = (value - datetimeobject).days
        # print ('timedelta:' + str(datetimeobject) +'delta:' + str(delta))
        if delta < expirydelta and delta>=0:
            return index

def get_sql_data(nameticker,timestamp_begin, timestamp_end):
    return sql_session.query(v).filter(and_(v.name == nameticker,
            v.timestamp > timestamp_begin,
            v.timestamp < timestamp_end)).all()

def outputCSV(data):
    headers = ['Week', 'Expiry', 'Enter timestamp', 'asset_price',
               'buy strike', 'buy type', 'buy price', 'sell strike',
               'sell type', 'sell price', 'enter pos balance',
               'close timestamp', 'asset_price_close', 'buy strike',
               'buy type', 'buy close price ', 'sell strike', 'sell type',
               'sell close price', 'close pos result', 'Total fin result']
    with open('output2017FYweekly.csv', 'wb') as csvfile:
        writer = csv.writer(csvfile, dialect='excel', lineterminator='\n')
        writer.writerow(headers)
        for row in data:
            writer.writerow(row)

def trading_loop():
    ''' trading loop doc string
    '''
    result = []
    for expiry_week_datetime in weekly_opt_expa_dict.values():
        # find index asset, delta in expiry should be <92 => our index
        asset_ticker_exp = find_asset(expiry_week_datetime,expa_dict,92)
        d = get_sql_data(ticker + '-' + asset_ticker_exp,
                        expiry_week_datetime - datetime.timedelta
                       (days=6, hours=8, minutes=30), expiry_week_datetime)
        inposition = False
        if not inposition:
        # openpositio returns timestamp, asset_price, sell_price,
        # buy_price, fin_pos, POSITION object
        # or None if didn't enter the position
            open_result = openposition(d,expiry_week_datetime)    
            if open_result:
                inposition = True
                position = open_result[-1]
                buy_info = position['buy']
                sell_info = position['sell']
# d[-1] - last record in sql query before expiry
# return timestamp, asset_price, sell_pos_exp_price, buy_pos_exp_price,
# fin_res,buy_strike,sell_strike,buy_opt_type,sell_opt_type
                close_result = closeposition(d[-1],position)
        # generate output string
        row = ['WeekN',str(expiry_week_datetime),open_result[0],open_result[1]]
        row1 = [buy_info[0],buy_info[1],open_result[3],sell_info[0]]
        row2 = [sell_info[1],open_result[2],open_result[4]]
        row3 = [close_result[0],close_result[1],close_result[5]]
        row4 = [close_result[7],close_result[3],close_result[6], close_result[8]]
        row5 = [close_result[2],close_result[4],open_result[4]+close_result[4]]
        row = row + row1 + row2 + row3 + row4 + row5
        result.append(row)
    outputCSV(result)
trading_loop()
