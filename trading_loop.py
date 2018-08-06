import datetime
import csv


from volatility_functions import theoreticalVolatility, option_priceBS
from strategy_logic import openposition, closeposition
from data_model import Volatility_storage, Base

from sqlalchemy import create_engine, and_, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import extract

# trade data
year = 2018
ticker = 'RTS'
v = Volatility_storage
reward_to_risk = 1.8

#sql alchemy session
dbname = 'sqlite:///RTS2018.db'
engine = create_engine(dbname, echo=True)
Session = sessionmaker(bind=engine)
sql_session = Session()

expa_dict = {
    '3.18' : datetime.datetime(year,3,15,18,45),
    '6.18' : datetime.datetime(year,6,21,18,45),
    '9.18' : datetime.datetime(year,9,20,18,45),
    '12.18': datetime.datetime(year,12,20,18,45),
}
weekly_opt_expa_dict = {
    2: datetime.datetime(year, 1, 11, 18, 35),
    3: datetime.datetime(year, 1, 18, 18, 35),
    4: datetime.datetime(year, 1, 25, 18, 35),
    5: datetime.datetime(year, 2, 1, 18, 35),
    6: datetime.datetime(year, 2, 8, 18, 35),
    7: datetime.datetime(year, 2, 15, 18, 35),
    8: datetime.datetime(year, 2, 22, 18, 35),
    9: datetime.datetime(year, 3, 1, 18, 35),
    10: datetime.datetime(year, 3, 7, 18, 35),
    11: datetime.datetime(year, 3, 15, 18, 35),
    12: datetime.datetime(year, 3, 22, 18, 35),
    13: datetime.datetime(year, 3, 29, 18, 35),
    14: datetime.datetime(year, 4, 5, 18, 35),
    15: datetime.datetime(year, 4, 12, 18, 35),
    16: datetime.datetime(year, 4, 19, 18, 35),
    17: datetime.datetime(year, 4, 26, 18, 35),
    18: datetime.datetime(year, 5, 3, 18, 35),
    19: datetime.datetime(year, 5, 10, 18, 35),
    20: datetime.datetime(year, 5, 17, 18, 35),
    21: datetime.datetime(year, 5, 24, 18, 35),
    22: datetime.datetime(year, 5, 31, 18, 35),
    23: datetime.datetime(year, 6, 7, 18, 35),
    24: datetime.datetime(year, 6,14, 18, 35),
    25: datetime.datetime(year, 6,21, 18, 35),
    26: datetime.datetime(year, 6,28, 18, 35)
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
    with open('output20181hweekly.csv', 'wb') as csvfile:
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
