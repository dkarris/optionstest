'''
convert csv files from csv_files  and load to sql_name sqlite3 database
also need to change line 46 to change filter to current year

'''
import os
import csv
import datetime

from config import tickers, csv_path, sql_name
# from config import csv_files

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from data_model import Volatility_storage
from data_model import Base

# preparing SQL Alchemy stuff
YEAR = '2015'
sql_name = 'sqlite:///' + sql_name
engine = create_engine(sql_name, echo=True)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
sql_session = Session()
columnnames = Volatility_storage.__table__.columns.keys()
# delete ID sqllite column as it is not in csv and we'll get an error when
# zipping
del  columnnames[0]

# get list of files in directory and
fileslist = os.listdir(os.path.join(os.getcwd(), csv_path))
for filename in fileslist:
    if '.csv' in filename:
        with open(os.path.join(os.getcwd(), csv_path, filename)) as f:
            print 'Opening file: ' + str(filename)
            reader = csv.reader(f, delimiter=';')
            # skip headers
            reader.next()
            count = 0
            # Count for records to sql
            for row in reader:
                #check if the row is in tickers then add otherwise proceed to the next row
                short_name = row[0].split('-')[0]
                if row[0][-2:] != 'vm': #remove some trash records with vm  like RTS-1.15vm
                    if row[1][-1:] == '6': # remove not current year:  RIH6..., Si
                        if short_name in tickers:
                            # if bad format - comma (,) is decimal replace with dot (.) to match Python
                            # float requirements
                            for index, value in enumerate(row):
                                row[index] = row[index].replace(',','.')
                            # convert csv datetime into Python datetime class and replace string with
                            # python datetime
                            csv_datetime = row[2]
                            pdate_time = datetime.datetime.strptime(csv_datetime, '%Y%m%d%H%M%S%f')
                            row[2] = pdate_time
                            # Now add short name to the row list zip and create object instance
                            # of csv record
                            #Now check if there is ','
                            row.append(short_name)
                            line_zipped = dict(zip(columnnames, row))
                            record = Volatility_storage(**line_zipped)
                            sql_session.add(record)
                            count += 1
            sql_session.commit()
            print 'Added ' + str(count) +' records. From' + str(filename)
