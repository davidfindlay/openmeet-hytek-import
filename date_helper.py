import datetime
from dateutil.relativedelta import *

HYTEK_DATE_FORMAT = '%m/%d/%y %H:%M:%S'


def parse_hytek_date(hytek_date):
    return datetime.datetime.strptime(hytek_date, HYTEK_DATE_FORMAT)


def get_hytek_dob(hytek_date, age):
    hytek_dob = parse_hytek_date(hytek_date)
    calc_age = relativedelta(datetime.date.today(), hytek_dob).years

    if calc_age < 0:
        hytek_dob = hytek_dob - relativedelta(years=100)

    return hytek_dob


def to_sql_date(dt):
    return dt.strftime('%Y-%m-%d')
