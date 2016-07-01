# coding: utf-8
import calendar
from compiler.ast import flatten

def add_0(item, month):
    if len(item) == 1:
        item = '2016' + month + '0' + item
    else:
        item = '2016' + month + item
    return item

def add_00(item):
    if len(item) == 1:
        item = '00' + item
    elif len(item) == 2:
        item = '0' + item
    return item

yearallday = []

for month in ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']:
    result = list(flatten(calendar.monthcalendar(2016, int(month))))
    result2 = [str(item) for item in result if item != 0]
    lastresult = [add_0(item, month) for item in result2]
    yearallday.append(lastresult)

yearallday = list(flatten(yearallday))

daysnum = [i for i in range(1, 121)]
daysnumber = [add_00(str(item)) for item in daysnum]

ererydaynumber = list()
for yearmonthday in yearallday:
    for daynum in daysnumber:
        ererydaynumber.append(yearmonthday + daynum)


def get_perfect(current, num):
    end = ererydaynumber.index(current) + 1
    start = end - num

    return ererydaynumber[start:end]
