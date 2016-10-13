import re
import datetime


# standard date format : yyyy-mm-dd hh:mm:ss
def date_formatter(date):
    try:
        now = datetime.datetime.now()
        this_year = now.year
        yyyy = 0
        mm = 0
        dd = 0
        hh = 0
        mi = 0
        ss = 0

        spl_datetime = date.strip().split(' ')

        spl_date = spl_datetime[0].split('-')
        if len(spl_date) == 3:
            yyyy = int(spl_date[0])
            mm = int(spl_date[1])
            dd = int(spl_date[2])
        elif len(spl_date) == 2:
            yyyy = int(this_year)
            mm = int(spl_date[0])
            dd = int(spl_date[1])
        else:
            yyyy = 9999
            mm = 12
            dd = 31

        # if time information exists
        if len(spl_datetime) == 2:
            spl_time = spl_datetime[1].split(':')
            if len(spl_time) == 3:
                hh = int(spl_time[0])
                mi = int(spl_time[1])
                ss = int(spl_time[2])
            elif len(spl_time) == 2:
                hh = int(spl_time[0])
                mi = int(spl_time[1])
                ss = 0
            else:
                hh = 23
                mi = 59
                ss = 59

        d = datetime.date(yyyy, mm, dd)
        t = datetime.time(hh, mi, ss)

    except ValueError as ve:
        tmp = re.findall('\d+', date)
        yyyy = int(tmp[0])
        mm = int(tmp[1])
        dd = int(tmp[2])

        d = datetime.date(yyyy, mm, dd)
        t = datetime.time(0, 0, 0)

    dt = datetime.datetime.combine(d, t)
    return dt.strftime('%Y-%m-%d %H:%M:%S')

# wdate = date_formatter('2016-09-20 23:11:52')
# print(wdate[0:10])
# print(wdate[11:19])
# print(date_formatter('2016-09-20　来源: 央广网'))
