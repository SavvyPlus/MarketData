import datetime


def get_time_now(text=True, format='%Y-%m-%d %H:%M:%S.%f'):
    if text is True:
        return datetime.datetime.now().strftime(format)
    else:
        return datetime.datetime.now()
