# data_controller.py
import logging
import csv
from operator import itemgetter

dc_logger = logging.getLogger('dc')


def limit(data, limit=None):
    if limit is not None:
        dc_logger.info('Limiting results to %s', limit[0])
        return data[:limit[0]]
    else:
        return data


def sort(data, valid_values, sortkey):
    if sortkey is None:
        return data
    else:
        dc_logger.info('Sort params=> %s', sortkey)
        valid_types = {'ASC': False, 'DESC': True}
        if sortkey[1].upper() in valid_types.keys():
            type = valid_types[sortkey[1].upper()]
        else:
            type = None

        sk = sortkey[0] if sortkey[0] in valid_values else False
        if not sk:
            dc_logger.error('Sortkey:%s is invalid', sortkey[0])
            raise ValueError('Unrecognised order_by field, must be in %r' %
                             valid_values)
        elif type is None:
            dc_logger.error('Sort type is invalid')
            raise ValueError('Unrecognised order_by field, must be in %r' %
                             list(valid_types.keys()))
        else:
            dc_logger.info('Sorting data by %s %s', sk, type)
            data = sorted(data, key=itemgetter(sk), reverse=type)
    return data


def save_to_file(filename, data):
    if not filename.endswith('.csv'): 
        filename += '.csv'
    try:
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
    except csv.Error as e:
        raise Exception(e)
