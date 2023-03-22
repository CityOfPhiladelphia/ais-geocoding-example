# given input data containing street and srid values try to get standardized street address using passayunk parser
# try to get coordinates by joining with address_summary table
# else get coordinates corresponding using AIS api or tomtom api
# note: srid can be set to 2272 or 4326. (set in config-> geocode_srid)

# requirements
# access to ais summary table and ais api key
# To obtain a key:
# Email ithelp@phila.gov to create a new support ticket, and copy maps@phila.gov on the email.
# Request that IT Help route the ticket to CityGeo.
# Describe the application that will be using AIS and provide a URL if possible.

import petl as etl
import geopetl
from passyunk.parser import PassyunkParser
import requests
import csv
import logging
import datetime
from addresser import parse_location
import cx_Oracle
from config import ais_url, gatekeeperKey, geocode_srid, ais_qry, tomtom_qry
import googlesearch
from os.path import exists

# file_exists = exists(path_to_file)


def ais_request(address_string,srid):
    '''
    :param address_string:
    :param srid: integer
    :return: list containing X and Y coordinates
    '''
    params = gatekeeperKey
    request_str = ais_qry.format(ais_url=ais_url, geocode_field=address_string,srid=srid)
    try:
        # extract coordinates from json request response
        r = ais_session.get(request_str, params=params)
        feats = r.json()['features'][0]
        geo = feats.get('geometry')
        coords = geo.get('coordinates')
        if r.status_code == 404:
            print('404 error')
            logging.info(request_str)
            raise
    except Exception as e:
        logging.info('''failed request for {}'''.format(address_string))
        logging.info(request_str)
        return None
        # raise e
    return coords


# request tomtom for X and Y coordinates
def tomtom_request(address='no address', city=None, state= None,zip=None,srid=2272):
    '''
    :param address_string: string
    :param srid: integer
    :return: list containing X and Y coordinates
    '''
    s = address.split(' ')
    address = '+'.join(s)
    request_str = tomtom_qry.format(address=address, city=city, state=state, zip=zip, srid=srid)
    try:
        r = tomtom_session.get(request_str)
    except Exception as e:
        logging.info(request_str)
        raise e
    # try to get a top address candidate if any
    try:
        top_candidate = r.json().get('candidates')[0].get('location')
        top_candidate = [top_candidate.get('x'), top_candidate.get('y')]
    except Exception as e:
        logging.info('''failed tomtom request for {}'''.format(address))
        logging.info(request_str)
        logging.info('')
        raise e
    return top_candidate


#def main():
if __name__ == "__main__":
    start = datetime.datetime.now()
    # Logging Params:
    today = datetime.date.today()
    logfile = 'geocode_sample_data_log_{}.txt'.format(today)
    logging.basicConfig(filename=logfile, level=logging.INFO,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    logging.info('''\n\n------------------------------Geocoding Sample Data------------------------------''')
    if exists('address_summary_data.csv'):
        #address summary data from local csv
        address_summary_rows = etl.fromcsv('address_summary_data.csv')
    else:
        # download address summary data using requests
        address_summary_data = requests.get('https://opendata-downloads.s3.amazonaws.com/address_summary.csv')
        address_summary_data = address_summary_data.text.splitlines()
        # write address summary data to memory
        address_summary_data = [s.split(',') for s in address_summary_data]
        # # save address summary table to memory
        with open('address_summary_data.csv', 'w', encoding='UTF8', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(address_summary_data)
        address_summary_rows = etl.fromcsv('address_summary_data.csv')

    # required address_summary table fields
    if geocode_srid == 2272:
        adrsum_fields = ['street_address', 'geocode_x', 'geocode_y','zip_code']
        # load address summary data from csv to petl frame
        address_summary_rows = etl.fromcsv('address_summary_data.csv').cut(adrsum_fields)
        address_summary_rows = etl.rename(address_summary_rows,
                                          {'geocode_x': 'x_coordinate', 'geocode_y': 'y_coordinate'})
    elif geocode_srid == 4326:
        adrsum_fields = ['street_address', 'geocode_lon', 'geocode_lat','zip_code']
        # load address summary data from csv to petl frame
        address_summary_rows = etl.fromcsv('address_summary_data.csv').cut(adrsum_fields)
        address_summary_rows = etl.rename(address_summary_rows,
                                          {'geocode_lon': 'x_coordinate', 'geocode_lat': 'y_coordinate'})
    else:
        print('invalid srid')
        raise

    # get zip codes in philadelphia
    cols = etl.columns(address_summary_rows)
    z = cols['zip_code']
    philly_zipcodes = list(set(z))
    philly_zipcodes.remove('')
    philly_zipcodes = [str(z) for z in philly_zipcodes]
    #remove zip_code column
    address_summary_rows = address_summary_rows.cutout('zip_code')

    # input addresses
    input_addresses = etl.fromcsv('list_of_real_usa_addresses.csv')

    #passayunk instance
    parser = PassyunkParser()
    dict_frame = []
    # try to get state and city using open source parser
    for row in input_addresses[1:]:
        ################################################################
        # if city is philly:
        #     try passayunk
        # elif not city:
        #     use addresser parser
        #     if zip is in philly or city is philly:
        #################################################################
        row_dict = dict(zip(input_addresses[0],row))
        row_dict['std_address'] = None
        # leave out state info for now
        address_full = "{} {} {}".format(row_dict.get('address'),
                                            row_dict.get('city'),
                                            row_dict.get('zip'))

        # if row dictionary city or input zip relates to philly
        if row_dict.get('city') and row_dict.get('city').lower() == 'philadelphia' or \
                (row_dict.get('zip') and row_dict.get('zip') in philly_zipcodes):
            # parse address using passayunk
            std_address = parser.parse(row_dict.get('address'))
            row_dict['std_address'] = std_address['components']['output_address']

        # if no row dictionary city or city is not philly and zip not in philly
        if (not row_dict.get('city') or row_dict.get('city') != 'philadelphia')\
                and (row_dict.get('zip') and row_dict.get('zip') not in philly_zipcodes):
            # parse full address using open source parser
            parser_response = parse_location(address_full)
            # if parser response returns city or zip relating to philly
            if (parser_response and str(parser_response.get('zip')) and str(parser_response.get('zip')) in philly_zipcodes)\
                or (parser_response and parser_response.get('city') and parser_response.get('city').lower() == 'philadelphia'):
                std_address = parser.parse(address_full)
                row_dict['std_address'] = std_address['components']['output_address']

            # if parser response returns two streets assume it's an intersection
            if parser_response and parser_response.get('street1') and parser_response.get('street2'):
                # reformat address 'street1+and+street2'
                row_dict['address'] = parser_response.get('street1') + '+and+' + parser_response.get('street2')

            #if parser response returns city, state or zip update row dict
            if parser_response and parser_response.get('city'):#and not row_dict.get('city'):
                row_dict['city'] = parser_response.get('city')
            if parser_response and parser_response.get('state'):# and not row_dict.get('state'):
                row_dict['state'] = parser_response.get('state')
            if parser_response and parser_response.get('zip'):# and not row_dict.get('zip'):
                row_dict['zip'] = parser_response.get('zip')
        # add input row to output list
        dict_frame.append(row_dict)


    # input addresses with std_address field and city/state/zip
    header = list(input_addresses[0]).append('std_address')
    input_addresses = etl.fromdicts(dict_frame, header=header)
    joined_none = etl.selectnotnone(input_addresses, 'std_address')

    #join input data with address summary table data on standardized street address column
    joined_addresses_to_address_summary = etl.leftjoin(input_addresses, address_summary_rows, lkey='std_address', rkey='street_address', presorted=False )
    t = datetime.datetime.now() - start

    # empty list to store rows with coordinates
    geocoded_frame = []
    fails = []
    fails.append('failed_addresses')
    # use apis to get coordinates (AIS for philly addresses and tomtom for addresses outside of philly)
    for row in joined_addresses_to_address_summary[1:]:
        ################################################################
        # if city or zip is philly:
        #     try request ais
        #     except request tomtom
        # elif city and zip not philly  :
        #     try tomtom
        #     except coordinates NA NA
        #     #################################################################
        ais_session = requests.Session()
        tomtom_session = requests.Session()
        row_dict = dict(zip(joined_addresses_to_address_summary[0], row))
        address_full = "{} {} {}".format(row_dict.get('address'),
                                            row_dict.get('city'),
                                            row_dict.get('zip'))
        # address already has coordinates from join
        if row_dict.get('x_coordinate'):
            geocoded_frame.append(row_dict)
            row_dict['time(s)'] = 'NA'
            row_dict['API'] = 'NA'
            continue
        # address does not have coordinates from join
        else:
            coordinates = None
            # if city or  zip relates to philly request ais
            if row_dict.get('city') and row_dict.get('city').lower() == 'philadelphia' or \
                    (row_dict.get('zip') and row_dict.get('zip') in philly_zipcodes):
                try:
                    t1 = datetime.datetime.now()
                    coordinates = ais_request(row_dict.get('address_std'), str(geocode_srid))
                    t2 = datetime.datetime.now()
                    time_delta = t2 - t1
                    row_dict['time(s)'] = '{}.{}'.format(time_delta.seconds, time_delta.microseconds)
                    row_dict['API'] = 'AIS'
                except:
                    try:
                        t1 = datetime.datetime.now()
                        coordinates = tomtom_request(address=row_dict.get('address'), city=row_dict.get('city'),zip=row_dict.get('zip'),
                                                     state=row_dict.get('state'), srid=geocode_srid)
                        t2 = datetime.datetime.now()
                        time_delta = t2 - t1
                        row_dict['time(s)'] = '{}.{}'.format(time_delta.seconds, time_delta.microseconds)
                        row_dict['API'] = 'TOMTOM'
                    except:
                        logging.info('neither apis worked for address ', row_dict.get('address'))
                        row_dict['time(s)'] = 'NA'
                        row_dict['API'] = 'UNABLE TO GEOCODE'
                        fails.append(row_dict.get('address'))
            #if city and zip not philly use tomtom
            elif (not row_dict.get('city') or row_dict.get('city') != 'philadelphia') \
                    and (row_dict.get('zip') and row_dict.get('zip') not in philly_zipcodes):
                try:
                    t1 = datetime.datetime.now()
                    coordinates = tomtom_request(address=row_dict.get('address'), city=row_dict.get('city'),state=row_dict.get('state'),
                                          zip=row_dict.get('zip'),srid=geocode_srid)
                    t2 = datetime.datetime.now()
                    time_delta = t2 - t1
                    row_dict['time(s)'] = '{}.{}'.format(time_delta.seconds, time_delta.microseconds)
                    row_dict['API'] = 'TOMTOM'
                except:
                    row_dict_fail = row_dict
                    google_result = googlesearch.search(address_full)
                    try:
                        first_result = next(google_result)
                    except:
                        first_result = 'no google result'
                    row_dict_fail['google_result'] = first_result
                    row_dict_fail['time(s)'] = 'NA'
                    row_dict_fail['API'] = 'UNABLE TO GEOCODE'
                    fails.append(row_dict_fail)

            # if we have coordinates from tomtom or ais, store in dictionary
            if coordinates:
                row_dict['x_coordinate'] = coordinates[0]
                row_dict['y_coordinate'] = coordinates[1]
                geocoded_frame.append(row_dict)
            else:
                logging.info('''unable to geocode {}'''.format(address_full))
    # write failures to memory
    if fails:
        fails_frame = etl.fromdicts(fails[1:], header=list(joined_addresses_to_address_summary[0]).append('google_result'))
        fails_frame.tocsv('geocode_sample_data_fails.csv')
    else:
        print('no fails')

    header = list(joined_addresses_to_address_summary[0])
    header.append('time(s)')
    header.append('API')
    geocoded_frame = etl.fromdicts(geocoded_frame, header=header)
    end = datetime.datetime.now() - start
    # write geocoded results to memory
    geocoded_frame.tocsv('geocode_sample_data_output_{}.csv'.format(geocode_srid))
