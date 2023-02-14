import petl as etl
import geopetl
from passyunk.parser import PassyunkParser
import requests
import csv
import logging
import datetime
from addresser import parse_location
from config import ais_url, gatekeeperKey, geocode_srid, ais_qry, tomtom_qry, ais_request, tomtom_request


# input data = https://www.kaggle.com/datasets/ahmedshahriarsakib/list-of-real-usa-addresses?resource=download
# given input data containing street and srid values try to get standardized street address using passayunk parser
# if it's in philly. If it's outside of philly, use open source parser addresser.
# try to get coordinates by joining with address_summary table
# else: use apis to get coordinates (AIS for philly addresses and tomtom for addresses outsideof philly)
# note: srid can be set to 2272 or 4326. (set in config-> geocode_srid)

# requirements
# access to ais api key
# To obtain a key:
# Email ithelp@phila.gov to create a new support ticket, and copy maps@phila.gov on the email.
# Request that IT Help route the ticket to CityGeo.
# Describe the application that will be using AIS and provide a URL if possible.


def main():
    # Logging Params:
    today = datetime.date.today()
    logfile = 'geocode_sample_data_log_{}.txt'.format(today)
    logging.basicConfig(filename=logfile, level=logging.INFO,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    logging.info('''\n\n------------------------------Geocoding Sample Data------------------------------''')
    # download address summary using requests
    address_summary_data = requests.get('https://opendata-downloads.s3.amazonaws.com/address_summary.csv')
    address_summary_data = address_summary_data.text.splitlines()
    address_summary_data = [s.split(',') for s in address_summary_data]
    # # save address summary table to memory
    with open('address_summary_fields.csv', 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(address_summary_data)

    # required address_summary (source) table fields
    if geocode_srid == 2272:
        adrsum_fields = ['street_address', 'geocode_x', 'geocode_y','zip_code']
        # load address summary data from csv to petl frame
        address_summary_rows = etl.fromcsv('address_summary_fields.csv').cut(adrsum_fields)
        address_summary_rows = etl.rename(address_summary_rows,
                                          {'geocode_x': 'x_coordinate', 'geocode_y': 'y_coordinate'})
    elif geocode_srid == 4326:
        adrsum_fields = ['street_address', 'geocode_lon', 'geocode_lat','zip_code']
        # load address summary data from csv to petl frame
        address_summary_rows = etl.fromcsv('address_summary_fields.csv').cut(adrsum_fields)
        address_summary_rows = etl.rename(address_summary_rows,
                                          {'geocode_lon': 'x_coordinate', 'geocode_lat': 'y_coordinate'})
    else:
        print('invalid srid')
        raise
    cols = etl.columns(address_summary_rows)
    z = cols['zip_code']
    philly_zipcodes = list(set(z))
    philly_zipcodes.remove('')
    philly_zipcodes = [str(z) for z in philly_zipcodes]
    address_summary_rows = address_summary_rows.cutout('zip_code')

    # get input address test data from input csv
    input_addresses = etl.fromcsv('list_of_real_usa_addresses.csv')
    #passayunk instance
    parser = PassyunkParser()
    dict_frame = []
    # try to get state and city using open source parser
    for row in input_addresses[1:]:
        ################################################################
        # if city is philly:
        #     use passayunk
        # elif not city:
        #     use addresser parser
        #     if zip is in philly or city is philly:
        #################################################################
        row_dict = dict(zip(input_addresses[0],row))
        row_dict['std_address'] = None
        address_full = "{} {} {} {}".format(row_dict.get('address'),
                                            row_dict.get('city'),
                                            row_dict.get('state'),
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
            # if parser response returns city, state or zip update row dict
            if parser_response and parser_response.get('city') and not row_dict.get('city'):
                row_dict['city'] = parser_response.get('city')
            if parser_response and parser_response.get('state') and not row_dict.get('state'):
                row_dict['state'] =parser_response.get('state')
            if parser_response and parser_response.get('zip') and not row_dict.get('zip'):
                row_dict['zip'] =parser_response.get('zip')

        # add input row to output list
        dict_frame.append(row_dict)

    # input addresses with std_address field and city/state/zip
    header = list(input_addresses[0]).append('std_address')
    input_addresses = etl.fromdicts(dict_frame, header=header)

    #join input data with address summary table data on standardized street address column
    joined_addresses_to_address_summary = etl.leftjoin(input_addresses, address_summary_rows, lkey='std_address', rkey='street_address', presorted=False )


    # empty
    geocoded_frame = []
    # use apis to get coordinates (AIS for philly addresses and tomtom for addresses outside of philly)
    for row in joined_addresses_to_address_summary[1:]:
        ################################################################
        # if city or zip is philly:
        #     use ais
        # elif city and zip not philly  :
        #     use tomtom
        #     if zip is in philly or city is philly:
        #################################################################
        row_dict = dict(zip(joined_addresses_to_address_summary[0], row))
        address_full = "{} {} {} {}".format(row_dict.get('address'),
                                            row_dict.get('city'),
                                            row_dict.get('state'),
                                            row_dict.get('zip'))
        # address already has coordinates from join
        if row_dict.get('x_coordinate'):
            row_dict['time(s)'] = 'NA'
            row_dict['API'] = 'NA'
            geocoded_frame.append(row_dict)
            continue
        # address does not have coordinates from join
        else:
            coordinates = None
            # if city or  zip relates to philly request ais
            if row_dict.get('city') and row_dict.get('city').lower() == 'philadelphia' or \
                    (row_dict.get('zip') and row_dict.get('zip') in philly_zipcodes):
                t1 = datetime.datetime.now()
                coordinates = ais_request(row_dict.get('address'), str(geocode_srid))
                t2 = datetime.datetime.now()
                time_delta = t2 - t1
                row_dict['time(s)'] = '{}.{}'.format(time_delta.seconds, time_delta.microseconds)
                row_dict['API'] = 'AIS'
            #if city and zip not philly use tomtom
            elif (not row_dict.get('city') or row_dict.get('city') != 'philadelphia') \
                    and (row_dict.get('zip') and row_dict.get('zip') not in philly_zipcodes):
                try:
                    t1 = datetime.datetime.now()
                    coordinates = tomtom_request(address=row_dict.get('address'), city=row_dict.get('city'),
                                          state= row_dict.get('state'),zip=row_dict.get('zip'),srid=geocode_srid)
                    t2 = datetime.datetime.now()
                    time_delta = t2 - t1
                    row_dict['time(s)'] = '{}.{}'.format(time_delta.seconds, time_delta.microseconds)
                    row_dict['API'] = 'TOMTOM'
                except:
                    row_dict['time(s)'] = 'NA'
                    row_dict['API'] = 'NA'

            # if we have coordinates from tomtom or ais, store in dictionary
            if coordinates:
                row_dict['x_coordinate'] = coordinates[0]
                row_dict['y_coordinate'] = coordinates[1]
                geocoded_frame.append(row_dict)
            else:
                row_dict['x_coordinate'] = 'NA'
                row_dict['y_coordinate'] ='NA'
                row_dict['time(s)'] = 'NA'
                row_dict['API'] = 'NA'
                logging.info('''unable to geocode {}'''.format(address_full))
                geocoded_frame.append(row_dict)


    header = list(joined_addresses_to_address_summary[0])
    header.append('API')
    header.append('time(s)')
    geocoded_frame = etl.fromdicts(geocoded_frame, header=header)
    # write geocoded results to memory
    geocoded_frame.tocsv('geocoded_sample_data_output_{}.csv'.format(geocode_srid))

if __name__ == "__main__":
    main()
