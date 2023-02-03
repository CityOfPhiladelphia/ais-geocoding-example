import petl as etl
import geopetl
from passyunk.parser import PassyunkParser
import requests
import csv
import logging
import datetime
from addresser import parse_location
import cx_Oracle
from config import ais_url, gatekeeperKey, source_creds, geocode_srid, ais_qry, tomtom_qry


# input data = opa property summary table (fields = MAILING_STREET, MAILING_CITY_STATE, MAILING_ZIP)
# note: srid can be set to 2272 or 4326. (set in config-> geocode_srid)
# given input data containing address, city/state and zip values and srid value:
# if it's in philly, try to get standardized street address using passayunk parser
# If its outside of philly, use open source parser ADDRESSER
# with acquired standardized addresses in data frame try to get coordinates by joining with ais address_summary table
# for rows with missing coordinates: use apis to get coordinates
# (AIS for philly addresses and tomtom for addresses outside of philly)


# requirements
# access to opa property summary table
# access to ais summary table and ais api key
# To obtain a key:
# Email ithelp@phila.gov to create a new support ticket, and copy maps@phila.gov on the email.
# Request that IT Help route the ticket to CityGeo.
# Describe the application that will be using AIS and provide a URL if possible.


# request AIS for X and Y coordinates
def ais_request(address_string,srid=2272):
    '''
    :param address_string:
    :param srid: integer
    :return: list containing X and Y coordinates
    '''
    request_str = ais_qry.format(ais_url=ais_url, geocode_field=address_string,srid=srid)
    try:
        r = requests.get(request_str, params=gatekeeperKey)
        if r.status_code == 404:
            logging.info('404 error')
            print('AIS 404 error!!')
            logging.info(request_str)
    except Exception as e:
        logging.info('''failed AIS request for {}'''.format(address_string))
        logging.info(request_str)
        raise e
    # extract coordinates from json request response
    feats = r.json()['features'][0]
    geo = feats.get('geometry')
    coordinates = geo.get('coordinates')
    return coordinates

# request tomtom for X and Y coordinates
def tomtom_request(address='no address', city=None, state=None,zip=None,srid=2272):
    '''
    :param address_string: string
    :param city: string
    :param state: string
    :param zip: string
    :param srid: integer
    :return: list containing X and Y coordinates
    '''
    if address:
        address = address.split(' ')
        address = '+'.join(address)
    request_str = tomtom_qry.format(address=address, city=city,
                                    state=state, zip=zip, srid=srid)
    # send request to tomtom
    try:
        r = requests.get(request_str)
        top_candidate = r.json().get('candidates')[0].get('location')
        coordinates = [top_candidate.get('x'), top_candidate.get('y')]
        return coordinates
    except Exception as e:
        logging.info('''failed TOMTOM request for {}'''.format(address))

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

    # connect to oracle opa
    dsn = cx_Oracle.makedsn(source_creds.get('host'), 1521, service_name=source_creds.get('service_name'))
    connection = cx_Oracle.connect(user=source_creds.get('user'), password=source_creds.get('password'),
                                   dsn=dsn, encoding="UTF-8")
    # input_addresses = input_addresses.selectnotnone('MAILING_ADDRESS_1')
    input_addresses = etl.fromoraclesde(connection, 'PROPERTY_SUMMARY_NEW',
            fields=['MAILING_STREET', 'MAILING_CITY_STATE', 'MAILING_ZIP'], limit=500)
    input_addresses = input_addresses.selectnotnone('mailing_street')
    input_addresses = etl.rename(input_addresses, {'mailing_street': 'address',
                                                   'mailing_city_state': 'city',
                                                   'mailing_zip':'zip'})


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

            # if parser response returns city, state or zip update row dict
            if parser_response and parser_response.get('city') and not row_dict.get('city'):
                row_dict['city'] = parser_response.get('city')
            if parser_response and parser_response.get('state') and not row_dict.get('state'):
                row_dict['state'] = parser_response.get('state')
            if parser_response and parser_response.get('zip') and not row_dict.get('zip'):
                row_dict['zip'] = parser_response.get('zip')

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
        #     try request ais
        #     except request tomtom
        # elif city and zip not philly  :
        #     try tomtom
        #     except coordinates NA NA
        #     #################################################################
        row_dict = dict(zip(joined_addresses_to_address_summary[0], row))
        address_full = "{} {} {}".format(row_dict.get('address'),
                                            row_dict.get('city'),
                                            row_dict.get('zip'))
        # address already has coordinates from join
        if row_dict.get('x_coordinate'):
            geocoded_frame.append(row_dict)
            continue
        # address does not have coordinates from join
        else:
            coordinates = None
            # if city or  zip relates to philly request ais
            if row_dict.get('city') and row_dict.get('city').lower() == 'philadelphia' or \
                    (row_dict.get('zip') and row_dict.get('zip') in philly_zipcodes):
                try:
                    coordinates = ais_request(row_dict.get('address'), str(geocode_srid))
                except:
                    coordinates = tomtom_request(address=row_dict.get('address'), city=row_dict.get('city'),
                                                 zip=row_dict.get('zip'), srid=geocode_srid)
                    print('%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%')
                else:
                    coordinates = ['NA','NA']
            #if city and zip not philly use tomtom
            elif (not row_dict.get('city') or row_dict.get('city') != 'philadelphia') \
                    and (row_dict.get('zip') and row_dict.get('zip') not in philly_zipcodes):
                try:
                    coordinates = tomtom_request(address=row_dict.get('address'), city=row_dict.get('city'),
                                          zip=row_dict.get('zip'),srid=geocode_srid)
                except:
                    coordinates = ['NA','NA']
            # if we have coordinates from tomtom or ais, store in dictionary
            if coordinates:
                row_dict['x_coordinate'] = coordinates[0]
                row_dict['y_coordinate'] = coordinates[1]
                geocoded_frame.append(row_dict)
            #
            else:
                logging.info('''unable to geocode {}'''.format(address_full))


    geocoded_frame = etl.fromdicts(geocoded_frame, header=joined_addresses_to_address_summary[0])
    # write geocoded results to memory
    geocoded_frame.tocsv('geocoded_opa_output_{}.csv'.format(geocode_srid))

if __name__ == "__main__":
    main()
