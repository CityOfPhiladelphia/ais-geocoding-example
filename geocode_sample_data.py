import petl as etl
import geopetl
from passyunk.parser import PassyunkParser
import requests
import csv
import logging
import datetime
from addresser import parse_location
from config import ais_url, gatekeeperKey, source_creds, geocode_srid, ais_qry, tomtom_qry


# input data = https://www.kaggle.com/datasets/ahmedshahriarsakib/list-of-real-usa-addresses?resource=download
# given input data containing street and srid values try to get standardized street address using passayunk parser
# if it's in philly. If its outside of philly, use open source parser addresset
# try to get coordinates by joining with address_summary table
# else get coordinates corresponding using AIS api or tomtom api
# note: srid can be set to 2272 or 4326. (set in config-> geocode_srid)

# requirements
# access to ais summary table and ais api key
# To obtain a key:
# Email ithelp@phila.gov to create a new support ticket, and copy maps@phila.gov on the email.
# Request that IT Help route the ticket to CityGeo.
# Describe the application that will be using AIS and provide a URL if possible.


# request AIS for X and Y coordinates
def ais_request(address_string,srid):
    '''
    :param address_string:
    :param srid:
    :return: list containing X and Y coordinates
    '''
    params = gatekeeperKey
    request_str = ais_qry.format(ais_url=ais_url, geocode_field=address_string,srid=geocode_srid)
    try:
        r = requests.get(request_str, params=params)
        if r.status_code == 404:
            print('404 error')
            logging.info(request_str)
            raise
    except Exception as e:
        print("Failed AIS request")
        logging.info('''failed request for {}'''.format(address_string))
        logging.info(request_str)
        raise e
    # extract coordinates from json request response
    feats = r.json()['features'][0]
    geo = feats.get('geometry')
    coords = geo.get('coordinates')
    return coords


# request tomtom for X and Y coordinates
def tomtom_request(address='no address', city=None, state= None,zip=None,srid=2272):
    '''
    :param address_string: string
    :param srid:
    :return: list containing X and Y coordinates
    '''
    s = address.split(' ')
    address = '+'.join(s)
    request_str = my_tomtom_qry.format(address = address, city = city,
                                    state = state, zip = zip, srid = srid)
    # send request to tomtom
    try:
        r = requests.get(request_str)
    except Exception as e:
        #print("Failed tomtom request")
        logging.info(request_str)
        raise e
    # try to get a top address candidate if any
    try:
        top_candidate = r.json().get('candidates')[0].get('location')
        top_candidate = [top_candidate.get('x'), top_candidate.get('y')]
    except:
        #print('failed to geocode ', street_str)
        logging.info('''failed tomtom request for {}'''.format(address))
        logging.info(request_str)
        logging.info('')
        return ['NA','NA']
    return top_candidate


def main():
    # Logging Params:
    today = datetime.date.today()
    logfile = 'ais_geocode_log3_{}.txt'.format(today)
    logging.basicConfig(filename=logfile, level=logging.INFO,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    logging.info('''\n\n------------------------------Geocoding Sample Data------------------------------''')
    # download address summary using requests
    # response = requests.get('https://opendata-downloads.s3.amazonaws.com/address_summary.csv')
    # lines = response.text.splitlines()
    # mydata = [s.split(',') for s in lines]
    # # save address summary table to memory
    # with open('address_summary_fields.csv', 'w', encoding='UTF8', newline='') as f:
    #     writer = csv.writer(f)
    #     writer.writerows(mydata)
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
    philly_zipcodes = [int(z) for z in philly_zipcodes]

    # get input address test data from input csv
    input_addresses = etl.fromcsv('list_of_real_usa_addresses.csv')
    cityresult=0
    stateresult=0
    zipresult = 0
    #passayunk instance
    parser = PassyunkParser()
    dict_frame = []
    # try to get state and city using open source parser
    for row in input_addresses[1:]:
        ################################################################
        # if city is philly:
        #     use passayunk
        # elif not city:
        #     use oparser
        #     if zip is in philly or city is philly:
        #################################################################
        input_dict = dict(zip(input_addresses[0],row))
        input_dict['std_address'] = None
        address_full = "{} {} {} {}".format(input_dict.get('address'),
                                            input_dict.get('city'),
                                            input_dict.get('state'),
                                            input_dict.get('zip'))

        # if input city or input zip relates to philly
        if input_dict.get('city') and input_dict.get('city').lower() == 'philadelphia' or \
                (input_dict.get('zip') and input_dict.get('zip') in philly_zipcodes):
            # parse address using passayunk
            std_address = parser.parse(input_dict.get('address'))
            input_dict['std_address'] = std_address['components']['output_address']

        # if no city input or city input is not philly and zip not in philly
        if (not input_dict.get('city') or input_dict.get('city') != 'philadelphia')\
                and (input_dict.get('zip') and input_dict.get('zip') not in philly_zipcodes):
            # parse full address using open source parser
            parser_response = parse_location(address_full)
            if (parser_response and parser_response.get('zip') and parser_response.get('zip') in philly_zipcodes)\
                or (parser_response and parser_response.get('city') and parser_response.get('city').lower() == 'philadelphia'):
                std_address = parser.parse(address_full)
                input_dict['std_address'] = std_address['components']['output_address']

            # else:# fill in city or state which seems unecessary?
            #     #mydict['std_address'] = None
            if parser_response and parser_response.get('city') and not input_dict.get('city'):
                input_dict['city'] = parser_response.get('city')
                cityresult = cityresult + 1
            if parser_response and parser_response.get('state') and not input_dict.get('state'):
                input_dict['state'] =parser_response.get('state')
                stateresult = stateresult + 1
            if parser_response and parser_response.get('zip') and not input_dict.get('zip'):
                input_dict['zip'] =parser_response.get('zip')
                zipresult = zipresult+1

        dict_frame.append(input_dict)
    # input addresses with std_address field
    header = list(input_addresses[0]).append('std_address')
    input_addresses = etl.fromdicts(dict_frame, header=header)


    #join input data with address summary table data on standardized street address column
    joined_addresses_to_address_summary = etl.leftjoin(input_addresses, address_summary_rows, lkey='std_address', rkey='street_address', presorted=False )


    dict_frame = []

    # use apis to get coordinates (AIS for philly addresses and tomtom for addresses outside of philly)
    for row in joined_addresses_to_address_summary[1:]:
        ################################################################
        # if city or zip is philly:
        #     use ais
        # elif city and zip not philly  :
        #     use tomtom
        #     if zip is in philly or city is philly:
        #################################################################
        rowzip = dict(zip(joined_addresses_to_address_summary[0], row))
        address_full = "{} {} {} {}".format(rowzip.get('address'),
                                            rowzip.get('city'),
                                            rowzip.get('state'),
                                            rowzip.get('zip'))
        # address already has coordinates from join
        if rowzip.get('x_coordinate'):
            dict_frame.append(rowzip)
            continue
        # address does not have coordinates from join
        else:
            geocoded = None
            # if city or  zip relates to philly request ais
            if rowzip.get('city') and rowzip.get('city').lower() == 'philadelphia' or \
                    (rowzip.get('zip') and rowzip.get('zip') in philly_zipcodes):
                geocoded = ais_request(rowzip.get('address'), str(geocode_srid))
            #if city and zip not philly use tomtom
            elif (not input_dict.get('city') or input_dict.get('city') != 'philadelphia') \
                    and (input_dict.get('zip') and input_dict.get('zip') not in philly_zipcodes):
                geocoded = tomtom_request(address=rowzip.get('address'), city=rowzip.get('city'),
                                          state= rowzip.get('state'),zip=rowzip.get('zip'),srid=geocode_srid)

            if geocoded:
                rowzip['x_coordinate'] = geocoded[0]
                rowzip['y_coordinate'] = geocoded[1]
                dict_frame.append(rowzip)
            else:
                logging.info('''unable to geocode {}'''.format(address_full))

    addresses = etl.fromdicts(dict_frame, header=joined_addresses_to_address_summary[0])
    print('address output')
    print(etl.look(addresses,limit=22))
    addresses.tocsv('geocoded_sample_output_{}.csv'.format(geocode_srid))


if __name__ == "__main__":
    main()
