import petl as etl
import geopetl
from passyunk.parser import PassyunkParser
import requests
import csv
from config import ais_url, gatekeeperKey, source_creds, geocode_srid, ais_qry, tomtom_qry
from addresser import parse_location


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


# request AIS for X and Y coordinates
def ais_request(address_string,srid):
    '''
    :param address_string:
    :param srid:
    :return: list containing X and Y coordinates
    '''
    params = gatekeeperKey
    request = ais_qry.format(ais_url=ais_url, geocode_field=address_string,srid=geocode_srid)
    try:
        r = requests.get(request, params=params)
        if r.status_code == 404:
            print('404 error')
            raise
    except Exception as e:
        print("Failed AIS request")
        raise e
    # extract coordinates from json request response
    feats = r.json()['features'][0]
    geo = feats.get('geometry')
    coords = geo.get('coordinates')
    return coords


# request tomtom for X and Y coordinates
def tomtom_request(street_str,srid):
    '''
    :param street_str: string
    :param srid:
    :return: list containing X and Y coordinates
    '''
    s = street_str.split(' ')
    address = '+'.join(s)
    request_str = tomtom_qry.format(address,srid)
    # send request to tomtom
    try:
        r = requests.get(request_str)
    except Exception as e:
        print("Failed tomtom request")
        raise e
    # try to get a top address candidate if any
    try:
        top_candidate =  r.json().get('candidates')[0].get('location')
        top_candidate = [top_candidate.get('x') ,top_candidate.get('y')]
    except:
        print('failed to geocode ', street_str)
        return ['NA','NA']
    return top_candidate


def main():

    # download address summary using requests
    response = requests.get('https://opendata-downloads.s3.amazonaws.com/address_summary.csv')
    lines = response.text.splitlines()
    mydata = [s.split(',') for s in lines]
    # save address summary table to memory
    with open('address_summary_fields.csv', 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(mydata)
    # required address_summary (source) table fields
    if geocode_srid == 2272:
        adrsum_fields = ['street_address', 'geocode_x', 'geocode_y']
        # load address summary data from csv to petl frame
        address_summary_rows = etl.fromcsv('address_summary_fields.csv').cut(adrsum_fields)
        address_summary_rows = etl.rename(address_summary_rows,
                                          {'geocode_x': 'x_coordinate', 'geocode_y': 'y_coordinate'})
    elif geocode_srid == 4326:
        adrsum_fields = ['street_address', 'geocode_lon', 'geocode_lat']
        # load address summary data from csv to petl frame
        address_summary_rows = etl.fromcsv('address_summary_fields.csv').cut(adrsum_fields)
        address_summary_rows = etl.rename(address_summary_rows,
                                          {'geocode_lon': 'x_coordinate', 'geocode_lat': 'y_coordinate'})
    else:
        print('invalid srid')
        raise

    # # load address summary data from csv to petl frame
    # address_summary_rows = etl.fromcsv('address_summary_fields.csv').cut(adrsum_fields)
    # if geocode_srid == 2272:
    #     address_summary_rows = etl.rename(address_summary_rows, {'geocode_x': 'x_coordinate', 'geocode_y': 'y_coordinate'})
    # elif geocode_srid == 4326:
    #     address_summary_rows = etl.rename(address_summary_rows,{'geocode_lon': 'x_coordinate', 'geocode_lat': 'y_coordinate'})
    #

    # get input address test data from input csv
    input_addresses = etl.fromcsv('ais_geocoding_example_input.csv')

    #passayunk instance
    parser = PassyunkParser()
    dict_frame = []
    # try to get state and city using open source parser
    for row in input_addresses[1:]:
        mydict = dict(zip(input_addresses[0],row))
        # try to get state info
        if not mydict.get('state'):
            res = parse_location(mydict.get('street_address'))
            if res.get('state'):
                mydict['state'] = res.get('state')
        # try to get city info
        if not mydict.get('city') or mydict.get('city').lower() != 'philadelphia':
            res = parse_location(mydict['street_address'])
            if res.get('city'):
                mydict['city'] = res.get('city')
        # if city is philly use passayunk to standardize street address
        if mydict.get('city').lower() == 'philadelphia':
            d = parser.parse(mydict.get('street_address'))
            mydict['address_std']=d['components']['output_address']
        else:
            # leave std address as none???
            mydict['address_std'] = None
        dict_frame.append(mydict)
    # input addresses with std_address field
    header = list(input_addresses[0]).append('address_std')
    input_addresses = etl.fromdicts(dict_frame, header=header)

    #join input data with address summary table data on standardized street address column
    joined_addresses_to_address_summary = etl.leftjoin(input_addresses, address_summary_rows, lkey='address_std', rkey='street_address', presorted=False )
    print('addresses joined')
    print(etl.look(joined_addresses_to_address_summary, limit=12))
    dict_frame = []
    for row in joined_addresses_to_address_summary[1:]:
        rowzip = dict(zip(joined_addresses_to_address_summary[0], row))
        if rowzip.get('x_coordinate'):
            #address already has coordinates
            dict_frame.append(rowzip)
            continue
        else:
            # if we have standardized address use them for apis else use street address field
            if rowzip.get('address_std'):
                try:
                    geocoded = ais_request(rowzip.get('address_std'), str(geocode_srid))
                except:
                    geocoded = tomtom_request(rowzip.get('address_std'), str(geocode_srid))
            else:
                try:
                    geocoded = ais_request(rowzip.get('street_address'), str(geocode_srid))
                except:
                    geocoded = tomtom_request(rowzip.get('street_address'), str(geocode_srid))
            rowzip['x_coordinate'] = geocoded[0]
            rowzip['y_coordinate'] = geocoded[1]
            dict_frame.append(rowzip)

    addresses = etl.fromdicts(dict_frame, header=joined_addresses_to_address_summary[0])
    addresses.tocsv('geocoded_output_{}.csv'.format(geocode_srid))


if __name__ == "__main__":
    main()
