import petl as etl
import geopetl
from passyunk.parser import PassyunkParser
import requests
import cx_Oracle
import csv
from config import aisCredentials, source_creds,geocode_srid

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
    ais_url = aisCredentials['url']
    params = {'gatekeeperKey': aisCredentials['gatekeeperKey']}
    request = "{ais_url}{geocode_field}".format(ais_url=ais_url, geocode_field=address_string)
    request = request+'?srid='+srid
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
    request_str = '''https://citygeo-geocoder-aws.phila.city/arcgis/rest/services/TomTom/US_StreetAddress/GeocodeServer/findAddressCandidates?Street={}
                &City=&State=&ZIP=&Single+Line+Input=&outFields=&maxLocations=&matchOutOfRange=true&langCode=&locationType=&sourceCountry=&category=
                &location=&distance=&searchExtent=&outSR={}&magicKey=&f=pjson'''.format(address,srid)
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
    # conect to source table
    source_dsn = cx_Oracle.makedsn(source_creds.get('host'),
                                   source_creds.get('port'),
                                   service_name=source_creds.get('service_name'))
    source_conn = cx_Oracle.connect(source_creds.get('user'), source_creds.get('password'), source_dsn)

    # required address_summary (source) table fields
    if geocode_srid == 2272:
        adrsum_fields = ['street_address', 'geocode_x', 'geocode_y']
    else:
        adrsum_fields = ['street_address', 'geocode_lon', 'geocode_lat']

    # download address summary using requests
    response = requests.get('https://opendata-downloads.s3.amazonaws.com/address_summary.csv')
    lines = response.text.splitlines()
    mydata = [s.split(',') for s in lines]
    # save address summary table to memory
    with open('address_summary_fields.csv', 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(mydata)
    # load address summary data from csv to petl frame
    address_summary_rows = etl.fromcsv('address_summary_fields.csv').cut(adrsum_fields)

    # get input address test data from input csv
    input_address = etl.fromcsv('ais_geocoding_example_input.csv')

    #passayunk instance
    parser = PassyunkParser()

    # add standardized address column to input csv using passyunk parser
    input_address = input_address.addfield('address_std', lambda p: parser.parse(p.street_address)['components']['output_address'])

    #join input data with address summary table data on standardized street address column
    joined_addresses_to_address_summary = etl.leftjoin(input_address, address_summary_rows, lkey='address_std', rkey='street_address', presorted=False )

    # new joined table header with standardized street address field
    header = list(etl.fieldnames(joined_addresses_to_address_summary))

    #empty list to store row with coordinates
    newlist = []

    # iterate over rows to geocode each address
    for row in joined_addresses_to_address_summary[1:]:
        rowzip = dict(zip(header, row))  # dictionary from etl data
        #if there is a longitude or x coordinates field retrieved from joining wiht address summary table,  continue
        if rowzip.get('geocode_lon') or rowzip.get('geocode_x'):
            newlist.append(rowzip)
            continue
        # if city column provided and is Philadelphia -> use AIS to geocode, if not Philadelphia use TomTom
        elif rowzip.get('city'):
            if rowzip.get('city') == 'philadelphia':
                geocoded = ais_request(rowzip.get('street_address'),str(geocode_srid))
            else:  # city is not philly so use tomtom
                geocoded = tomtom_request(rowzip.get('street_address'),str(geocode_srid))
        # if no city column available, try AIS then TomTom
        else:
            try:
                geocoded = ais_request(rowzip.get('street_address'), str(geocode_srid))
            except:
                geocoded = tomtom_request(rowzip.get('street_address'), str(geocode_srid))

        #insert geocoded coordinates in row with corresponding desired SRID value
        if geocode_srid== 2272:
            rowzip['geocode_x'] = geocoded[0]
            rowzip['geocode_y'] = geocoded[1]
        elif geocode_srid== 4326:
            rowzip['geocode_lon'] = geocoded[0]
            rowzip['geocode_lat'] = geocoded[1]
        # append result row
        newlist.append(rowzip)

    # write new geocoded coordinate results to memory
    newframe = etl.fromdicts(newlist,header=header)
    newframe.tocsv('geocoded_output_{}.csv'.format(geocode_srid))


if __name__ == "__main__":
    main()
