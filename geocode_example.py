import petl as etl
import geopetl
from passyunk.parser import PassyunkParser
import requests
import cx_Oracle
from config import aisCredentials, source_creds, geocode_srid
# I feel like much of these comments should go in a README document, not necessarily in the code file itself

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

def ais_request(address_string,srid):
    '''
    Request AIS for X and Y coordinates
    :param address_string:
    :param srid:
    :return: list containing X and Y coordinates
    '''
    ais_url = aisCredentials['url']
    params = {'gatekeeperKey': aisCredentials['gatekeeperKey'], 'srid': srid}
    request = "{ais_url}{geocode_field}".format(ais_url=ais_url, geocode_field=address_string)
    try:
        r = requests.get(request, params=params)
        r.raise_for_status() # Will flag 404 error or any other 400+ or 500+ error
    except Exception as e:
        print("Failed AIS request")
        raise e
    # extract coordinates from json request response
    feats = r.json()['features'][0]
    geo = feats.get('geometry')
    coords = geo.get('coordinates')
    return coords

def tomtom_request(street_str,srid):
    '''
    Request tomtom for X and Y coordinates
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
    '''
    Can we put here a general description of what main() is doing ?
    '''
    source_dsn = cx_Oracle.makedsn(source_creds.get('host'),
                                   source_creds.get('port'),
                                   service_name=source_creds.get('service_name'))
    source_conn = cx_Oracle.connect(source_creds.get('user'), source_creds.get('password'), source_dsn)

    if geocode_srid == 2272:
        adrsum_fields = ['street_address', 'geocode_x', 'geocode_y']
    else:
        adrsum_fields = ['street_address', 'geocode_lon', 'geocode_lat']

    address_summary_rows = etl.fromoraclesde(source_conn, 'ADDRESS_SUMMARY',fields=adrsum_fields, limit=5000)
    input_address = etl.fromcsv('ais_geocoding_example_input.csv')
    parser = PassyunkParser()
    input_address = input_address.addfield('address_std', lambda p: parser.parse(p.street_address)['components']['output_address'])
    
    joined_addresses_to_address_summary = etl.leftjoin(input_address, address_summary_rows, lkey='address_std', rkey='street_address', presorted=False )
    header = list(etl.fieldnames(joined_addresses_to_address_summary))

    newlist = []
    # iterate over rows to geocode each address
    for row in joined_addresses_to_address_summary[1:]:
        rowzip = dict(zip(header, row))  # dictionary from etl data
        if rowzip.get('geocode_lon') or rowzip.get('geocode_x'):
            newlist.append(rowzip)
            continue
#       Could the below code block be simplified to...? 
#         try:
#             geocoded = ais_request(rowzip.get('street_address'), str(geocode_srid))
#         except:
#             geocoded = tomtom_request(rowzip.get('street_address'), str(geocode_srid))        
#       If not, no worries

        # if city column provided and is Philadelphia -> use AIS to geocode, if not Philadelphia use TomTom
        elif rowzip.get('city'):
            if rowzip.get('city') == 'philadelphia':
                geocoded = ais_request(rowzip.get('street_address'),str(geocode_srid))
            else:  # city is not philly so use tomtom
                geocoded = tomtom_request(rowzip.get('street_address'),str(geocode_srid))
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

        newlist.append(rowzip)

    newframe = etl.fromdicts(newlist,header=header)
    newframe.tocsv('geocoded_output_{}.csv'.format(geocode_srid))


if __name__ == "__main__":
    main()
