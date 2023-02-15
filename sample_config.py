# Config file
import requests
import logging

gatekeeperKey : 'XXXXXXXX'
ais_url='https://api.phila.gov/ais/v1/search/'
geocode_srid = 2272 # 4326
ais_qry = "aisqry"
tomtom_qry = '''tomtom'''

# request AIS for X and Y coordinates
def ais_request(address_string,srid):
    '''
    :param address_string:
    :param srid: integer
    :return: list containing X and Y coordinates
    '''
    params = gatekeeperKey
    request_str = ais_qry.format(ais_url=ais_url, geocode_field=address_string,srid=srid)
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
    :param srid: integer
    :return: list containing X and Y coordinates
    '''
    s = address.split(' ')
    address = '+'.join(s)
    request_str = tomtom_qry.format(address=address, city=city, state=state, zip=zip, srid=srid)
    try:
        r = requests.get(request_str)
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

