import csv
from config import aisCredentials
import requests

def ais_request(address_string):
    ais_url = aisCredentials['url']
    params = {'gatekeeperKey': aisCredentials['gatekeeperKey']}
    request = "{ais_url}{geocode_field}".format(ais_url=ais_url, geocode_field=address_string)
    # send request to tomtom
    try:
        r = requests.get(request, params=params)
        print('ais request for ', address_string)
        print(r)
    except Exception as e:
        logging.ERROR("Failed AIS request")
        raise e

    # extract coordinates from json request response
    feats = r.json()['features'][0]
    geo = feats.get('geometry')
    coords = geo.get('coordinates')
    coords = [str(c) for c in coords]
    coords = ' '.join(coords)
    opanum = feats.get('opa_account_num')
    return coords


# return tomtom first candidate address
def tomtom_request(street_str):
    s = street_address.split(' ')
    address = '+'.join(s)
    request_str = '''https://citygeo-geocoder-aws.phila.city/arcgis/rest/services/TomTom/US_StreetAddress/GeocodeServer/findAddressCandidates?Street={}
                &City=&State=&ZIP=&Single+Line+Input=&outFields=&maxLocations=&matchOutOfRange=true&langCode=&locationType=&sourceCountry=&category=
                &location=&distance=&searchExtent=&outSR=&magicKey=&f=pjson'''.format(address)
    # send request to tomtom
    try:
        r = requests.get(request_str)
        print('got tomtom request ')
    except Exception as e:
        print("Failed tomtom request")
        raise e
    # try to get a top address candidate if any
    try:
        top_candidate =  r.json().get('candidates')[0].get('location')
        top_candidate = '{} {}'.format(str(top_candidate.get('x')), str(top_candidate.get('y')))
    except:
        return 'tom tom unable to geocode'
    return top_candidate

## NEW LOGIC
# if city column
#   if citycol_val == phila, philadelphia etc
        # send to ais
    #else:
        #send to tomtom
#else:
    #if 'phil or philadedlephia etc' in streetadress val :
        #send to ais
    # else
        #try:
            # send to ais
        #except:
            # send to tomtom

i =0
with open('ais_geocoding_example_input.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    new_rows = []
    for row in csv_reader:
        if i == 0:
            # add coordinates to new header
            row.append('coordinates')
            new_rows.append(row)
        else: # for every row except header
            street_address =row[1]
            city_col =row[2]

            if city_col:
                # if philadelphia in city val use ais else try tomtom
                if city_col== 'philadelphia':
                    geocoded = ais_request(street_address)
                else: # city is not philly
                    print('tomtom get {} {}'.format(street_address,city_col))
                    geocoded = tomtom_request(street_address)
                    print('tomtom geocoded ', geocoded)

            else:# if no city column
                # if philadelphia in address use ais else try tomtom
                if 'philadelphia' in street_address:
                    geocoded = ais_request(street_address)
                else:
                    try:
                        geocoded = ais_request(street_address)
                    except:
                        geocoded= tomtom_request(street_address)

            #this_list.append(geocoded)
            row.append(geocoded)
            new_rows.append(row)
        i=i+1

# write output in csv file
with open("out_test.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerows(new_rows)
