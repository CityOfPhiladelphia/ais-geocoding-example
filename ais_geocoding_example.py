import csv
import logging
import requests
from retrying import retry
import datetime
from smart_open import open

# Logging Params:
today = datetime.date.today()
logfile = 'ais_geocode_log_{}.txt'.format(today)
logging.basicConfig(filename=logfile, level=logging.INFO, format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')

# I/O Params:
input_file = 'ais_geocode_example_input.csv' # Name of input file
output_file = 'ais_geocode_example_output.csv' # Name of output file

# Request Params:
ais_url = 'http://api-test.phila.gov/ais/v1/search/'
geocode_field = 'street_address' # Field in input file to send to API for geocoding
input_fields_for_output = [''] # List of fields in input file to include in output file. If none then will use include all input fields in output.
ais_response_fields_for_output = ['opa_account_num', 'lon', 'lat'] # List of AIS response fields to include in output file
gatekeeper_key = 'ENTER GATEKEEPER KEY HERE' # Enter your gatekeeper key given to you for your geocoding usage
params = {'gatekeeperKey': gatekeeper_key} # Add other key/value pairs of params (see options in docs @ https://github.com/CityOfPhiladelphia/ais/blob/master/docs/APIUSAGE.md

# Setup:
session = requests.session()
out_rows = None
if type(input_fields_for_output) == list:
    input_fields_for_output = [f for f in input_fields_for_output if f]
if not input_fields_for_output:
    input_fields_for_output = None

# Geocode function:
@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=5)
def geocode(request):
    global params
    try:
        response = session.get(request, params=params, timeout=10)
    except Exception as e:
        raise e
    return response.json()

# Geocode input:
print("Starting geocoding...")
with open(input_file, mode='r', encoding='utf-8') as input_stream:
    with open(output_file, mode='w', encoding='utf-8') as output_stream:
        rows = csv.DictReader(input_stream)
        if not input_fields_for_output:
            input_fields_for_output = rows.fieldnames
        header = input_fields_for_output + ais_response_fields_for_output if input_fields_for_output else ais_response_fields_for_output
        for row in rows:
            row_output = {}
            if input_fields_for_output:
                for field in input_fields_for_output:
                    row_output[field] = row.get(field)
            geocode_field_value = row.get(geocode_field)
            request = "{ais_url}{geocode_field}".format(ais_url=ais_url, geocode_field=geocode_field_value)
            json_response = geocode(request)
            if json_response and 'features' in json_response and len(json_response['features']) > 0:
                feature = json_response['features'][0]
                properties = feature.get('properties', '')
                geometry = feature.get('geometry', '')
                for field in ais_response_fields_for_output:
                    if field == 'lon' or field == 'longitude' and geometry:
                        row_output[field] = geometry['coordinates'][0]
                    elif field == 'lat' or field == 'latitude' and geometry:
                        row_output[field] = geometry['coordinates'][1]
                    elif properties:
                        row_output[field] = properties.get(field, '')
            else:
                logging.warning('Could not geocode "{}"'.format(geocode_field_value))
            if out_rows == None:
                out_rows = csv.DictWriter(output_stream, header, lineterminator = '\n')
                out_rows.writeheader()
            out_rows.writerow(row_output)
