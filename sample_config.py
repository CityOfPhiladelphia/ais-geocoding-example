# Config file

# I/O Params:
input_file = "C:\Projects\\ais-geocoding-example\\ais_geocode_example_input.csv" # path to your file to be geocoded
output_file = "C:\Projects\\ais-geocoding-example\\ais_geocode_example_output.csv" # path to your output file

ais_url = 'https://api.phila.gov/ais/v1/search/' 
geocode_field = 'street_address' # Field in input file to send to API for geocoding
input_fields_for_output = [''] # List of fields in input file to include in output file. If none then will use include all input fields in output.
ais_response_fields_for_output = ['opa_account_num', 'lon', 'lat'] # Sample list of AIS response fields to include in output file
gatekeeper_key = 'XXXXXXXX' # Enter your gatekeeper key given to you for your geocoding usage
params = {'gatekeeperKey': gatekeeper_key} # Add other key/value pairs of params (see options in docs @ https://github.com/CityOfPhiladelphia/ais/blob/master/docs/APIUSAGE.md


aisCredentials = {'gatekeeperKey' : 'XXXXXXXX',
                  'url':'https://api.phila.gov/ais/v1/search/'}

source_creds = {'host': '',
                'port': '',
                'service_name':'',
                'user':'',
                'password':''
                }
geocode_srid = 2272 # 4326
