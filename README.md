# Example AIS Geocoder Package
This package is intended to serve as an example of how to use the AIS API with Python. 
It is designed as an out-of-the-box tool which takes an input csv containing an address or account number field to be sent to the API, a config file specifying the input fields as well as the AIS repsonse fields to be outputted and the field that represents the data to be geocoded, and outputs the data to an output csv.

## Installation
_Before installing, it is advised to create and activate a virtual environment in order to isolate the dependencies for this project._

This package can be installed by opening the command line, from a directory of your choice on your local machine via this command: 
 
    pip install git+https://github.com/CityOfPhiladelphia/ais-geocoding-example.git
    

## Usage
#### Setup
From the project files, look for the file called [sample_config.py](sample_config.py). In order for this package to work, you will need to change the following variables: 

- `input_file`: The name of the input csv file with addresses/account #s you need geocoded. (This should have a clean address or account number field, [for example](ais_geocoding_example_input.csv)).
- `output_file`: is the name of the output file you want the package to create.
- `geocode_field`: Name of field in input file that contains the data to be sent to the API. This field can be an _address, block, intersection, OPA account number, Regmap ID and coordinates_. Our sample specifies an address. See documentation for [details](https://github.com/CityOfPhiladelphia/ais/blob/master/docs/APIUSAGE.md#search). 
- `input_fields_for_output`: A list of fields from the input csv that should be included in the output csv. If left empty, all fields from the input csv will be included in the output csv.
- `ais_response_fields_for_output` List of fields from the AIS API response object to be included in the output csv. [Other fields](https://github.com/CityOfPhiladelphia/ais/blob/master/docs/APIUSAGE.md#ais-feature-types) can additionally be included. 
- `gatekeeper_key` this key is necessary for the api to work. Request one by emailing maps@phila.gov 

#### Run
Once these parameters are changed and match your needs, activate python from the command line/terminal and run the `ais_geocoding_example.py` script or simply run the script in an IDE of your choice. 

A logfile will appear in your directory with detailed information about what addresses have worked or failed. Its name will look something like this `ais_geocode_log3_{}.txt`. 

Your geocoded addresses will be in a csv format in the path you designated earlier.  
