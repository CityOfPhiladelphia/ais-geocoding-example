# Example AIS & TOMTOM Geocoder Package
This package is intended to serve as an example of how to use the AIS API and TOMTOM API with Python. 
It is designed to import a table containing address, city, and zip code information and a user-defined variable, SRID, to standardize the address and
then obtain the respective geographical coordinates. 

1. the script, 'geocode_opa_property_summary.py', uses parsers Passyunk and Addresser to standardized the address value of each row
https://github.com/CityOfPhiladelphia/passyunk
https://pypi.org/project/addresser/
2. then the table joins with ais address summary table on the standardized address column and obtain the coordinates for some of the addresses
3. For the remainder of the missing coordinates we use TOMTOM or AIS APIs to request coordinates 


## Installation
_Before installing, it is advised to create and activate a virtual environment in order to isolate the dependencies for this project._

This package can be installed by opening the command line, from a directory of your choice on your local machine via this command: 
 
    pip install git+https://github.com/CityOfPhiladelphia/ais-geocoding-example.git
    

## Usage
#### Setup
From the project files, you will need access to opa property summary table, need to rename the (sample_config.py) to config.py and need to change the following variables: 

- 'srid' can be set to 2272 or 4326.
- 'gatekeeper_key' this key is necessary for the api to work. Request one by emailing maps@phila.gov 

#### Run
python geocode_opa_property_summary.py

A logfile will appear in your directory with detailed information about what addresses have worked or failed. Its name will look something like this `geocode_opa_data_{srid}.txt`. 

Geocoded addresses will be in a csv format in the 'geocoded_opa_output_{srid}.csv'.  
