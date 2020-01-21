import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="sample-ais-geocoder", 
    version="0.0.1",
    author="CityGeo",
    author_email="keisan.gittens@phila.gov",
    description="Python package for geocoding addresses using the AIS API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    package_data={
        '':['*csv'],
    },
    url="https://github.com/CityOfPhiladelphia/ais-geocoding-example",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=['requests', ' retrying', 'smart_open']

)