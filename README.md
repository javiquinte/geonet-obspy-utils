# geonet-obspy-utils

This repository contains Python classes for
retrieving seismic waveform and event data
from the GeoNet AWS S3 buckets as ObsPy 
stream and catalogue objects.

Codes written by Pasan Herath 
p.herath @gns.cri.nz


## Installation

1. Clone the repository
2. `cd geonet-obspy-utils` 
3. `pip install .`

## Testing the installation

1. `pytest` 


## Usage

```
# import the libraries
from obspy import UTCDateTime, read_events
from geonet_obspy_utils.clients.aws.client import Client

# initialize client
client = Client("GEONET")

# define start and end times
starttime = UTCDateTime("2024-03-20T16:59:00")
endtime = UTCDateTime("2024-03-20T18:05:00")

# request stream
stream = client.get_waveforms(network = "NZ", 
                              station = "DCZ", 
                              location = "*", 
                              channel = "HH*", 
                               starttime, 
                               endtime, 
                               max_threads=4)

print (stream)

# request events
cat = client.get_events(starttime, endtime)

print (cat)

```
