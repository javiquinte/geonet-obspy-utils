# An ObsPy Client to retrieve seismic data from the GeoNet Open Data Bucket 

This repository contains Python classes for retrieving seismic waveform and 
event data from the GeoNet AWS S3 buckets as [ObsPy](https://docs.obspy.org/#)
stream and catalogue objects.

Codes written by [Pasan Herath](mailto:p.herath@gns.cri.nz)

**Please note that this service is not suitable for real-time data retrieval. 
If you need to retrieve data from within the last 7 days, please use FDSN.**

**For your projects that do not require near real-time data, please use this 
libary to retrieve data.**

*If you discover any bugs or would like to see any features added, please raise
an issue, and we will look into it*

Thanks very much for using this plugin in your work, and it will help reduce
the demand for our FDSN services. 



## Installing the plugin

1. Clone the repository into a directory of your choice
2. Create a Python environment using conda (or other) or use an existing Python
environment
2. Open a terminal in the directory where the repository was cloned to and run
 `cd geonet-obspy-utils` 
3. Then run `pip install .`

This will install the geonet-obspy-plugin into the selected Python environment.

## Testing the installation

1. To test the installation, run `pytest` 


## Using the plugin

### Downloading MSEED waveforms into an ObsPy stream object by querying
```
# import the libraries
from obspy import UTCDateTime
from geonet_obspy_utils.clients.aws.client import Client

# initialize client
client = Client("GEONET")

# define start and end times
starttime = UTCDateTime("2024-03-20T16:59:00")
endtime = UTCDateTime("2024-03-20T18:05:00")

# request stream by querying
stream = client.get_waveforms(network = "NZ", 
                              station = "DCZ,JCZ", 
                              location = "10,?", 
                              channel = "HH*, EH*", 
                               starttime = starttime, 
                               endtime = endtime, 
                               max_threads=4)

print (stream)
```

### Downloading MSEED waveforms into an ObsPy stream object using file name
```
# import the libraries
from geonet_obspy_utils.clients.aws.client import Client

# initialize client
client = Client("GEONET")

# input file name
## file name structure for GeoNet:
## "station.network/year.julday.station.location-channel.network.D"
fname = "DCZ.NZ/2023.125.DCZ.10-HHZ.NZ.D"
stream = client.read(fname)
print (stream)

```

### Request event xmls into an ObsPy catalogue oject
```
# import the libraries
from geonet_obspy_utils.clients.aws.client import Client

# initialize client
client = Client("GEONET")

starttime = UTCDateTime("2021-03-20T16:59:00")
endtime = UTCDateTime("2024-03-20T18:05:00")

minlatitude = -50
maxlatitude = -30
minlongitude = 160
maxlongitude = -175
mindepth = 0
maxdepth = 10
minmagnitude = 3
maxmagnitude = 6

cat = client.get_events(starttime=starttime, endtime=endtime,
                            minlatitude=minlatitude, maxlatitude=maxlatitude,
                            minlongitude=minlongitude,
                            maxlongitude=maxlongitude,
                            mindepth=mindepth, maxdepth=maxdepth,
                            minmagnitude=minmagnitude,
                            maxmagnitude=maxmagnitude)

print (cat)

```

