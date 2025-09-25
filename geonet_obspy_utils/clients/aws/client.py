#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
GeoNet AWS  client for ObsPy.

"""


from obspy import UTCDateTime, Stream, Trace, read_events, Catalog
import fnmatch
import tempfile 
import yaml
import os 
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from mseedlib import MSTraceList, sourceid2nslc
import numpy as np
from  concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import requests
from io import StringIO, BytesIO
from tqdm import tqdm



class Client:
    def __init__(self, base_name):
        """
        Initialize the client with the base configuration loaded from a YAML file.
        """
        config_path = os.path.join(os.path.dirname(__file__), "client_config.yml")
        with open(config_path, 'r') as f:
            base_config = yaml.safe_load(f)

        if base_name.upper() in base_config:
            self.config = base_config[base_name.upper()]
        else:
            raise ValueError(f"Unknown base name: {base_name}")
        
        self.waveform_bucket_name = self.config["waveform_bucket_name"]
        self.event_bucket_name = self.config["event_bucket_name"]
        self.base_url = self.config["base_url"]
        self.waveform_dir = self.config["waveform_dir"]
        self.year_day_format = self.config["year_day_format"]
        self.mseed_file_format = self.config["mseed_file_format"]
    
    @property
    def _s3(self):
        return boto3.client("s3", config=Config(signature_version=UNSIGNED))

    @property
    def _s3_waveform(self):
        """ S3 access as property to enforce instantiation for individual threads. """
        waveform_bob = boto3.resource('s3', config=Config(signature_version=UNSIGNED))
        return waveform_bob.Bucket(self.waveform_bucket_name)
    
    @property
    def _s3_event(self):
        """ S3 access as property to enforce instantiation for individual threads. """
        event_bob = boto3.resource('s3', config=Config(signature_version=UNSIGNED))
        return event_bob.Bucket(self.event_bucket_name)


    def _list_available_files(self, prefix):
        """
        List files under the given prefix using boto3 and anonymous access.
        """
        file_list = []
        # self._s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))  # client, not resource
        paginator = self._s3.get_paginator('list_objects_v2')
        try:
            for page in paginator.paginate(Bucket=self.waveform_bucket_name, Prefix=prefix):
                contents = page.get("Contents", [])
                
                file_list.extend([item["Key"] for item in contents])
        except self._s3.exceptions.NoSuchBucket:
            print(f"Bucket '{self.waveform_bucket_name}' does not exist.")
        except Exception as e:
            print(f"Error accessing S3 bucket '{self.bucket_name}': {e}")
        return file_list
    
    
    
    def get_waveforms(self, network, station, location, channel, starttime, endtime, max_threads=1):
        """
        Fetch MiniSEED waveform data from the given source within the specified time range.

        Parameters:
        - network (str): Network code. Supports wildcards '*' (any number of characters) 
                         and '?' (single character).
        - station (str): Station code. Supports wildcards '*' and '?'.
        - location (str): Location code. Supports wildcards '*' and '?'.
        - channel (str): Channel code. Supports wildcards '*' and '?'.
        - starttime (UTCDateTime): Start time of the waveform data.
        - endtime (UTCDateTime): End time of the waveform data.

        Returns:
        - Stream: An ObsPy Stream object containing the waveform data.

        Raises:
        - ValueError: If starttime or endtime is not an instance of obspy.UTCDateTime.
        """
        if not isinstance(starttime, UTCDateTime) or not isinstance(endtime, UTCDateTime):
            raise ValueError("starttime and endtime must be obspy.UTCDateTime objects")

        st = Stream()
        current_time = starttime
        while current_time <= endtime:
            year = current_time.year
            day_of_year = current_time.julday
            
            # Generate object key pattern
            object_key_pattern = self.waveform_dir + \
                                 self.year_day_format.format(year=year, day_of_year=day_of_year, network=network) + \
                                 self.mseed_file_format.format(network=network, station=station, location=location, 
                                                               channel=channel, year=year, day_of_year=day_of_year)
       
            # Format the prefix with the current year and day_of_year
            prefix = self.waveform_dir + self.year_day_format.format(year=year, day_of_year=day_of_year, network=network)

            # List available files matching the prefix in the bucket
            available_files = self._list_available_files(prefix)
            ## Filter based on wildcards using match_wildcard
            matching_files = [f for f in available_files if _match_wildcard(object_key_pattern, f)]
            
            if len (matching_files) <1:
                raise IndexError ("No matching waveform file(s) found in the bucket. Please check your input parameters. ")
            
            ## We use mseedlib here because ObsPy does not apply timing correctly
            ## when the sampling rate in not uniform with the dataloggers.
            def download_file(file):
                bin_obj = self._s3_waveform.Object(file).get()["Body"].read()
                mstl = MSTraceList()
                with tempfile.NamedTemporaryFile(delete=True) as tmp:
                    tmp.write(bin_obj)
                    tmp.flush()
                    mstl.read_file(tmp.name, unpack_data=True, record_list=True)
                return _fix_mseed_timing(mstl.traceids())
            
            with ThreadPoolExecutor(max_workers=max_threads) as executor:
                futures = [executor.submit(download_file, f) for f in matching_files]
                for future in tqdm(as_completed(futures), total=len(futures), desc="Downloading waveforms", position=2):
                    day_st = future.result()
                    st += day_st
                
            current_time += 86400  # Move to the next day
        
        
        st.trim(starttime, endtime)
        
        if len(st) > 0: 
            return st
        
        else:
            raise IndexError ("No waveforms found!")
                

    
    
    def get_events(self, starttime, endtime,  max_threads=1, **kwargs):
        # Fetch earthquake data
        
        event_data = _fetch_geonet_earthquake_data(starttime, endtime, **kwargs)
        
        if event_data is None:

            return Catalog()

        # Initialize catalog
        cat = Catalog()
        event_ids = event_data.tolist()  # Ensure this matches the actual data structure
        def download_event(eventid, max_retries=2):
            bin_obj = self._s3_event.Object(eventid+".xml").get()['Body'].read()
            event_obj = read_events(BytesIO(bin_obj))[0]
            return event_obj

        ev_list = []
        if max_threads > 1:
            with ThreadPoolExecutor(max_workers=max_threads) as executor:
                results = {executor.submit(download_event, event_id): event_id for event_id in event_ids}
                for event in tqdm(as_completed(results), total=len(results), desc="Downloading events", position=1):
                    event_id = results[event]
                    data = event.result()
                    ev_list.append(data)

        else:
            ev_list = [download_event(event_id) for event_id in event_ids]
        
        cat = Catalog(ev_list)
        
        return cat


def _fetch_geonet_earthquake_data(starttime, endtime, **kwargs):


    if starttime is None or endtime is None:
        raise ValueError("Both 'starttime' and 'endtime' must be provided.")

    # Convert UTCDateTime to pandas Timestamp
    t1 = pd.Timestamp(str(starttime))
    t2 = pd.Timestamp(str(endtime))

    # Determine if the duration is greater than 6 months
    if (endtime - starttime) / (86400 * 30) > 6:
        tot_months = (endtime - starttime) / (86400 * 30)
        num_periods = int(tot_months / 6) + 1
        time_periods = pd.date_range(t1, t2, periods=num_periods, inclusive="both")
    else:
        time_periods = pd.date_range(t1, t2, periods=2, inclusive="both")

    all_data = []

    for p, time_period in enumerate(time_periods[:-1]):
        start = UTCDateTime(time_periods[p])
        end = UTCDateTime(time_periods[p + 1])

        # Start constructing the URL with the mandatory parameters
        url = f"https://quakesearch.geonet.org.nz/csv?startdate={start}&enddate={end}"

        # Add optional parameters if provided
        if "minmag" in kwargs:
            url += f"&minmag={kwargs['minmag']}"
        if "maxmag" in kwargs:
            url += f"&maxmag={kwargs['maxmag']}"
        if "maxdepth" in kwargs:
            url += f"&maxdepth={kwargs['maxdepth']}"
        if "mindepth" in kwargs:
            url += f"&mindepth={kwargs['mindepth']}"
        if "bbox" in kwargs and isinstance(kwargs["bbox"], list) and len(kwargs["bbox"]) == 4:
            bbox_str = ",".join(map(str, kwargs["bbox"]))
            url += f"&bbox={bbox_str}"

        # print(f"Fetching data from {start} to {end} with URL: {url}")

        response = requests.get(url)
        if response.status_code == 200:
            df = pd.read_csv(StringIO(response.text))
            all_data.append(df)
        else:
            print(f"Failed to fetch data for the period {start} to {end}")

    # Combine all the DataFrames
    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
        return combined_data.publicid
    else:
        print("No data retrieved")
        return None
    
    

def _fix_mseed_timing(mstl_traceids):
    
    stream = Stream()
    for traceid in mstl_traceids:
        for segment in traceid.segments():
            
            data = np.ctypeslib.as_array(segment.datasamples)
            # print (UTCDateTime(segment.starttime_str()), UTCDateTime(segment.endtime_str()))
            # compute actual average sampling interval
            dt = (UTCDateTime(segment.endtime_str()) - UTCDateTime(segment.starttime_str()))/(len(data)-1)
            trace = Trace()
            n, s, l, c = sourceid2nslc(traceid.sourceid)
            trace.data = data
            trace.stats.network = n
            trace.stats.station = s
            trace.stats.location = l
            trace.stats.channel = c
            trace.stats.starttime = UTCDateTime(segment.starttime_str())
            trace.stats.delta = dt 
            
            # print (trace)
            trace.resample(int(segment.samprate))
            trace.stats.sampling_rate = int(segment.samprate)
            # print (trace)
            stream += trace
            
    return stream


def _match_wildcard(pattern, filename):
    # Replace multiple underscores with a single '*'
    pattern = pattern.replace("_", "*")  # '?' matches a single character in fnmatch
    pattern = pattern.replace("?", "*")  # '?' matches a single character in fnmatch
    
    # print(f"Pattern: {pattern}, Filename: {filename}")  # Debugging
    return fnmatch.fnmatch(filename, pattern)