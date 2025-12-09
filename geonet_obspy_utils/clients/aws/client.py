#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
GeoNet AWS  client for ObsPy.
"""
import datetime
from obspy import UTCDateTime, Stream, Trace, read_events, Catalog
import fnmatch
from typing import List
import tempfile
import yaml
import os
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError
from mseedlib import MSTraceList, sourceid2nslc
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import requests
from io import StringIO, BytesIO
from parse import parse
from itertools import product


class Client(object):
    """
    AWS Client to access waveform (all available clients) and event
    (only for GeoNet) data.
    """

    def __init__(self, client_name="GEONET"):
        """
        Initialize the client with the  configuration loaded from a YAML file.

        The YAML file (client_config.yml) contains configurations for different
        clients. New clients can be added by adding their configurations to the
        YAML file.

        >>> client = Client("GEONET")
        >>> print (client)

        :type client_name: str
        :param client_name: Name of the client to initialize. Must match a key
            in the YAML config file.

        """

        config_path = os.path.join(os.path.dirname(__file__),
                                   "client_config.yml")

        with open(config_path, 'r') as f:
            base_config = yaml.safe_load(f)

        if client_name.upper() in base_config:
            self.config = base_config[client_name.upper()]
        else:
            raise ValueError(f"Unknown base name: {client_name}")

        self.waveform_bucket_name = self.config["waveform_bucket_name"]
        self.event_bucket_name = self.config.get("event_bucket_name", None)
        self.base_url = self.config["base_url"]
        self.waveform_dir = self.config.get("waveform_dir", '')
        self.filename = [part for part in self.config["filename"].split('/') if len(part)]

    @property
    def _s3(self):
        """
        To enable instantiation for individual threads.
        """
        s3params = {'config': Config(signature_version=UNSIGNED)}
        if self.base_url is not None:
            s3params['endpoint_url'] = self.base_url
        return boto3.client("s3", **s3params)

    @property
    def _s3_waveform(self):
        """
        S3 access as property to enforce instantiation for individual
        threads.
        """
        s3params = {'config': Config(signature_version=UNSIGNED)}
        if self.base_url is not None:
            s3params['endpoint_url'] = self.base_url
        waveform_bob = boto3.client(
            's3', **s3params)

        return waveform_bob.Bucket(self.waveform_bucket_name)

    @property
    def _s3_event(self):
        """
        S3 access as property to enforce instantiation for individual threads.
        Only applies to GeoNet for now.
        """
        if self.event_bucket_name is None:
            raise Exception('No bucket for event data')

        s3params = {'config': Config(signature_version=UNSIGNED)}
        if self.base_url is not None:
            s3params['endpoint_url'] = self.base_url
        event_bob = boto3.resource(
            's3', **s3params)

        return event_bob.Bucket(self.event_bucket_name)

    def _check_s3_match(self, bucket, root, prefix, params, level=0) -> List:
        """
        Check if the directories in this level match the input provided
        """
        # print('Entering:', prefix)
        result = list()
        entries = self._s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')

        # If this is the last entry that means that we only need to look at the files
        if level == len(self.filename)-1:
            for file in entries['Contents']:
                f = file['Key']
                req = root + '/'.join([self.filename[l].format(**params) for l in range(level+1)])
                print(req, f)
                if _match_wildcard(req, f):
                    result.append(f)
            return result

        # If this is not the last entry we still need to go deep down through the tree structure,
        # so look only the directories
        for folder in entries['CommonPrefixes']:
            f = folder['Prefix'].rstrip('/')
            req = root + '/'.join([self.filename[l].format(**params) for l in range(level+1)])
            # print(req, f)
            if _match_wildcard(req, f):
                result.extend(self._check_s3_match(bucket, root, folder['Prefix'], params, level+1))
        return result


    def get_waveforms(self, network, station, location, channel, starttime,
                      endtime, filename=None, max_threads=1):
        """
        Fetch MiniSEED waveform data from AWS S3 bucket.

        >>> client = Client("GEONET")
        >>> t1 = UTCDateTime("2024-03-20T16:59:00")
        >>> t2 = UTCDateTime("2024-03-20T18:05:00")
        >>> st = client.get_waveforms("NZ", "DCZ", "10", "LHZ", t1, t2)
        >>> print(st)
        1 Trace(s) in Stream:
        NZ.DCZ.10.LHZ | 2025-09-20T16:59:00.000000Z - .. | 1.0 Hz, 3961 samples

        The service can deal with UNIX style wildcards.

        >>> st = client.get_waveforms("NZ,IU", "DCZ, ?", "*", "HH*", t1, t2)
        >>> print(st)
        6 Trace(s) in Stream:
        NZ.DCZ.10.HHN  | 2025-09-20T16:59:00.0Z - .. | 100.0 Hz, 396001 samples
        NZ.DCZ.10.HHE  | 2025-09-20T16:59:00.0Z - .. | 100.0 Hz, 396001 samples
        IU.SNZO.00.HH1 | 2025-09-20T16:58:59.0Z - .. | 100.0 Hz, 396001 samples
        IU.SNZO.00.HHZ | 2025-09-20T16:58:59.0Z - .. | 100.0 Hz, 396001 samples
        IU.SNZO.00.HH2 | 2025-09-20T16:58:59.0Z - .. | 100.0 Hz, 396001 samples
        NZ.DCZ.10.HHZ  | 2025-09-20T16:59:00.0Z - .. | 100.0 Hz, 396001 samples

        :type network: str
        :param network: Select one or more network codes. Can be SEED network
            codes or data center defined codes. Multiple codes are
            comma-separated (e.g. ``"NZ,IU"``). Wildcards are allowed.
        :type station: str
        :param station: Select one or more SEED station codes. Multiple codes
            are comma-separated (e.g. ``"LBZ,SNZO"``). Wildcards are allowed.
        :type location: str
        :param location: Select one or more SEED location identifiers. Multiple
            identifiers are comma-separated (e.g. ``"00,01"``). Wildcards are
            allowed.
        :type channel: str
        :param channel: Select one or more SEED channel codes. Multiple codes
            are comma-separated (e.g. ``"BHZ,HHZ"``).
        :type starttime: :class:`~obspy.core.utcdatetime.UTCDateTime`
        :param starttime: Limit results to time series samples on or after the
            specified start time
        :type endtime: :class:`~obspy.core.utcdatetime.UTCDateTime`
        :param endtime: Limit results to time series samples on or before the
            specified end time
        :type filename: str or file
        :param filename: If given, the downloaded data will be saved there
            instead of being parsed to an ObsPy object. Thus it will contain
            the raw data from the webservices.
        """

        if (not isinstance(starttime, UTCDateTime) or
                not isinstance(endtime, UTCDateTime)):
            raise ValueError("starttime and endtime must be" +
                             " obspy.UTCDateTime objects")

        st = Stream()
        current_time = starttime

        # Create all combinations of network, station, location, and channel
        nets = [net.strip() for net in network.split(",")]
        stas = [st.strip() for st in station.split(",")]
        locs = [loc.strip() for loc in location.split(",")]
        chans = [ch.strip() for ch in channel.split(",")]

        # Cartesian product
        groups = [{"network": n, "station": s, "location": l, "channel": c}
                  for n, s, l, c in product(nets, stas, locs, chans)]

        matching_files = []

        t0 = datetime.datetime.now()
        while current_time <= endtime:
            year = current_time.year
            day_of_year = current_time.julday

            for group in groups:
                # Format the prefix with the current year and day_of_year
                prefix = self.waveform_dir.lstrip('/')
                params = group.copy()
                params['year'] = year
                params['day_of_year'] = day_of_year
                matching_files += self._check_s3_match(self.waveform_bucket_name, self.waveform_dir,
                                                       prefix, params)
                
            # Move to the next day
            secs_to_next_day = 86400 - (current_time.ns -
                                        UTCDateTime(current_time.year,
                                                    current_time.month,
                                                    current_time.day).ns)/1e9

            current_time += secs_to_next_day  # Move to the next day

        if len(matching_files) < 1:
            raise IndexError("No matching waveform file(s) found in the" +
                             "bucket. Please check your input parameters. ")

        # We use mseedlib here because ObsPy does not apply timing correctly
        # when the sampling rate is not uniform with the dataloggers.
        def download_file(file):
            mstl = MSTraceList()
            with tempfile.NamedTemporaryFile(delete=True) as tmp:
                self._s3.download_file(self.waveform_bucket_name, file, tmp.name)
                mstl.read_file(tmp.name, unpack_data=True, record_list=True)
            return _fix_mseed_timing(mstl.traceids())

        print('List files: %.2f seconds', (datetime.datetime.now() - t0).seconds)
        t0 = datetime.datetime.now()
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = [executor.submit(download_file, f)
                       for f in matching_files]
            for future in as_completed(futures):
                day_st = future.result()
                st += day_st

        print('Download: %.2f seconds', (datetime.datetime.now() - t0).seconds)
        st.trim(starttime, endtime)

        if len(st) > 0:
            if filename:
                st.write(filename)
                return None
            else:
                return st

        else:
            raise IndexError("No waveforms found!")

    def get_events(self, starttime=None, endtime=None, minlatitude=None,
                   maxlatitude=None, minlongitude=None, maxlongitude=None,
                   mindepth=None, maxdepth=None, minmagnitude=None,
                   maxmagnitude=None, eventid=None, filename=None,
                   max_threads=1, **kwargs):
        """
        Get event information from GeoNet AWS S3 bucket.

        >>> client = Client("GEONET")
        >>> cat = client.get_events(eventid=609301)
        >>> print(cat)
        1 Event(s) in Catalog:
        1997-10-14T09:53:11.070000Z | -22.145, -176.720 | 7.8 ...

        The return value is a :class:`~obspy.core.event.Catalog` object
        which can contain any number of events.

        >>> t1 = UTCDateTime("2001-01-07T00:00:00")
        >>> t2 = UTCDateTime("2001-01-07T03:00:00")
        >>> cat = client.get_events(starttime=t1, endtime=t2, minmagnitude=4,
        ...                         catalog="ISC")
        >>> print(cat)
        3 Event(s) in Catalog:
        2001-01-07T02:55:59.290000Z |  +9.801,  +76.548 | 4.9 ...
        2001-01-07T02:35:35.170000Z | -21.291,  -68.308 | 4.4 ...
        2001-01-07T00:09:25.630000Z | +22.946, -107.011 | 4.0 ...

        :type starttime: :class:`~obspy.core.utcdatetime.UTCDateTime`, optional
        :param starttime: Limit to events on or after the specified start time.
        :type endtime: :class:`~obspy.core.utcdatetime.UTCDateTime`, optional
        :param endtime: Limit to events on or before the specified end time.
        :type minlatitude: float, optional
        :param minlatitude: Limit to events with a latitude larger than the
            specified minimum.
        :type maxlatitude: float, optional
        :param maxlatitude: Limit to events with a latitude smaller than the
            specified maximum.
        :type minlongitude: float, optional
        :param minlongitude: Limit to events with a longitude larger than the
            specified minimum.
        :type maxlongitude: float, optional
        :param maxlongitude: Limit to events with a longitude smaller than the
            specified maximum.
        :type mindepth: float, optional
        :param mindepth: Limit to events with depth, in kilometers, larger than
            the specified minimum.
        :type maxdepth: float, optional
        :param maxdepth: Limit to events with depth, in kilometers, smaller
            than the specified maximum.
        :type minmagnitude: float, optional
        :param minmagnitude: Limit to events with a magnitude larger than the
            specified minimum.
        :type maxmagnitude: float, optional
        :param maxmagnitude: Limit to events with a magnitude smaller than the
            specified maximum.
        :type filename: str or file
        :param filename: If given, the downloaded data will be saved there
            instead of being parsed to an ObsPy object. Thus it will contain
            the raw data from the webservices.
        """
        if eventid:
            try:
                bin_obj = self._s3_event.Object(eventid+".xml") \
                                                .get()['Body'].read()
                event_obj = read_events(BytesIO(bin_obj))[0]
                return event_obj
            except ClientError as e:
                error_code = e.response["Error"]["Code"]
                if error_code == "NoSuchKey":
                    raise FileNotFoundError(f"File '{eventid}' not found in " +
                                            "S3 bucket.") from e

                else:
                    raise RuntimeError(f"Error fetching file '{eventid}' " +
                                       "from S3: {e}") from e
        else:
            event_data = _fetch_geonet_earthquake_data(
                starttime=starttime, endtime=endtime, minlatitude=minlatitude,
                maxlatitude=maxlatitude, minlongitude=minlongitude,
                maxlongitude=maxlongitude, mindepth=mindepth,
                maxdepth=maxdepth, minmagnitude=minmagnitude,
                maxmagnitude=maxmagnitude, **kwargs)
            if event_data is None:
                raise ValueError("No events found for the given parameters.")

            # Initialize catalog
            cat = Catalog()

            event_ids = event_data.tolist()

            def download_event(eventid, max_retries=2):
                obj = self._s3_event.Object(eventid+".xml").get()
                bin_obj = obj["Body"].read()
                event_obj = read_events(BytesIO(bin_obj))[0]
                return event_obj

            ev_list = []
            if max_threads > 1:
                with ThreadPoolExecutor(max_workers=max_threads) as executor:
                    results = {
                        executor.submit(download_event, event_id): event_id for
                        event_id in event_ids}

                    for event in as_completed(results):
                        # event_id = results[event]
                        data = event.result()
                        ev_list.append(data)

            else:
                ev_list = [download_event(event_id) for event_id in event_ids]

            cat = Catalog(ev_list)

            if filename:
                cat.write(filename)
                return None
            else:
                return cat

    def read(self, fname):
        """
        Function to read mseed file by giving filename
        No wildcards are supported here.
        """
        mseed_stats = _parse_mseed_filename(fname, self.filename[-1])
        # # Generate object key pattern
        filename = self.waveform_dir + '/'.join(self.filename).format(**mseed_stats)
        try:
            mstl = MSTraceList()
            with tempfile.NamedTemporaryFile(delete=True) as tmp:
                self._s3.download_file(self.waveform_bucket_name, filename, tmp.name)
                mstl.read_file(tmp.name, unpack_data=True, record_list=True)

            return _fix_mseed_timing(mstl.traceids())

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "NoSuchKey":
                raise FileNotFoundError(f"File '{filename}' not found in S3" +
                                        "bucket.") from e
            else:
                raise RuntimeError(f"Error fetching file '{filename}' from" +
                                   "S3: {e}") from e


def _fetch_geonet_earthquake_data(starttime, endtime, minlatitude=-90,
                                  maxlatitude=90, minlongitude=-180,
                                  maxlongitude=180, mindepth=0, maxdepth=1000,
                                  minmagnitude=-1, maxmagnitude=10,
                                  ):
    """
    Function to fetch earthquake data from GeoNet QuakeSearch service.
    :type starttime: :class:`~obspy.core.utcdatetime.UTCDateTime`, optional
    :param starttime: Limit to events on or after the specified start time.
    :type endtime: :class:`~obspy.core.utcdatetime.UTCDateTime`, optional
    :param endtime: Limit to events on or before the specified end time.
    :type minlatitude: float, optional
    :param minlatitude: Limit to events with a latitude larger than the
        specified minimum.
    :type maxlatitude: float, optional
    :param maxlatitude: Limit to events with a latitude smaller than the
        specified maximum.
    :type minlongitude: float, optional
    :param minlongitude: Limit to events with a longitude larger than the
        specified minimum.
    :type maxlongitude: float, optional
    :param maxlongitude: Limit to events with a longitude smaller than the
        specified maximum.
    :type mindepth: float, optional
    :param mindepth: Limit to events with depth, in kilometers, larger than
        the specified minimum.
    :type maxdepth: float, optional
    :param maxdepth: Limit to events with depth, in kilometers, smaller than
        the specified maximum.
    :type minmagnitude: float, optional
    :param minmagnitude: Limit to events with a magnitude larger than the
        specified minimum.
    :type maxmagnitude: float, optional
    :param maxmagnitude: Limit to events with a magnitude smaller than the
        specified maximum.
    :return: Pandas Series of public IDs of the events matching the query.
    :rtype: :class:`pandas.Series`
    """
    if starttime is None or endtime is None:
        # Default to a wide time range if not provided
        starttime = UTCDateTime("1970-01-01")
        endtime = UTCDateTime.now()

    t1 = pd.Timestamp(str(starttime))
    t2 = pd.Timestamp(str(endtime))

    # Determine if the duration is greater than 6 months
    if (endtime - starttime) / (86400 * 30) > 6:
        tot_months = (endtime - starttime) / (86400 * 30)
        num_periods = int(tot_months / 6) + 1
        time_periods = pd.date_range(t1, t2, periods=num_periods,
                                     inclusive="both")
    else:
        time_periods = pd.date_range(t1, t2, periods=2, inclusive="both")

    all_data = []

    for p, time_period in enumerate(time_periods[:-1]):
        start = UTCDateTime(time_periods[p])
        end = UTCDateTime(time_periods[p + 1])

        # Start constructing the URL with the mandatory parameters
        url = (
            f"https://quakesearch.geonet.org.nz/csv?"
            f"startdate={start}&enddate={end}"
        )

        if minmagnitude:
            url += "&minmag={:f}".format(minmagnitude)
        if maxmagnitude:
            url += "&maxmag={:f}".format(maxmagnitude)
        if maxdepth:
            url += "&maxdepth={:f}".format(maxdepth)
        if mindepth:
            url += "&mindepth={:f}".format(mindepth)

        bbox = [minlongitude, minlatitude, maxlongitude, maxlatitude]
        if (isinstance(bbox, list) and len(bbox) == 4):
            bbox_str = ",".join(map(str, bbox))
            url += "&bbox={:s}".format(bbox_str)

        response = requests.get(url)
        if response.status_code == 200:
            df = pd.read_csv(StringIO(response.text))
            all_data.append(df)
        else:
            print(f"Failed to fetch data for the period {start} \
                             to {end}")

    # Combine all the DataFrames
    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
        return combined_data.publicid
    else:
        print("No data retrieved")
        return None


def _fix_mseed_timing(mstl_traceids):
    """
    Fix timing issues in MiniSEED data using mseedlib and convert to
    ObsPy Stream.
    :type mstl_traceids: list of :class:`mseedlib.MSTraceID`
    :param mstl_traceids: List of MSTraceID objects from mseedlib.
    :return: ObsPy Stream with corrected timing.
    :rtype: :class:`obspy.core.stream.Stream`
    """
    stream = Stream()
    for traceid in mstl_traceids:
        for segment in traceid.segments():
            data = np.ctypeslib.as_array(segment.datasamples)
            # compute actual average sampling interval
            dt = (UTCDateTime(segment.endtime_str()) -
                  UTCDateTime(segment.starttime_str()))/(len(data)-1)
            trace = Trace()
            n, s, l, c = sourceid2nslc(traceid.sourceid)
            trace.data = data
            trace.stats.network = n
            trace.stats.station = s
            trace.stats.location = l
            trace.stats.channel = c
            trace.stats.starttime = UTCDateTime(segment.starttime_str())
            trace.stats.delta = dt

            trace.resample(int(segment.samprate))
            trace.stats.sampling_rate = int(segment.samprate)

            stream += trace

    return stream


def _match_wildcard(pattern, filename):
    """
    Match a filename against a pattern with wildcards.
    :type pattern: str
    :param pattern: Pattern with wildcards ('*' and '?').
    :type filename: str
    :param filename: Filename to match against the pattern.
    :return: True if the filename matches the pattern, False otherwise.
    :rtype: bool
    """
    # Replace multiple underscores with a single '*'
    pattern = pattern.replace("_", "*")
    pattern = pattern.replace("?", "*")

    return fnmatch.fnmatch(filename, pattern)


def _parse_mseed_filename(filename, mseed_format):
    """
    Parse MiniSEED filename to extract metadata using a given format.
    :type filename: str
    :param filename: MiniSEED filename to parse.
    :type mseed_format: str
    :param mseed_format: Format string defining the filename structure.
    :return: Dictionary with extracted metadata.
    :rtype: dict
    """
    result = parse(mseed_format, filename)
    if not result:
        raise ValueError(f"Filename does not match format: {filename}")
    return result.named
