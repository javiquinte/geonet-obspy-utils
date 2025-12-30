#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
The geonet_obspy_utils.clients.fdsn.client test suite.
"""


from obspy import UTCDateTime
from geonet_obspy_utils.clients.aws.client import Client


def test_geonet_waveform_download():
    """
    Add docstring
    """
    client = Client("GEONET")

    # waveform query
    starttime = UTCDateTime("2025-08-20T20:59:00")
    endtime = UTCDateTime("2025-08-21T14:05:00")

    stream = client.get_waveforms("NZ", "DCZ, JCZ", "10", "HH*, EH*",
                                  starttime, endtime, max_threads=4)

    assert len(stream) > 0, "No waveforms returned by GeoNet AWS client."

    # file query
    fname = "2023.125.DCZ.10-HHZ.NZ.D"

    f = client.read(fname)
    assert len(f) > 0, "No files were returned from GeoNet AWS client."


def test_scedc_waveform_download():
    """
    Add docstring
    """
    client = Client("SCEDC")

    # waveform query
    starttime = UTCDateTime("2024-03-20T16:59:00")
    endtime = UTCDateTime("2024-03-20T18:05:00")

    stream = client.get_waveforms("CI", "ABL", "*", "?Z, ?N",
                                  starttime, endtime, max_threads=4)
    assert len(stream) > 0, "No waveforms were returned from SCEDC AWS client."


def test_geonet_event_download():
    """
    Add docstring
    """
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

    assert len(cat) > 0, "No events were returned from GEONET AWS client."


def test_geofon_waveform_ge():
    """
    Download waveforms from GE network hosted at GEOFON
    """
    client = Client("GEOFON")

    # waveform query
    starttime = UTCDateTime("2025-08-20T20:59:00")
    endtime = UTCDateTime("2025-08-21T14:05:00")

    stream = client.get_waveforms("GE", "APE", "*", "HHZ,BHZ",
                                  starttime, endtime, max_threads=2)

    assert len(stream) > 0, "No waveforms returned by GEOFON."

    # file query
    fname = "GE.APE..HHZ.D.2023.125"

    f = client.read(fname)
    assert len(f) > 0, "No files were returned from GEOFON."


def test_geofon_waveform_cx():
    """
    Download waveforms from CX network hosted at GEOFON
    """
    client = Client("GEOFON")

    # waveform query
    starttime = UTCDateTime("2025-08-20T20:59:00")
    endtime = UTCDateTime("2025-08-21T14:05:00")

    stream = client.get_waveforms("CX", "PB01", "*", "HHZ,BHZ",
                                  starttime, endtime, max_threads=2)

    assert len(stream) > 0, "No waveforms returned by GEOFON."

    # file query
    fname = "CX.PB01..HHZ.D.2023.125"

    f = client.read(fname)
    assert len(f) > 0, "No files were returned from GEOFON."


if __name__ == "__main__":
    test_geonet_waveform_download()
    # test_scedc_waveform_download()
    test_geonet_event_download()
    # print("GeoNet waveform data downloaded successfully!")
