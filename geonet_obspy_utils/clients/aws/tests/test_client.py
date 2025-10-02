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

    stream = client.get_waveforms("NZ", "DCZ, JCZ", "10,?", "HH*, EH*",
                                  starttime, endtime, max_threads=4)

    assert len(stream) > 0, "No waveforms returned by GeoNet AWS client."

    # file query
    fname = "DCZ.NZ/2023.125.DCZ.10-HHZ.NZ.D"

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

    starttime = UTCDateTime("2024-03-20T16:59:00")
    endtime = UTCDateTime("2024-03-20T18:05:00")

    cat = client.get_events(starttime, endtime)

    assert len(cat) > 0, "No events were returned from GEONET AWS client."

    cat = client.get_events(starttime, endtime)
    assert len(cat) > 0, "No events were returned from GEONET AWS client."


if __name__ == "__main__":
    test_geonet_waveform_download()
    test_scedc_waveform_download()
    test_geonet_event_download()
    # print("GeoNet waveform data downloaded successfully!")
