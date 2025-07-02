from obspy import UTCDateTime
from geonet_obspy_utils.clients.aws.client import Client  # Assuming this is your custom AWSClient wrapper


def test_geonet_waveform_download():
    
    client = Client("GEONET")

    starttime = UTCDateTime("2024-03-20T16:59:00")
    endtime = UTCDateTime("2024-03-20T18:05:00")

    stream = client.get_waveforms("NZ", "DCZ", "*", "HH*", 
                                  starttime, endtime, max_threads=4)
    assert len(stream) > 0, "No waveforms were returned from GeoNet AWS client."


    ################ Test waveforms and picks ################
    # event_xml_files = [
    #                '/scratch/pasanh/S3_MSEED_ISSUE/event_xmls/2024p215431.xml',
    #                ]
    
    
    # ## Apply filter 
    # stream.filter("bandpass", freqmin=2, freqmax=15, corners=4, zerophase=True)
    
    
    # ## read event file to compare pick with waveforms
    # ev_file = event_xml_files[0]
    # event = read_events(ev_file)[0]

    # station_of_interest = stream[0].stats.station
    # picks = event.picks
    # # print (picks)
    # ot = event.origins[0].time
    # station_picks = [pick for pick in picks if pick.waveform_id.station_code == station_of_interest]
    # station_pick_times = [pick.time for pick in station_picks]
    # ## trim streams
    
    # print (station_pick_times)
    # stream.trim(station_pick_times[0]-10, station_pick_times[-1]+20)
    
    # print (stream)
    # ## compute relative pick times
    # pick_times_relative_orig = [pt - stream[0].stats.starttime for pt in station_pick_times]
    


    # fig, axes = plt.subplots(nrows=2, figsize=(10, 6), sharex=True)
    
    # axes[0].plot(stream[0].times(), stream[0].data, color="black", lw=0.5)
    # # axes[2].plot(st_fixed_packets[0].times(), st_fixed_packets[0].data, color='black', lw=0.5)
    
    
    # for rpt in pick_times_relative_orig:
    #     axes[0].axvline(x=rpt, color='red', lw=0.5, ls='--', label='Picks')
  

    # axes[1].set_xlabel("Time (s)")
    # plt.savefig("{:s}_{:s}.png".format(station_of_interest, ev_file.split("/")[-1][:-4]), dpi=300, bbox_inches="tight")
    
    #####################################################
    

def test_scedc_waveform_download():
    
    client = Client("SCEDC")

    starttime = UTCDateTime("2024-03-20T16:59:00")
    endtime = UTCDateTime("2024-03-20T18:05:00")


    stream = client.get_waveforms("CI", "ABL", "*", "?Z",
                                               starttime, endtime, max_threads=4)

    assert len(stream) > 0, "No waveforms were returned from SCEDC AWS client."


def test_geoent_event_download():
    
    client = Client("GEONET")

    starttime = UTCDateTime("2024-03-20T16:59:00")
    endtime = UTCDateTime("2024-03-20T18:05:00")


    cat = client.get_events(starttime, endtime)

    assert len(cat) > 0, "No events were returned from GEONET AWS client."



# Optional: Run manually if not using pytest CLI
if __name__ == "__main__":
    test_geonet_waveform_download()
    test_scedc_waveform_download()
    test_geoent_event_download()
    # print("GeoNet waveform data downloaded successfully!")
