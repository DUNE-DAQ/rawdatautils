#!/usr/bin/env python3
"""
Created on: 17/05/2023 

Author: Vitaliy Popov

Description: Script checks PDS data and prints some of the ADC stats.

"""


from hdf5libs import HDF5RawDataFile

import daqdataformats
import detdataformats
import fddetdataformats
from daqdataformats import FragmentType
from rawdatautils.unpack.daphne import *
import detchannelmaps

import click
import datetime
import time
import numpy as np
import time
#import matplotlib.pyplot as plt

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

dmodes = {3 : "Self-triggered", 13 : "Streaming"}

def print_links(pds_geo_ids):

    print("-"*60)
    split = " "*6 + "|" + " "*6
    geo_data = [[] for i in range(4)]
    for gid in pds_geo_ids:
        #geo_info = detchannelmaps.HardwareMapService.parse_geo_id(gid)
        det_link = 0xffff & (gid >> 48);
        det_slot = 0xffff & (gid >> 32);
        det_crate = 0xffff & (gid >> 16);
        det_id = 0xffff & gid;
        subdet = detdataformats.DetID.Subdetector(det_id)
        det_name = detdataformats.DetID.subdetector_to_string(subdet)
        geo_data[det_slot].append(det_link)

    
    for i in range(len(geo_data)):
        if len(geo_data[i])>0:
            print(f"\t{geo_info.det_crate:3}{split}{i:3}{split}{geo_data[i]}")
    return


@click.command()
@click.argument('filename', type=click.Path(exists=True))
@click.option('--det', default='HD_PDS', help='Subdetector string (default: HD_PDS)')
@click.option('--nrecords', '-n', default=-1, help='How many Trigger Records to process (default: all)')
@click.option('--nskip', default=0, help='How many Trigger Records to skip (default: 0)')
@click.option('--channel-map', default=None, help="Channel map to load (default: None)")
@click.option('--summary', is_flag=True, help="Print checks summary (currently broken?)")
@click.option('--check_ts', is_flag=True, help="Print timestamps check (works for streaming data)")
@click.option('--adc_stats', is_flag=True, help="Print adc stats (works for streaming data)")
@click.option('--print_frame_timestamps', is_flag=True, help="Print individual frame timestamps (can be very verbose)")

def main(filename, det, nrecords, nskip, channel_map, adc_stats, check_ts, summary, print_frame_timestamps):

    h5_file   = HDF5RawDataFile(filename)
    records   = h5_file.get_all_record_ids()

    if nskip > len(records):
        print(f'Requested records to skip {nskip} is greater than number of records {len(records)}. Exiting...')
        return
    
    if nrecords > 0:
        if (nskip+nrecords) > len(records):
            nrecords = -1
        else:
            nrecords=nskip+nrecords
            
    records_to_process = []

    if nrecords==-1:
        records_to_process = records[nskip:]
    else:
        records_to_process = records[nskip:nrecords]

    print(f'Will process {len(records_to_process)} of {len(records)} records.')
    
    #have channel numbers per geoid in here
    ch_map = None
    if channel_map is not None:
        ch_map = detchannelmaps.make_pds_map(channel_map)
    offline_ch_num_dict = {}

    for r in records_to_process:

        pds_geo_ids    = list(h5_file.get_geo_ids_for_subdetector(r,detdataformats.DetID.string_to_subdetector(det)))
        
        if len(pds_geo_ids) == 0:
            print(f"Record {r} has no data for {det}. Exiting..")
            return
        
        trigger_stamps = []
        stamp_begin    = []
        timelines      = []

        active_channels = 0
        n_channels      = 0


        headline = f"{'CRATE':^10} {'SLOT':^10} {'LINK':^10} {'Fragment Type':^15} {'CHANNEL':^10} {'OFF CHANNEL':^15}"
        
        if adc_stats:
            headline += f" {'MEAN':^10} {'Std.dev.':^10}"
        
        if check_ts:
            headline += f" {'TS stats':^17} {'TS Check':^18}"

        #trg_ts_nsec = float(h5_file.get_frag(r,pds_geo_ids[0]).get_trigger_timestamp())/62500000.0
        #trg_time_string = datetime.datetime.fromtimestamp(trg_ts_nsec)

        #print("-"*114)
        #print(f"{'RECORD':>50}: {r[0]:<15} {str(trg_time_string):^26}")
        #print("-"*114)
        #print(headline)
        #print("-"*114)

        scanned_channels = 0
        tslot = -1

        for gid in pds_geo_ids:

            print(gid)
            
            det_link = 0xffff & (gid >> 48);
            det_slot = 0xffff & (gid >> 32);
            det_crate = 0xffff & (gid >> 16);
            det_id = 0xffff & gid;
            subdet = detdataformats.DetID.Subdetector(det_id)
            det_name = detdataformats.DetID.subdetector_to_string(subdet)

            print(det_id,det_crate,det_slot,det_link)
            print(r)
            
            frag     = h5_file.get_frag(r,gid)
            fragType = frag.get_header().fragment_type

            if fragType == FragmentType.kDAPHNE.value:
            
                first_frame = fddetdataformats.DAPHNEFrame(frag.get_data())
                n_frames   = get_n_frames(frag)
                timestamps = np_array_timestamp(frag)
                adcs       = np_array_adc(frag)
                channels   = np_array_channels(frag)
                n_channels = len(np.unique(channels))

            elif fragType == FragmentType.kDAPHNEStream.value:

                first_frame = fddetdataformats.DAPHNEStreamFrame(frag.get_data())
                n_frames   = get_n_frames_stream(frag)
                timestamps = np_array_timestamp_stream(frag)
                adcs       = np_array_adc_stream(frag)
                channels   = np_array_channels_stream(frag)[0]
                n_channels = len(np.unique(channels))
                #print(f'Frame size = {first_frame.sizeof()}, number of timestamps, adcs, channels, channels_in_list = {len(timestamps)}, {len(adcs)}, {len(channels)}, {len(np_array_channels_stream(frag))}')

            #fill channel map info if needed
            if(offline_ch_num_dict.get(gid) is None):
                if channel_map is None:
                    offline_ch_num_dict[gid] = np.arange(48)
                else:
                    dh = first_frame.get_daqheader()
                    offline_ch_num_dict[gid] = np.array([ch_map.get_offline_channel_from_det_crate_slot_stream_chan(dh.det_id, dh.crate_id, dh.slot_id, dh.link_id, c) for c in range(48)])


            trigger_stamps.append(frag.get_trigger_timestamp())

            daq_header = first_frame.get_daqheader()
            #print(daq_header,daq_header.version)

            ts_status = f"{bcolors.FAIL}{'Problems':^20}{bcolors.ENDC}"

            for ch_num in range(n_channels):
                scanned_channels += 1
                line = f"{det_crate:^10} {det_slot:^10} {det_link:^10} {dmodes[fragType] :^15} {channels[ch_num]:^10} {offline_ch_num_dict[gid][channels[ch_num]]:^15}"

                if np.mean(adcs[:]) > 10:
                    active_channels += 1
                
                if adc_stats:
                    if fragType == FragmentType.kDAPHNE.value:
                        line += f"{np.mean(adcs[:]):^10.2f}  {np.std(adcs[:]):^10.2f} "
                    else:
                        line += f"{np.mean(adcs[:, ch_num]):^10.2f}  {np.std(adcs[:, ch_num]):^10.2f} "

                if check_ts:
                    delta = np.diff(timestamps)
                    line += f"{np.mean(delta):>8.1f}/{np.std(delta):<8.1f}"

                    if np.std(delta) < 2:
                        ts_status = f"{bcolors.OKGREEN}{'OK':^18}{bcolors.ENDC}"

                    line += ts_status

                print(line)

            if (print_frame_timestamps):
                temp_channels = np_array_channels_stream(frag)
                temp_dashes_string = "-"*110
                print(f"    {temp_dashes_string}")
                print("      --> Frame timestamp details <--")
                print("      Index  PDS Ch  Off Ch  DTS Timestamp (ticks)  DTS Timestamp (time string)")
                print(f"    {temp_dashes_string}")
                loop_counter = 0;
                for idx in range(len(timestamps)):
                    if fragType == FragmentType.kDAPHNEStream.value and (idx % 64) != 0:
                        continue
                    ts_nsec = float(timestamps[idx])/62500000.0
                    time_string = datetime.datetime.fromtimestamp(ts_nsec)
                    if fragType == FragmentType.kDAPHNEStream.value:
                        print(f'     {(idx/64):>5}   {temp_channels[loop_counter]}   {offline_ch_num_dict[gid][temp_channels[loop_counter]]}    {timestamps[idx]:>20}    {str(time_string):<26}')
                    else:
                        print(f'     {idx:>5}   {channels[idx]:>5}   {offline_ch_num_dict[gid][channels[idx]]:>5}    {timestamps[idx]:>20}    {str(time_string):<26}')
                    loop_counter += 1
                print()

            if tslot == det_slot:
                continue
            else:
                tslot = det_slot
                print("")

            if True:
                dict_tp_ch_ts = {}
                for i_f in range(n_frames):
                    frame = fddetdataformats.DAPHNEFrame(frag.get_data(i_f*fddetdataformats.DAPHNEFrame.sizeof()))
                    peaks_data = frame.get_peaks_data()
                    print(f'Analyzing Frame {i_f}: TS={frame.get_timestamp()} CH={frame.get_channel()}')
                    if frame.get_channel() not in dict_tp_ch_ts.keys():
                        dict_tp_ch_ts[frame.get_channel()] = set()
                    for i_p in range(5):
                        if not peaks_data.is_found(i_p): continue
                        print(f'\tTP Peak {i_p} at ts={peaks_data.get_sample_start(i_p)}, adc_integral={peaks_data.get_adc_integral(i_p)}, adc_max={peaks_data.get_adc_max(i_p)}, t_over_baseline={peaks_data.get_samples_over_baseline(i_p)}, n_subpeaks={peaks_data.get_num_subpeaks(i_p)}')
                        if (frame.get_timestamp()+peaks_data.get_sample_start(i_p)) in dict_tp_ch_ts[frame.get_channel()]:
                            print (f"ALREADY FOUND TP! CH={frame.get_channel()}, TS={frame.get_timestamp()+peaks_data.get_sample_start(i_p)}")
                        else:
                            dict_tp_ch_ts[frame.get_channel()].add(frame.get_timestamp()+peaks_data.get_sample_start(i_p))
                        


        print(f"Number of active/total channels \t-- {active_channels:>20}/{scanned_channels}\n")

        
    if summary:

        print("-"*80)
        print(f"{'SUMMARY':^80}")
        print("-"*80)
        print(f"Processed records \t - \t {len(records_to_process)} \n")
        print("-"*60)
        print(f"\t crates \t slots \t\t links")
        print_links(pds_geo_ids)
        print("-"*60)
        s = " "
        if (np.all(timelines) == 1):
            print(f"Timelines \t - \t {bcolors.OKGREEN} OK {bcolors.ENDC} \n")
        else:
            print(f"Timelines \t - \t {bcolors.FAIL} PROBLEMS {bcolors.ENDC} \n")

    print(f"{'Processing fnished': ^80}")

if __name__ == '__main__':
    main()





