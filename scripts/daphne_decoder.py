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
from rawdatautils.unpack.utils import *
import detchannelmaps

import click
import datetime
import time
import numpy as np
import time

from rawdatautils.unpack.dataclasses import dts_to_datetime


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
@click.option('--adc-stats', is_flag=True, help="Print adc stats (works for streaming data)")
@click.option('--print-wvfm-samples', default=0, help='How many samples in each waveform to print.')
@click.option('--print-tp-info', is_flag=True, help='Print TP info from DAPHNFrame.')

def main(filename, det, nrecords, nskip, channel_map, adc_stats, print_wvfm_samples, print_tp_info):

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
            
    records_to_process = records[nskip:] if nrecords==-1 else records[nskip:nrecords]

    print(f'Will process {len(records_to_process)} of {len(records)} records.')

    unpacker_stream = DAPHNEStreamUnpacker(channel_map=channel_map,ana_data_prescale=1,wvfm_data_prescale=1)
    unpacker_slftrg = DAPHNEUnpacker(channel_map=channel_map,ana_data_prescale=1,wvfm_data_prescale=1)


    #have channel numbers per geoid in here
    ch_map = None
    if channel_map is not None:
        ch_map = detchannelmaps.make_pds_map(channel_map)
    offline_ch_num_dict = {}

    for r in records_to_process:

        print(f'Processing (Record Number,Sequence Number)=({r[0],r[1]})')
        pds_geo_ids    = list(h5_file.get_geo_ids_for_subdetector(r,detdataformats.DetID.string_to_subdetector(det)))
        
        if len(pds_geo_ids) == 0:
            print(f"Record {r} has no data for {det}. Exiting..")
            return
        
        for gid in pds_geo_ids:

            det_stream = 0xffff & (gid >> 48);
            det_slot = 0xffff & (gid >> 32);
            det_crate = 0xffff & (gid >> 16);
            det_id = 0xffff & gid;
            subdet = detdataformats.DetID.Subdetector(det_id)
            det_name = detdataformats.DetID.subdetector_to_string(subdet)
            print(f'\tProcessing gid {gid}: ',
                  f'subdetector {det_name}, '
                  f'crate {det_crate}, '
                  f'slot {det_slot}, '
                  f'stream {det_stream}')

            frag     = h5_file.get_frag(r,gid)
            fragType = frag.get_header().fragment_type
            fragType_string = daqdataformats.fragment_type_to_string(daqdataformats.FragmentType(fragType))

            is_selftrigger = (fragType==FragmentType.kDAPHNE.value)

            unpacker = unpacker_slftrg if is_selftrigger else unpacker_stream


            #get and print fragment header
            frag_header = unpacker.get_frh_data(frag)[0]
            print('\t',frag_header)

            n_frames = unpacker.get_n_obj(frag)
            print(f'\tFound {n_frames} {fragType_string} frames in this fragment.')
            if n_frames==0:
                continue

            daq_header_data = unpacker.get_daq_header_data(frag)
            det_header_data = unpacker.get_det_header_data(frag)

            for i_daqh, daqh in enumerate(daq_header_data):
                print(f'\tDAQ header {i_daqh}:\n\t\t',daq_header_data[i_daqh])
                print(det_header_data)
                if det_header_data is None or len(det_header_data)<(i_daqh+1): continue
                print(f'\tDAPHNE header info {i_daqh}:\n\t\t',det_header_data[i_daqh])

            pds_ana_data, pds_wvfm_data = unpacker.get_det_data_all(frag)

            if adc_stats:
                for pds_ana in pds_ana_data:
                    if is_selftrigger:
                        print(f'\t\tPDS channel {pds_ana.channel}, timestamp {pds_ana.timestamp_dts} adc stats:')
                    else:
                        print(f'\t\tPDS channel {pds_ana.channel} adc stats:')
                    print('\t\t',pds_ana)

            if print_wvfm_samples:
                for pds_wvfm in pds_wvfm_data:
                    if is_selftrigger:
                        print(f'\t\tPDS channel {pds_wvfm.channel}, timestamp {pds_wvfm.timestamp_dts} ({dts_to_datetime(pds_wvfm.timestamp_dts)}), waveform ({print_wvfm_samples}/{len(pds_wvfm.timestamps)} samples):')
                    else:
                        print(f'\t\tPDS channel {pds_wvfm.channel}, timestamp {pds_wvfm.timestamps[0]} ({dts_to_datetime(pds_wvfm.timestamps[0])}), waveform ({print_wvfm_samples}/{len(pds_wvfm.timestamps)} samples):')
                    for i_sample in range(print_wvfm_samples):
                        print(f'\t\t\t {i_sample:>5}:  ts={pds_wvfm.timestamps[i_sample]:<25.0f}  val={pds_wvfm.adcs[i_sample]}')

            if print_tp_info:
                if not is_selftrigger:
                    print(f'--print-tp-info called, but fragment is not kDAPHNE. Skipping...')
                else:
                    print(f'--PRINTING TP INFO--')
                    dict_tp_ch_ts = {}
                    for i_f in range(n_frames):
                        frame = fddetdataformats.DAPHNEFrame(frag.get_data(i_f*fddetdataformats.DAPHNEFrame.sizeof()))
                        print(f'\tAnalyzing Frame {i_f}: TS={frame.get_timestamp()} DAPHNE_CH={frame.get_channel()}')
                        peaks_data = frame.get_peaks_data()

                        n_tps = 0
                        for i_p in range(5):
                            if peaks_data.is_found(i_p): n_tps+=1
                        print(f'\t\tFound {n_tps} TPs:')
                        if frame.get_channel() not in dict_tp_ch_ts.keys():
                            dict_tp_ch_ts[frame.get_channel()] = set()
                        for i_p in range(n_tps):
                            if not peaks_data.is_found(i_p): continue
                            print(f'\t\t\tTP Peak {i_p} at ts={peaks_data.get_sample_start(i_p)}, ',
                                  f'adc_integral={peaks_data.get_adc_integral(i_p)}, '
                                  f'adc_max={peaks_data.get_adc_max(i_p)}, ',
                                  f't_over_baseline={peaks_data.get_samples_over_baseline(i_p)}, ',
                                  f'n_subpeaks={peaks_data.get_num_subpeaks(i_p)}')
                            if (frame.get_timestamp()+peaks_data.get_sample_start(i_p)) in dict_tp_ch_ts[frame.get_channel()]:
                                print (f"\t\t\t\t=====PREVIOUSLY FOUND TP w/ CH={frame.get_channel()}, TS={frame.get_timestamp()+peaks_data.get_sample_start(i_p)}")
                            else:
                                dict_tp_ch_ts[frame.get_channel()].add(frame.get_timestamp()+peaks_data.get_sample_start(i_p))

    print(f"{'Processing finished': ^80}")

if __name__ == '__main__':
    main()





