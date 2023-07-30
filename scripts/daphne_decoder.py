#!/usr/bin/env python3
"""
Created on: 17/05/2023 

Author: Vitaliy Popov

Description: Script checks PDS data and prints some of the ADC stats.

"""


from hdf5libs import HDF5RawDataFile

import daqdataformats
import detdataformats
from daqdataformats import FragmentType
from rawdatautils.unpack.daphne import *
import detchannelmaps

import click
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
        geo_info = detchannelmaps.HardwareMapService.parse_geo_id(gid)
        geo_data[geo_info.det_slot].append(geo_info.det_link)

    
    for i in range(len(geo_data)):
        if len(geo_data[i])>0:
            print(f"\t{geo_info.det_crate:3}{split}{i:3}{split}{geo_data[i]}")
    return


@click.command()
@click.argument('filename', type=click.Path(exists=True))
@click.option('--det', default='HD_PDS', help='Subdetector string (default: HD_PDS)')
@click.option('--nrecords', '-n', default=-1, help='How many Trigger Records to process (default: all)')
@click.option('--nskip', default=0, help='How many Trigger Records to skip (default: 0)')
@click.option('--summary', is_flag=True, help="Print checks summary")
@click.option('--check_ts', is_flag=True, help="Print timestamps check")
@click.option('--adc_stats', is_flag=True, help="Print adc stats")

def main(filename, det, nrecords, nskip, adc_stats, check_ts, summary):

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


        headline = f"{'CRATE':^10} {'SLOT':^10} {'LINK':^10} {'Fragment Type':^15} {'CHANNEL':^10} "
        
        if adc_stats:
            headline += f" {'MEAN':^10} {'Std.dev.':^10}"
        
        if check_ts:
            headline += f" {'TS stats':^17} {'TS Check':^18}"

        print("-"*114)
        print(f"{'RECORD':>50}: {r[0]:<15} {time.ctime(h5_file.get_frag(r,pds_geo_ids[0]).get_trigger_timestamp()*16 /1e9):^20}")
        print("-"*114)
        print(headline)
        print("-"*114)

        scanned_channels = 0
        tslot = -1

        for gid in pds_geo_ids:
            
            frag     = h5_file.get_frag(r,gid)
            geo_info = detchannelmaps.HardwareMapService.parse_geo_id(gid)
            fragType = frag.get_header().fragment_type

            if fragType == FragmentType.kDAPHNE.value:
            
                timestamps = np_array_timestamp(frag)
                adcs       = np_array_adc(frag)
                channels   = np_array_channels(frag)
                n_channels = len(np.unique(channels))

            elif fragType == 13:

                timestamps = np_array_timestamp_stream(frag)
                adcs       = np_array_adc_stream(frag)
                channels   = np_array_channels_stream(frag)[0]
                n_channels = len(np.unique(channels))

            trigger_stamps.append(frag.get_trigger_timestamp())     

            ts_status = f"{bcolors.FAIL}{'Problems':^20}{bcolors.ENDC}"

            for ch_num in range(n_channels):
                scanned_channels += 1
                line = f"{geo_info.det_crate:^10} {geo_info.det_slot:^10} {geo_info.det_link:^10} {dmodes[fragType] :^15} {channels[ch_num]:^10} "

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

            if tslot == geo_info.det_slot:
                continue
            else:
                tslot = geo_info.det_slot
                print("")


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





