#!/usr/bin/env python3
"""
Created on: 17/05/2023 

Author: Vitaliy Popov

Description: Script checks CRT data and prints some of the ADC stats.

"""


from hdf5libs import HDF5RawDataFile

import daqdataformats
import detdataformats
from daqdataformats import FragmentType
from rawdatautils.unpack.crt import *
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


@click.command()
@click.argument('filename', type=click.Path(exists=True))
@click.option('--det', default='HD_CRT', help='Subdetector string (default: HD_CRT)')
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

        crt_geo_ids    = list(h5_file.get_geo_ids_for_subdetector(r,detdataformats.DetID.string_to_subdetector(det)))
        
        if len(crt_geo_ids) == 0:
            print(f"Record {r} has no data for {det}. Exiting..")
            return
        
        trigger_stamps = []
        stamp_begin    = []
        timelines      = []

        active_channels = 0
        n_channels      = 0


        headline = f"{'CRATE':^10} {'SLOT':^10} {'LINK':^10} {'MODULE':^10} "
        
        if adc_stats:
            headline += f" {'MEAN':^10} {'Std.dev.':^10}"
        
        if check_ts:
            headline += f" {'TS stats':^17} {'TS Check':^18}"

        print("-"*114)
        print(f"{'RECORD':>50}: {r[0]:<15} {time.ctime(h5_file.get_frag(r,crt_geo_ids[0]).get_trigger_timestamp()*16 /1e9):^20}")
        print("-"*114)
        print(headline)
        print("-"*114)

        scanned_channels = 0
        tslot = -1

        for gid in crt_geo_ids:
            
            frag     = h5_file.get_frag(r,gid)
            det_link = 0xffff & (gid >> 48)
            det_slot = 0xffff & (gid >> 32)
            det_crate = 0xffff & (gid >> 16)
            det_id = 0xffff & gid
            subdet = detdataformats.DetID.Subdetector(det_id)
            det_name = detdataformats.DetID.subdetector_to_string(subdet)

            fragType = frag.get_header().fragment_type
            #print(fragType)
           
            if fragType == FragmentType.kCRT.value:
            
                timestamps = np_array_timestamp(frag)
                modules    = np_array_modules(frag)
                channels   = np_array_channel(frag)
                adcs       = np_array_adc(frag)
                n_frames = len(modules)
                crt_hits = []
                print(n_frames)
                for frame in range(n_frames):
                    hit = {"timestamp":timestamps[frame],"module":modules[frame]}


            trigger_stamps.append(frag.get_trigger_timestamp())     

            ts_status = f"{bcolors.FAIL}{'Problems':^20}{bcolors.ENDC}"

            if n_frames == 0:
                main_line = f"{det_crate:^10} {det_slot:^10} {det_link:^10} {'Empty fragment':^40} "
                print(main_line)

            for fr_num in range(n_frames):
                scanned_channels += 1
                main_line = f"{det_crate:^10} {det_slot:^10} {det_link:^10} {modules[fr_num]:^10} "
                ch_lines = []
                adc_lines = []
                if adc_stats:
                    for ch in channels[fr_num]:
                        if(adcs[fr_num][channels[fr_num].tolist().index(ch)] != 0):
                            ch_lines.append(f"{ch:^5} ")
                        if(ch>63):
                            print("Error, channel out of expected range!")
                    for adc_val in adcs[fr_num]:
                        if(adc_val != 0):
                            adc_lines.append(f"{adc_val:^5} ")
                    if fragType == FragmentType.kCRT.value:
                        if np.std(adcs[:]) > 10:
                            active_channels += 1
                        main_line += f"{np.mean(adcs[fr_num][:]):^10.2f}  {np.std(adcs[fr_num][:]):^10.2f} "
                    else:
                        if np.std(adcs[:, fr_num]) > 10:
                            active_channels += 1
                        main_line += f"{np.mean(adcs[:, fr_num]):^10.2f}  {np.std(adcs[:, fr_num]):^10.2f} "

                if check_ts:
                    delta = np.diff(timestamps)
                    main_line += f"{np.mean(delta):>8.1f}/{np.std(delta):<8.1f}"

                    if np.std(delta) < 2:
                        ts_status = f"{bcolors.OKGREEN}{'OK':^18}{bcolors.ENDC}"

                    main_line += ts_status

                print(main_line)
                print(ch_lines)
                print(adc_lines)

            #if tslot == geo_info.det_slot:
            #    continue
            #else:
            #    tslot = geo_info.det_slot
            #    print("")


        print(f"Number of active/total channels \t-- {active_channels:>20}/{scanned_channels}\n")

        

    if summary:

        print("-"*80)
        print(f"{'SUMMARY':^80}")
        print("-"*80)
        print(f"Processed records \t - \t {len(records_to_process)} \n")
        print("-"*60)
        print(f"\t crates \t slots \t\t links")
        print("-"*60)
        s = " "
        if (np.all(timelines) == 1):
            print(f"Timelines \t - \t {bcolors.OKGREEN} OK {bcolors.ENDC} \n")
        else:
            print(f"Timelines \t - \t {bcolors.FAIL} PROBLEMS {bcolors.ENDC} \n")

    print(f"{'Processing finished': ^80}")

if __name__ == '__main__':
    main()





