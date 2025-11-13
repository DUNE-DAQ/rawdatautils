#!/usr/bin/env python3

from hdf5libs import HDF5RawDataFile
import h5py

import daqdataformats
import detdataformats
import detchannelmaps

from rawdatautils.unpack.dataclasses import *
import rawdatautils.unpack.utils

import click
import time
import numpy as np


@click.command()
@click.argument('filename', type=click.Path(exists=True))
@click.option('--nrecords', '-n', default=-1, help='How many Trigger Records to process (default: all)')
@click.option('--nskip', default=0, help='How many Trigger Records to skip (default: 0)')
@click.option('--print-adc-stats', is_flag=True, help="Print ADC Pedestals/RMS")
@click.option('--print-wvfm-samples', default=0, help='How many samples in each waveform to print.')
@click.option('--det', multiple=True, default=['VD_TopTPC','VD_BottomTPC'], help='Subdetector string (default: VD_TopTPC,VD_BottomTPC)')
@click.option('--channel-map', default=None, help="Channel map to load (default: None)")

def main(filename, nrecords, nskip, print_adc_stats, print_wvfm_samples, det, channel_map):

    #get the file
    h5_file = HDF5RawDataFile(filename)

    #get the run number and operational environment out of the file attributes
    with h5py.File(h5_file.get_file_name(), 'r') as f:
        run_number = f.attrs["run_number"]
        op_env = f.attrs["operational_environment"]
        print(f'Processing file {h5_file.get_file_name()}. run_number={run_number}, operational_environemnt={op_env}')

    #fill in appropriate channel name if we can tell from operational_environment
    openv_channel_map=None
    if op_env=="np04hd":
        openv_channel_map="PD2HDTPCChannelMap"
    elif op_env=="np04hdcoldbox":
        openv_channel_map="HDColdboxTPCChannelMap"
    elif op_env=="iceberghd" or op_env=="iceberg" or op_env=="icebergvd":
        openv_channel_map="ICEBERGChannelMap"
    elif op_env=="np02vd":
        openv_channel_map="PD2VDTPCChannelMap"
    else:
        print(f'Unknown operational_environment ({op_env}). Will use specified channel-map {channel_map}')
 
    if openv_channel_map!=channel_map and channel_map is not None:
        print(f'Operational environment {op_env} suggests channel map {openv_channel_map}, not {channel_map}.')
        print(f'Please correct (or do not specify channel map and use operational environment to select). Exiting...')
        return

    channel_map=openv_channel_map
    print(f'Using channel_map={channel_map}')

    #get list of records in the file
    records = h5_file.get_all_record_ids()

    #pick which records to process based on cmdline inputs
    if nskip > len(records):
        print(f'Requested records to skip {nskip} is greater than number of records {len(records)}. Exiting...')
        return
    if nrecords>0:
        if (nskip+nrecords)>len(records):
            nrecords=-1
        else:
            nrecords=nskip+nrecords
    records_to_process = []
    if nrecords==-1:
        records_to_process = records[nskip:]
    else:
        records_to_process = records[nskip:nrecords]
    print(f'Will process {len(records_to_process)} of {len(records)} records.')

    #loop over the records
    for r in records_to_process:

        #get the record index and print it
        record_index = RecordDataBase(run=run_number,trigger=r[0],sequence=r[1])
        print(f'Processing ',record_index)

        #get the trigger record header data and print it
        trh = h5_file.get_trh(record_index.trigger,record_index.sequence)
        n_frags = len(h5_file.get_fragment_dataset_paths(record_index.trigger,record_index.sequence))
        trh_data = rawdatautils.unpack.utils.TriggerRecordHeaderUnpacker().get_trh_data(trh,n_frags)[0]
        print(trh_data)

        # Process each detector type in the list
        for det_type in det:

            #get all geo_ids for this type
            geo_ids = h5_file.get_geo_ids_for_subdetector(r,detdataformats.DetID.string_to_subdetector(det_type))
            
            #loop through geo_ids
            for gid in geo_ids:

                #print basic information from geo id
                det_stream = 0xffff & (gid >> 48);
                det_slot = 0xffff & (gid >> 32);
                det_crate = 0xffff & (gid >> 16);
                det_id = 0xffff & gid;
                subdet = detdataformats.DetID.Subdetector(det_id)
                det_name = detdataformats.DetID.subdetector_to_string(subdet)
                print(f'\tProcessing subdetector {det_name}, '
                      f'crate {det_crate}, '
                      f'slot {det_slot}, '
                      f'stream {det_stream}')

                #get the fragment
                frag = h5_file.get_frag(r,gid)

                unpacker = rawdatautils.unpack.utils.FragmentUnpacker()
                frag_header = unpacker.get_frh_data(frag)[0]
                print('\t',frag_header)

                #get fragment type and pick the correct unpacker
                frag_type = frag.get_fragment_type()
                if frag_type==daqdataformats.FragmentType.kWIBEth:
                    unpacker = rawdatautils.unpack.utils.WIBEthUnpacker(channel_map=channel_map,ana_data_prescale=1,wvfm_data_prescale=1)
                elif frag_type==daqdataformats.FragmentType.kTDEEth:
                    unpacker = rawdatautils.unpack.utils.TDEEthUnpacker(channel_map=channel_map,ana_data_prescale=1,wvfm_data_prescale=1)
                else:
                    print(f'\tUnknown fragment type {frag_type}. Continuing...')
                    continue

                n_frames = unpacker.get_n_obj(frag)
                print(f'Found {n_frames} frames in this fragment.')
                if n_frames==0:
                    continue

                daq_header_data = unpacker.get_daq_header_data(frag)
                det_header_data = unpacker.get_det_header_data(frag)

                for i_deth, deth in enumerate(det_header_data):
                    print(f'\t',daq_header_data[i_deth])
                    print(f'\t',deth)

                det_ana_data, det_wvfm_data = unpacker.get_det_data_all(frag)

                if print_adc_stats:
                    for det_ana in det_ana_data:
#                        print(f'\t\tChannel {det_ana.channel} adc stats:')
                        print('\t\t',det_ana)

                if print_wvfm_samples:
                    for det_wvfm in det_wvfm_data:
                        print(f'\t\tChannel {det_wvfm.channel} waveform:')
                        for i_sample in range(print_wvfm_samples):
                            ts_str = np.format_float_positional(det_wvfm.timestamps[i_sample])
                            print(f'\t\t\t {i_sample:>5}:  ts={ts_str:<25}  val={det_wvfm.adcs[i_sample]}')


    #end record loop

    print(f'Processed all requested records')

if __name__ == '__main__':
    main()
