#!/usr/bin/env python3

from hdf5libs import HDF5RawDataFile

import daqdataformats
import detdataformats
from rawdatautils.unpack.tde import *
from rawdatautils.unpack.utils import *
import detchannelmaps

import click
import time
import numpy as np


@click.command()
@click.argument('filename', type=click.Path(exists=True))
@click.option('--nrecords', '-n', default=-1, help='How many Trigger Records to process (default: all)')
@click.option('--nskip', default=0, help='How many Trigger Records to skip (default: 0)')
@click.option('--print-adc-stats', is_flag=True, help="Print ADC Pedestals/RMS")
@click.option('--print-wvfm-samples', default=0, help='How many samples in each waveform to print.')
@click.option('--det', default='VD_Top_TPC', help='Subdetector string (default: VD_TopTPC)')

def main(filename, nrecords, nskip, print_adc_stats, print_wvfm_samples, det):

    h5_file = HDF5RawDataFile(filename)

    records = h5_file.get_all_record_ids()

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

    unpacker = TDEEthUnpacker(channel_map=None,ana_data_prescale=1,wvfm_data_prescale=1)

    for r in records_to_process:

        print(f'Processing (Record Number,Sequence Number)=({r[0],r[1]})')
        geo_ids = h5_file.get_geo_ids_for_subdetector(r,detdataformats.DetID.string_to_subdetector(det))

        for gid in geo_ids:

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

            #perform unpacking of fragment and detector header info
            frag_header = unpacker.get_frh_data(frag)[0]

            print('\t',frag_header)

            n_frames = unpacker.get_n_obj(frag)
            print(f'Found {n_frames} TDE frames in this fragment.')
            if n_frames==0:
                continue

            daq_header_data = unpacker.get_daq_header_data(frag)
            tde_header_data = unpacker.get_det_header_data(frag)

            for i_tdeh, tdeh in enumerate(tde_header_data):
                print(f'\tDAQ header {i_tdeh}: ',daq_header_data[i_tdeh])
                print(f'\tTDE header info: ',tdeh)

            tde_ana_data, tde_wvfm_data = unpacker.get_det_data_all(frag)

            if print_adc_stats:
                for tde_ana in tde_ana_data:
                    print(f'\t\tTDE channel {tde_ana.channel} adc stats:')
                    print('\t\t',tde_ana)

            if print_wvfm_samples:
                for tde_wvfm in tde_wvfm_data:
                    print(f'\t\tTDE channel {tde_wvfm.channel} waveform:')
                    for i_sample in range(print_wvfm_samples):
                        ts_str = np.format_float_positional(tde_wvfm.timestamps[i_sample])
                        print(f'\t\t\t {i_sample:>5}:  ts={ts_str:<25}  val={tde_wvfm.adcs[i_sample]}')


    #end record loop

    print(f'Processed all requested records')

if __name__ == '__main__':
    main()
