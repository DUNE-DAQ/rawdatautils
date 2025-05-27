#!/usr/bin/env python3

from hdf5libs import HDF5RawDataFile
import h5py

import daqdataformats
import trgdataformats
import detchannelmaps

import click
import time
import numpy as np
import sys

from rawdatautils.unpack.dataclasses import TriggerPrimitiveData

@click.command()
@click.argument('filenames', nargs=-1, type=click.Path(exists=True))
@click.option('--nrecords', '-n', default=-1, help='How many records to process (default: all)')
@click.option('--nskip', default=0, help='How many records to skip (default: 0)')
@click.option('--channel-map', default=None, help="Channel map to load (default: None)")

def main(filenames, nrecords, nskip, channel_map):

    for filename in filenames:

        
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

        
        #have channel numbers per geoid in here
        ch_map = None
        if channel_map is not None:
            if 'TPC' in channel_map:
                ch_map = detchannelmaps.make_tpc_map(channel_map)
            elif 'PDS' in channel_map:
                ch_map = detchannelmaps.make_pds_map(channel_map)

        with h5py.File(h5_file.get_file_name(), 'r') as f:
            record_type = f.attrs["record_type"]
            print(f'Record type is {record_type}')
        
        for r in records_to_process:

            print(f'Processing (Record Number,Sequence Number)=({r[0],r[1]})')

            src_ids = h5_file.get_source_ids(r)

            for sid in src_ids:
                if(sid.subsystem!=daqdataformats.SourceID.Subsystem.kTrigger): continue

                frag = h5_file.get_frag(r,sid)
                if(frag.get_fragment_type()!=daqdataformats.FragmentType.kTriggerPrimitive): continue

                print(f'Fragment (run,trigger,sequence)=({frag.get_run_number()},{frag.get_trigger_number()},{frag.get_sequence_number()})')
                    
                n_tps = int(frag.get_data_size() / trgdataformats.TriggerPrimitive.sizeof())

                print(f'Found {n_tps} TPs in fragment.')

                for i in range(n_tps):
                    tp = trgdataformats.TriggerPrimitive(frag.get_data(i*trgdataformats.TriggerPrimitive.sizeof()))
                    if tp.version != trgdataformats.TriggerPrimitive.s_trigger_primitive_version:
                        sys.exit(f"ERROR: the TP data structure version found in the data ({tp.version}) does not match the version expected by this version of the software ({trgdataformats.TriggerPrimitive.s_trigger_primitive_version}). Please use a version of the software that matches the data.")

                    plane=-1
                    apa="Unknown"
                    if ch_map is not None and (tp.detid==3 or tp.detid==10):
                        plane=ch_map.get_plane_from_offline_channel(tp.channel)
                        apa=ch_map.get_tpc_element_from_offline_channel(tp.channel)
                        
                    tpd = TriggerPrimitiveData(run=frag.get_run_number(),
                                               trigger=frag.get_trigger_number(),
                                               sequence=frag.get_sequence_number(),
                                               src_id=frag.get_element_id().id,
                                               time_start=tp.time_start,
                                               samples_to_peak=tp.samples_to_peak,
                                               samples_over_threshold=tp.samples_over_threshold,
                                               channel=tp.channel,
                                               plane=plane,
                                               apa=apa,
                                               adc_integral=tp.adc_integral,
                                               adc_peak=tp.adc_peak,
                                               detid=tp.detid,
                                               flag=tp.flag,
                                               id_ta=-1)
                    print(tpd)
                    
                
            
    #end file loop
    print(f'Processed all requested records')

    
if __name__ == '__main__':
    main()
