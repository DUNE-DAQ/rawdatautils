from hdf5libs import HDF5RawDataFile
import daqdataformats
from rawdatautils.unpack.wib import *
import detdataformats.wib
import click
import time
import numpy as np

@click.command()
@click.argument('filename', type=click.Path(exists=True))
@click.option('--tr-count', default=-1, help='How many Trigger Records to test')
def main(filename, tr_count):

    h5_file = HDF5RawDataFile(filename)

    records = h5_file.get_all_record_ids()
    records_to_process = records[0:tr_count]
    print(f'Will process {len(records_to_process)} of {len(records)} records.')

    for r in records_to_process:
        print(f'Processing (Record Number,Sequence Number)=({r[0],r[1]})')
        wib_geo_ids = h5_file.get_geo_ids(r,daqdataformats.GeoID.SystemType.kTPC)
        for gid in wib_geo_ids:
            print(f'\tProcessing geoid {gid}')
            frag = h5_file.get_frag(r,gid)
            frag_ts = frag.get_trigger_timestamp()

            n_frames = (frag.get_size()-frag_hdr.sizeof())//detdataformats.wib.WIBFrame.sizeof()

            ts = np.zeros(n_frames,dtype='uint64')
            adcs = np.zeros((n_frames,256),dtype='uint16')

            #unpack timestamps into numpy array of uin64
            #timestamps = np_array_timestamp(frag)

            #unpack adcs into a n_frames x 256 numpy array of uint16
            adcs = np_array_adc(frag)

            adcs_rms = np.std(adcs,axis=0)
            for ch,rms in enumerate(adcs_rms):
                print(f'\t\tch {ch}: rms = {rms}')

        #end gid loop
    #end record loop

    print(f'Processed all requested records')

if __name__ == '__main__':
    main()
