from hdf5libs import HDF5RawDataFile

import daqdataformats
import detdataformats.wib2
from rawdatautils.unpack.wib2 import *
import detchannelmaps

import click
import time
import numpy as np

def print_wib2_header(header):
    print(f'\tVersion: {header.version}')
    print(f'\tDetector ID: {header.detector_id}')
    print(f'\t(Crate,Slot,Link): ({header.crate},{header.slot},{header.link})')
    print(f'\t(Timestamp): {header.timestamp_1 + (header.timestamp_2 << 32)}')
    print(f'\tColddata Timestamp ID: {header.colddata_timestamp_id}')
    print(f'\tFEMB Valid: {header.femb_valid}')
    print(f'\tLink Mask: {header.link_mask}')
    print(f'\tLock output status: {header.lock_output_status}')
    print(f'\tFEMB Pulser Frame Bits: {header.femb_pulser_frame_bits}')
    print(f'\tFEMB Sync Flags: {header.femb_sync_flags}')
    print(f'\tColddata Timestamp: {header.colddata_timestamp}')


@click.command()
@click.argument('filename', type=click.Path(exists=True))
@click.option('--tr-count', default=-1, help='How many Trigger Records to test')
@click.option('--channel-map', default=None, help="Channel map to load (default: None)")
def main(filename, tr_count, channel_map):

    h5_file = HDF5RawDataFile(filename)

    records = h5_file.get_all_record_ids()
    records_to_process = records[0:tr_count]
    print(f'Will process {len(records_to_process)} of {len(records)} records.')

    #have channel numbers per geoid in here
    ch_map = None
    if channel_map is not None:
        ch_map = detchannelmaps.make_map(channel_map)
    offline_ch_num_dict = {}

    for r in records_to_process:
        print(f'\n\n\nProcessing (Record Number,Sequence Number)=({r[0],r[1]})')
        wib_geo_ids = h5_file.get_geo_ids(r,daqdataformats.GeoID.SystemType.kTPC)
        timestamps_all = np.zeros((8192,len(wib_geo_ids)),dtype='int64')
        cd_timestamps_all = np.zeros((8192,len(wib_geo_ids)),dtype='int32')
        for gid in wib_geo_ids:
            #print(f'\n\tProcessing geoid {gid}')

            frag = h5_file.get_frag(r,gid)
            frag_hdr = frag.get_header()
            frag_ts = frag.get_trigger_timestamp()

            #print(f'\tTrigger timestamp for fragment is {frag_ts}')

            n_frames = (frag.get_size()-frag_hdr.sizeof())//detdataformats.wib2.WIB2Frame.sizeof()
            #print(f'\tFound ({frag.get_size()} - {frag_hdr.sizeof()}) / {detdataformats.wib2.WIB2Frame.sizeof()} = {n_frames} WIB2 Frames.')

            wf0 = detdataformats.wib2.WIB2Frame(frag.get_data())
            wh0 = wf0.get_header()

            #print('\t\t\t====WIB HEADER 0====')
            #print_wib2_header(wh0)

            if(offline_ch_num_dict.get(gid) is None):
                if channel_map is None:
                    offline_ch_num_dict[gid] = range(256)
                else:
                    offline_ch_num_dict[gid] = [ch_map.get_offline_channel_from_crate_slot_fiber_chan(wh0.crate, wh0.slot, wh0.link, c) for c in range(256)]


            #unpack timestamps into numpy array of uin64
            timestamps = np_array_timestamp(frag)
            timestamps_diff = np.diff(timestamps)
            timestamps_diff_vals, timestamps_diff_counts = np.unique(timestamps_diff, return_counts=True)

            cd_timestamps = np_array_colddata_timestamp(frag)
            cd_timestamps_diff = np.diff(cd_timestamps)
            cd_timestamps_diff_vals, cd_timestamps_diff_counts = np.unique(cd_timestamps_diff, return_counts=True)

            timestamps_all[:, gid.element_id] = timestamps
            cd_timestamps_all[:, gid.element_id] = cd_timestamps

            print(f'\nElement {gid.element_id}:')
            print(f'Number of Frames: {len(timestamps)}')
            print(f'(Crate,Slot,Link): ({wh0.crate},{wh0.slot},{wh0.link})')

            if(n_frames>0):
                print(f'Timestamps (First, Last, Min, Max): ({timestamps[0]},{timestamps[-1]},{np.min(timestamps)},{np.max(timestamps)})')
                print(f'Timestamp diffs: {timestamps_diff_vals}')
                print(f'Timestamp diff counts: {timestamps_diff_counts}')
                print(f'Average diff: {np.mean(timestamps_diff)}')

                print(f'Coldata Timestamps (First, Last, Min, Max): ({cd_timestamps[0]},{cd_timestamps[-1]},{np.min(cd_timestamps)},{np.max(cd_timestamps)})')
                print(f'Coldata Timestamp diffs: {cd_timestamps_diff_vals}')
                print(f'Coldata Timestamp diff counts: {cd_timestamps_diff_counts}')
                print(f'Coldata Average diff: {np.mean(cd_timestamps_diff)}')

#            if len(timestamps_diff_vals)==3:
#                for i in range(len(timestamps_diff)):
#                    if timestamps_diff[i]!=32 and i!=(len(timestamps_diff)-1):
#                        print(i,timestamps_diff[i],timestamps_diff[i+1])


            #unpack adcs into a n_frames x 256 numpy array of uint16
            adcs = np_array_adc(frag)

            adcs_rms = np.std(adcs,axis=0)
            adcs_ped = np.mean(adcs,axis=0)

            #print('====WIB DATA====')

            #for ch,rms in enumerate(adcs_rms):
            #    print(f'\t\tch {offline_ch_num_dict[gid][ch]}: ped = {adcs_ped[ch]}, rms = {adcs_rms[ch]}')

        #end gid loop

    for iframe in range(100):
        print(f'Frame {iframe}:\n\tWIB Timestamp (diff from WIB0 Link0): {[timestamps_all[iframe][ie]-timestamps_all[iframe][0] for ie in range(10) ]}',
              f'\n\tWIB COLDDATA Timestamp (diff from WIB0 Link0): {[cd_timestamps_all[iframe][ie] - cd_timestamps_all[iframe][0] for ie in range(10) ]}')
    #end record loop

    print(f'Processed all requested records')

if __name__ == '__main__':
    main()
