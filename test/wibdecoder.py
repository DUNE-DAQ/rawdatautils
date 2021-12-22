import detdataformats.wib as wib
import detdataformats
from hdf5libs import DAQDecoder
import time

import hdf5libs
import daqdataformats
import detdataformats
import detchannelmaps

import numpy as np

dec = DAQDecoder('np02_bde_coldbox_run011918_0001_20211029T122926.hdf5', 100)
datasets = dec.get_datasets()

trname = [ name for name in datasets if 'TriggerRecordHeader' not in name]
for i, d in enumerate(trname):
    frag = dec.get_frag_ptr(d)
    frag_hdr = frag.get_header()

    n_frames = (frag.get_size()-frag_hdr.sizeof())//detdataformats.wib.WIBFrame.sizeof()

    wf = detdataformats.wib.WIBFrame(frag.get_data())
    wh = wf.get_wib_header()

    ts = np.zeros(n_frames, dtype='uint64')
    adcs = np.zeros((n_frames, 256), dtype='uint16')

    if i < 5:
        print(f'Testing the TR number {i}')
        t0 = time.time()
        for j in range(n_frames):
            wf = detdataformats.wib.WIBFrame(frag.get_data(j*detdataformats.wib.WIBFrame.sizeof())) 
            ts[j] = wf.get_timestamp()
            adcs[j] = [wf.get_channel(k) for k in range(256)]
        print(f'Time to decode with the python for loop       {time.time() - t0:.3f} s')

        t0 = time.time()
        timestamps = wib.numpy_timestamps(frag.get_data(), n_frames)
        ary = wib.numpy_decoder(frag.get_data(), n_frames)
        print(f'Time to decode with the C++ -> numpy function {time.time() - t0:.3f} s')

        if (adcs == ary).all() and (ts == timestamps).all():
            print(f'The arrays obtained for TR number {i} are the same for the python for loop and the C++ -> numpy functions')
        else:
            print('Test failed, the python for loop and the C++ -> numpy functions return different results')
