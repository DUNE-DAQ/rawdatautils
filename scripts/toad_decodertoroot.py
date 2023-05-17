#!/usr/bin/env python3

from hdf5libs import HDF5RawDataFile

import daqdataformats
import detdataformats
from rawdatautils.unpack.toad import *
from rawdatautils.utilities.toad import *

import ROOT
import sys
import click
import time
import numpy as np
import os
import array

@click.command()
@click.argument('filename', type=click.Path(exists=True))
@click.argument('channelmapfile', type=click.Path(exists=True))
#@click.option('--nrecords', '-n', default=-1, help='How many Trigger Records to process (default: all)')
#@click.option('--nskip', default=0, help='How many Trigger Records to skip (default: 0)')
#@click.option('--print-headers', is_flag=True, help="Print TOADFrame headers")
#@click.option('--print-adc-stats', is_flag=True, help="Print ADC Samples")

def main(filename, channelmapfile):

    h5_file = HDF5RawDataFile(filename)

    records = h5_file.get_all_record_ids()

    print(f'Will process {len(records)} records.')

    chanmap = np.genfromtxt(channelmapfile, dtype = int)
    garsoftindex = chanmap[:, 0]
    toadindex = chanmap[:, -1]
    filenamenopath = os.path.basename(filename)
    filetuple = os.path.splitext(filenamenopath)
    rootfileout = filetuple[0]
    fout = ROOT.TFile.Open(rootfileout,"RECREATE");
    tree = ROOT.TTree("tree","OROC channel map");
    ind = array.array('i',[0])
    timestamp = array.array('i',[0])
    adcsample = array.array('s',[0])
    tree.Branch("ind",ind,"ind/i");
    tree.Branch("timestamp",timestamp,"timestamp/i");
    tree.Branch("adcsample",adcsample,"adcsample/s");

    for r in records:

        print(f'Processing (Record Number,Sequence Number)=({r[0],r[1]})')
        toad_geo_ids = h5_file.get_geo_ids_for_subdetector(r,detdataformats.DetID.Subdetector.kND_GAr)

        for gid in toad_geo_ids:
            hex_gid = format(gid, '016x')
            print(f'\tProcessing geoid {hex_gid}')

            frag = h5_file.get_frag(r,gid)
            frag_hdr = frag.get_header()
            frag_ts = frag.get_trigger_timestamp()

            print(f'\tTrigger timestamp for fragment is {frag_ts}')
            print(frag.get_size()) 
            frag_size = frag.get_size() - 72
            i = 0
            while i < frag_size:
                toad_f = detdataformats.toad.TOADFrameOverlay(frag.get_data(i))
                print('\n\t==== TOAD FRAGMENT ====')

                #Check if Timestamp Sync number is correct
                prefix = '\t\t'
                print(f'{prefix} Timestamp in ticks: {toad_f.tstmp}')
                print(f'{prefix} FEC number: {toad_f.fec}')
                print(f'{prefix} Header Parity Check: {toad_f.hdr_par_check}')
                print(f'{prefix} Number of samples: {toad_f.n_samples}')
                print(f'{prefix} Number of bytes: {toad_f.n_bytes}')

                
                #unpack adcs into a numpy array of uint16
                adcs = np_array_adc_data(frag.get_data(i), (toad_f.n_samples))
                adcs_rms = np.std(adcs,axis=0)
                adcs_ped = np.mean(adcs,axis=0)
                print(adcs)
                
                if(toad_f.n_samples != len(adcs)):
                    print("Error: n_samples not equal to length of sample array")

                nsamples = toad_f.n_samples
                garsoftindexindex = np.where(toadindex == toad_f.fec)
                for j in range(nsamples):
                    ind = garsoftindex[garsoftindexindex]
                    timestamp = toad_f.tstmp
                    adcsample = nsamples[j]
                    tree.Fill()

                print('\n\t====TOAD DATA====')
                i+=toad_f.n_bytes
            print("\n")
        #end gid loop       
    tree.Write()
    fout.Close()
    print(f'Processed all requested records')

if __name__ == '__main__':
    main()
