#!/usr/bin/env python3

from hdf5libs import HDF5RawDataFile

import daqdataformats
import detdataformats
from rawdatautils.unpack.wib2 import *
from rawdatautils.utilities.wib2 import *
import detchannelmaps

import click
import time
import numpy as np


@click.command()
@click.argument('filename', type=click.Path(exists=True))
@click.option('--nrecords', '-n', default=-1, help='How many Trigger Records to process (default: all)')

def main(filename, nrecords):

    h5_file = HDF5RawDataFile(filename)

    records = h5_file.get_all_trigger_record_ids()

    records_to_process = []
    if nrecords==-1:
        records_to_process = records
    else:
        records_to_process = records[:nrecords]
    print(f'Will process {len(records_to_process)} of {len(records)} records.')

    for r in records_to_process:

        print("")
        print(f'Processing (Record Number,Sequence Number)=({r[0],r[1]})')
        dset_paths = h5_file.get_fragment_dataset_paths(r)

        for dset_path in dset_paths:
            frag = h5_file.get_frag(dset_path)
            print(f"{frag.get_fragment_type()} {frag.get_trigger_number()} {frag.get_sequence_number()} {frag.get_size()}") #  {int(frag.get_error_bits())}

if __name__ == '__main__':
    main()
