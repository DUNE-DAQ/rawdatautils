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

    sequence_ids = [] # print only if a problem
    tr_global_stats = {}
    
    for r in records_to_process:

        print("")
        print(f'Processing (Record Number,Sequence Number)=({r[0],r[1]})')
        dset_paths = h5_file.get_fragment_dataset_paths(r)
        sequence_ids.append(r[1])
        tr_stats = {}
        
        for dset_path in dset_paths:
            frag = h5_file.get_frag(dset_path)
            print(f"{frag.get_fragment_type()} {frag.get_trigger_number()} {frag.get_sequence_number()} {frag.get_size()}") #  {int(frag.get_error_bits())}
            if frag.get_fragment_type() in tr_stats:
                tr_stats[ frag.get_fragment_type()]["count" ] += 1
                if frag.get_size() > tr_stats[ frag.get_fragment_type() ]["max_size" ]:
                    tr_stats[ frag.get_fragment_type() ][ "max_size" ] = frag.get_size()
                if frag.get_size() < tr_stats[ frag.get_fragment_type()]["min_size" ]:
                    tr_stats[ frag.get_fragment_type()]["min_size" ] = frag.get_size()
            else:
                tr_stats[ frag.get_fragment_type() ] = { "count":1, "max_size": frag.get_size(), "min_size": frag.get_size() }

        for frag_type in tr_stats:
            if frag_type in tr_global_stats:
                if tr_stats[frag_type]["count"] > tr_global_stats[frag_type]["max_count"]:
                    tr_global_stats[frag_type]["max_count"] = tr_stats[frag_type]["count"]
                if tr_stats[frag_type]["count"] < tr_global_stats[frag_type]["min_count"]:
                    tr_global_stats[frag_type]["min_count"] = tr_stats[frag_type]["count"]
                if tr_stats[frag_type]["max_size"] > tr_global_stats[frag_type]["max_size"]:
                    tr_global_stats[frag_type]["max_size"] = tr_stats[frag_type]["max_size"]
                if tr_stats[frag_type]["min_size"] < tr_global_stats[frag_type]["min_size"]:
                    tr_global_stats[frag_type]["min_size"] = tr_stats[frag_type]["min_size"]

            else:
                tr_global_stats[frag_type] = { "max_count": tr_stats[frag_type]["count"],
                                               "min_count": tr_stats[frag_type]["count"],
                                               "max_size": tr_stats[frag_type]["max_size"],
                                               "min_size": tr_stats[frag_type]["min_size"]
                                               }


    if len(set(sequence_ids)) == 1:
        print(f"All sequence #'s are the same, {sequence_ids[0]}")
    elif len(set(sequence_ids)) == len(sequence_ids) and \
         max(sequence_ids) - min(sequence_ids) + 1 == len(sequence_ids):
        print(f"All sequence #'s follow one another, counting from {min(sequence_ids)} to {max(sequence_ids)}")
    else:
        print("Sequence #'s are neither identical nor increasing one-by-one: ")
        print(" ".join([str(seqid) for seqid in sequence_ids]))
        print("")
        
    for frag_type in tr_global_stats:
        print(f"{frag_type}: max # in a record == %s, min # in a record == %s, max size == %s bytes, min size == %s bytes" %
              (tr_global_stats[frag_type]["max_count"], tr_global_stats[frag_type]["min_count"],
               tr_global_stats[frag_type]["max_size"], tr_global_stats[frag_type]["min_size"]))

    
        
if __name__ == '__main__':
    main()
