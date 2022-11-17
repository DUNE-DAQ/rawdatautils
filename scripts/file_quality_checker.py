#!/usr/bin/env python3

from hdf5libs import HDF5RawDataFile

import detchannelmaps
import daqdataformats
import detdataformats
import glob
import os
from rawdatautils.unpack.wib2 import *
from rawdatautils.utilities.wib2 import *


import click
import time
import numpy as np


@click.command()
@click.argument('filenames', nargs=-1)
@click.option('--nrecords', '-n', default=-1, help='How many Trigger Records to process (default: all)')

def main(filenames, nrecords):
    
    for filename in filenames:

        if not os.path.exists(filename):
            sys.exit(f"ERROR: file \"{filename}\" doesn't appear to exist")
        
        try:
            h5file = HDF5RawDataFile(filename)
        except:
            sys.exit(f"ERROR: file \"{filename}\" couldn't be opened; is it an HDF5 file?")

        print(f"Processing {filename}...")
        records = h5file.get_all_trigger_record_ids()

        records_to_process = []
        if nrecords==-1:
            records_to_process = records
        else:
            records_to_process = records[:nrecords]
        print(f'Will process {len(records_to_process)} of {len(records)} trigger records.')

        sequence_ids = [] # print only if a problem
        tr_global_stats = {}

        for i_r, r in enumerate(records_to_process):

            for i_quadrant in range(1,4):
                if i_r == i_quadrant * len(records_to_process)/4:
                    print(f"Processed {i_r} of {len(records_to_process)} records...")
            
            #print("")
            #print(f'Processing (Record Number,Sequence Number)=({r[0],r[1]})')
            dset_paths = h5file.get_fragment_dataset_paths(r)
            sequence_ids.append(r[1])
            tr_stats = {}

            for dset_path in dset_paths:
                frag = h5file.get_frag(dset_path)
                #print(f"{frag.get_fragment_type()} {frag.get_trigger_number()} {frag.get_sequence_number()} {frag.get_size()}") #  {int(frag.get_error_bits())}
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

        print(f"Processed {len(records_to_process)} of {len(records_to_process)} records...")
        print("")
        if len(set(sequence_ids)) == 1:
            print(f"All sequence #'s are the same, {sequence_ids[0]}")
        elif len(set(sequence_ids)) == len(sequence_ids) and \
             max(sequence_ids) - min(sequence_ids) + 1 == len(sequence_ids):
            print(f"All sequence #'s follow one another, counting from {min(sequence_ids)} to {max(sequence_ids)}")
        else:
            print("Sequence #'s are neither identical nor increasing one-by-one: ")
            print(" ".join([str(seqid) for seqid in sequence_ids]))
            print("")
        print("")

        frag_type_phrase_length = max([len(str(fragname)) - len("FragmentType.") for fragname in tr_global_stats]) + 1
        max_count_phrase = " max # in a record "
        min_count_phrase = " min # in a record "
        max_size_phrase = " max size in a record (bytes) "
        min_size_phrase = " min size in a record (bytes) "

        fmtstring=f"%-{frag_type_phrase_length}s|%s|%s|%s|%s"
        print(fmtstring % (" FragType ", max_count_phrase, min_count_phrase, max_size_phrase, min_size_phrase))

        print("-" * (frag_type_phrase_length + len(max_count_phrase) + len(min_count_phrase) +
                     len(max_size_phrase) + len(min_size_phrase) + 5))
        
        for frag_type in tr_global_stats:
            fmtstring = f"%-{frag_type_phrase_length}s|%-{len(max_count_phrase)}s|%-{len(min_count_phrase)}s|%-{len(max_size_phrase)}s|%-{len(min_size_phrase)}s|"
            print(fmtstring % (str(frag_type).replace("FragmentType.",""),
                               tr_global_stats[frag_type]["max_count"],
                               tr_global_stats[frag_type]["min_count"],
                               tr_global_stats[frag_type]["max_size"],
                               tr_global_stats[frag_type]["min_size"]
                               ))

            print("-" * (frag_type_phrase_length + len(max_count_phrase) + len(min_count_phrase) +
                         len(max_size_phrase) + len(min_size_phrase) + 5))
        print("")
            
if __name__ == '__main__':
    main()
