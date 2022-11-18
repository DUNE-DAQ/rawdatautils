#!/usr/bin/env python3

from hdf5libs import HDF5RawDataFile

import detchannelmaps
import daqdataformats
import detdataformats
import gc
import glob
import os
from rawdatautils.unpack.wib2 import *
from rawdatautils.utilities.wib2 import *
import sys
import time

import click
import time
import numpy as np


@click.command()
@click.argument('filenames', nargs=-1)
@click.option('--nrecords', '-n', default=-1, help='How many Records to process (default: all)')

def main(filenames, nrecords):
    
    for filename in filenames:

        if not os.path.exists(filename):
            sys.exit(f"ERROR: file \"{filename}\" doesn't appear to exist")

        try:
            h5file = HDF5RawDataFile(filename)
        except:
            sys.exit(f"ERROR: file \"{filename}\" couldn't be opened; is it an HDF5 file?")

        print(f"Processing {filename}...")

        is_trigger_records = True
        try:
            records = h5file.get_all_trigger_record_ids()
        except:
            is_trigger_records = False

        if not is_trigger_records:
            try:
                records = h5file.get_all_timeslice_ids()
            except:
                sys.exit(f"ERROR: neither get_all_trigger_record_ids() nor get_all_timeslice_ids() returned records")
    

        records_to_process = []
        if nrecords==-1:
            records_to_process = records
        else:
            records_to_process = records[:nrecords]

        if is_trigger_records:
            print(f'Will process {len(records_to_process)} of {len(records)} trigger records.')
        else:
            print(f'Will process {len(records_to_process)} of {len(records)} timeslice records.')
            
        sequence_ids = [] 
        record_ids = []
        tr_global_stats = {}

        assumed_sequence_id_step = -1
        assumed_record_id_step = -1
        first_sequence_id = -1
        first_record_id = -1
        
        for i_r, r in enumerate(records_to_process):

            for i_quadrant in range(1,4):
                if i_r == i_quadrant * len(records_to_process)/4:
                    print(f"Processed {i_r} of {len(records_to_process)} records...")

            record_ids.append(r[0])
            sequence_ids.append(r[1])
                    
            if i_r == 0:
                first_record_id = r[0]
                first_sequence_id = r[1]
            elif i_r == 1:
                assumed_record_id_step = r[0] - first_record_id
                assumed_sequence_id_step = r[1] - first_sequence_id
                    
            dset_paths = h5file.get_fragment_dataset_paths(r)
            tr_stats = {}

            for dset_path in dset_paths:
                frag = h5file.get_frag(dset_path)
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

        sequence_ids_ok = True
        for i_s, seqid in enumerate(sequence_ids):
            if i_s > 0:
                if seqid - sequence_ids[i_s - 1] != assumed_sequence_id_step:
                    sequence_ids_ok = False

        record_ids_ok = True
        for i_r, recid in enumerate(record_ids):
            if i_r > 0:
                if recid - record_ids[i_r - 1] != assumed_record_id_step:
                    record_ids_ok = False

        if record_ids_ok:
            print(f"Progression of record ids over records looks ok (expected change of {assumed_record_id_step} for each new record)")
        else:
            print("Unexpected progression of record ids over records")
            print(" ".join([str(recid) for recid in record_ids]))
                    
        if sequence_ids_ok:
            print(f"Progression of sequence ids over records looks ok (expected change of {assumed_sequence_id_step} for each new record)")
        else:
            print("Unexpected progression of sequence numbers over records: ")
            print(" ".join([str(seqid) for seqid in sequence_ids]))

        print("")
            

        frag_type_phrase_length = max([len(str(fragname)) - len("FragmentType.") for fragname in tr_global_stats]) + 1
        max_count_phrase = " max # in a record "
        min_count_phrase = " min # in a record "
        max_size_phrase = " max size in a record (bytes) "
        min_size_phrase = " min size in a record (bytes) "

        fmtstring=f"%-{frag_type_phrase_length}s|%s|%s|%s|%s"
        print(fmtstring % (" FragType ", min_count_phrase, max_count_phrase, min_size_phrase, max_size_phrase))

        print("-" * (frag_type_phrase_length + len(min_count_phrase) + len(max_count_phrase) +
                     len(min_size_phrase) + len(max_size_phrase) + 5))
        
        for frag_type in tr_global_stats:
            fmtstring = f"%-{frag_type_phrase_length}s|%-{len(min_count_phrase)}s|%-{len(max_count_phrase)}s|%-{len(min_size_phrase)}s|%-{len(max_size_phrase)}s|"
            print(fmtstring % (str(frag_type).replace("FragmentType.",""),
                               tr_global_stats[frag_type]["min_count"],
                               tr_global_stats[frag_type]["max_count"],
                               tr_global_stats[frag_type]["min_size"],
                               tr_global_stats[frag_type]["max_size"]
                               ))

            print("-" * (frag_type_phrase_length + len(min_count_phrase) + len(max_count_phrase) +
                         len(min_size_phrase) + len(max_size_phrase) + 5))
        print("")

        del h5file
        gc.collect()
            
if __name__ == '__main__':
    main()
