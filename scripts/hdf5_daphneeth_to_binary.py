#!/usr/bin/env python3
"""
Created on: 11/08/2026

Description: Python script to write DAPHNEEthFrame frames from DUNE-DAQ HDF5 files to binary files.
"""

import argparse

from hdf5libs import HDF5RawDataFile
import daqdataformats
from daqdataformats import FragmentType
import fddetdataformats

from rich import print


def Debug(x):
    """ print if we are in debug mode

    Args:
        x (any): thing to prints
    """
    if debug:
        print(x)


def get_daphneeth_fragments(h5file, record):
    """ get the fragment dataset paths in a record that hold DAPHNEEthFrame objects

    DAPHNEEthFrame objects are stored in fragments of type kDAPHNEEth (self-trigger).
    Other DAPHNE fragment types (kDAPHNE, kDAPHNEEthStream) hold different frame
    classes and are excluded here.

    Args:
        h5file (HDF5RawDataFile): the open HDF5 file
        record (tuple): record id

    Returns:
        list[str]: fragment dataset paths for DAPHNEEth fragments
    """
    daphneeth_fragments = []
    for f in h5file.get_fragment_dataset_paths(record):
        frag = h5file.get_frag(f)
        if frag.get_header().fragment_type == FragmentType.kDAPHNEEth.value:
            daphneeth_fragments.append(f)
    return daphneeth_fragments


def main(args):
    h5file = HDF5RawDataFile(args.file_name)

    records = h5file.get_all_record_ids()
    total_records = len(records)
    if args.n_records == -1:
        args.n_records = total_records
    if args.n_records > total_records:
        raise Exception(f"Number of specified records is greater than the total {total_records}")

    n_links = len(get_daphneeth_fragments(h5file, records[0])) # number of DAPHNEEth fragments in the first record
    if args.link >= n_links:
        raise Exception(f"Link number out of range. Found {n_links} DAPHNEEth fragments in the first record.")

    out_name = f"daphneeth_link_{args.link}.bin"
    with open(out_name, "wb") as bf:
        total_frames = 0
        # loop over all triggers
        for i in range(args.n_records):
            header = h5file.get_record_header_dataset_path(records[i]) # trigger number starts at 1
            Debug(header)

            fragments = get_daphneeth_fragments(h5file, records[i])

            Debug(f"loading fragment: {fragments[args.link]}")
            f = h5file.get_frag(fragments[args.link])

            DAPHNEEthFrame_size = fddetdataformats.DAPHNEEthFrame.sizeof()

            n_frames = (f.get_size() - f.get_header().sizeof()) // DAPHNEEthFrame_size # calculate the number of daphneeth frames per fragment
            for j in range(n_frames):
                Debug(f.get_fragment_type())
                Debug(f.get_element_id())

                data = fddetdataformats.DAPHNEEthFrame(f.get_data(j * DAPHNEEthFrame_size)) # unpack fragment to DAPHNEEthFrame

                Debug(f"{data.sizeof()=}")

                bf.write(bytearray(data.get_bytes())) # write binary data to the file
            total_frames += n_frames
            print(f"writing {total_frames} DAPHNEEth frames to binary file.", "\r")
    print(f"wrote {args.n_records} fragments from daphneeth link {args.link} to file {out_name}.")
    return

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'Python script to write DAPHNEEthFrame frames from DUNE-DAQ HDF5 files to binary files.')
    parser.add_argument(dest = "file_name", help = 'path to HDF5 file')
    parser.add_argument("-l", "--link", dest = "link", type = int, help = "link number (index of DAPHNEEth fragment) to convert to binary", required = True)
    parser.add_argument('-n', '--num-of-records', dest = "n_records", type = int, help = 'specify number of records to be parsed, -1 will parse all records', default = 0, required = True)
    parser.add_argument("--debug", dest = "debug", action = "store_true", help = "Debugging information")
    args = parser.parse_args()
    debug = args.debug
    Debug(vars(args))
    main(args)
