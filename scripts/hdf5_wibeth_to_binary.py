#!/usr/bin/env python3
"""
Created on: 21/02/2023 15:28

Author: Shyam Bhuller

Description: Python script to write WIBEth frames from DUNE-DAQ HDF5 files to binary files.
"""

import click

from hdf5libs import HDF5RawDataFile
import daqdataformats
import fddetdataformats

from rich import print


def Debug(x):  
    """ print if we are in debug mode

    Args:
        x (any): thing to prints
    """      
    if debug:
        print(x)


def get_wib_fragments(h5file : HDF5RawDataFile, record_num : int, frame : str) -> list:
    """ get the fragments which correspond to WIBEth frames.

    Args:
        h5file (HDF5RawDataFile): Trigger record file.
        record_num (int): Trigger record number.
        frame (str): Frame type.

    Returns:
        list: list of WIBEth fragments.
    """
    fragments = h5file.get_fragment_dataset_paths(record_num+1)
    fragments = [f for f in fragments if f.split("_")[-1] == frame] # exclude other fragments in the record that are not WIB frames
    return fragments


@click.command()
@click.argument('filename', type=click.Path(exists=True))
@click.option('--stream', '-s', default=1, help='Which detector stream to convert to binary (default: 1).')
@click.option('--nrecords', '-n', default=-1, help='How many Trigger Records to process (default: all)')
@click.option('--frame', '-f', default='WIBEth', type=click.Choice(['ProtoWIB', 'WIB2', 'WIBEth']))
@click.option('--verbose', '-v', is_flag=True, help="Print more output.")
def main(filename, stream, nrecords, frame, verbose):
    global debug
    debug = verbose

    h5file = HDF5RawDataFile(filename)

    total_records = len(h5file.get_all_record_ids())
    if nrecords == -1:
        nrecords = total_records
    if nrecords > total_records:
        raise Exception(f"Number of specified records is greater than the total {total_records}")

    n_links = len(get_wib_fragments(h5file, 0, frame)) # exclude other fragments in the record that are not WIB frames
    if stream >= n_links:
        raise Exception(f"Link number specified {stream} out of range {n_links}.")

    out_name = f"wib_link_{stream}.bin"
    with open(out_name, "wb") as bf:
        total_frames = 0
        # loop over all triggers
        for i in range(nrecords):
            header = h5file.get_record_header_dataset_path(i+1) # trigger number starts at 1
            Debug(header)

            fragments = get_wib_fragments(h5file, i, frame)

            Debug(f"loading fragment: {fragments[stream]}")
            f = h5file.get_frag(fragments[stream])
            
            WIBEthFrame_size = fddetdataformats.WIBEthFrame.sizeof()

            n_frames = (f.get_size() - f.get_header().sizeof()) // WIBEthFrame_size # calculate the number of wib frames per fragment
            for j in range(n_frames):
                Debug(f.get_fragment_type())
                Debug(f.get_element_id())

                data = fddetdataformats.WIBEthFrame(f.get_data(j * WIBEthFrame_size)) # unpack fragment to WIB2Frame
                
                Debug(f"{data.sizeof()=}")

                bf.write(bytearray(data.get_bytes())) # write binary data to the file
            total_frames += j
            print(f"writing {total_frames} WIBEth frames to binary file.", "\r")
    print(f"wrote {nrecords} fragments from wib link {stream} to file {out_name}.")
    return

if __name__ == "__main__":
    main()