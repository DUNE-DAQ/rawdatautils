#!/usr/bin/env python3

import fddetdataformats
from rawdatautils.utilities.wibeth import *

import click
import time


@click.command()
@click.argument('filenames', nargs=-1, type=click.Path(exists=True))
@click.option('--frame-type', '-t', type=click.Choice(['wibeth','daphne','tdeeth'], case_sensitive=False), default='wibeth', help="Frame type for decoding")
@click.option('--print-headers', is_flag=True, help="Print Frame headers")

def main(filenames, frame_type, print_headers):

    frame_size = 0
    if frame_type == 'wibeth':
        frame_size = fddetdataformats.WIBEthFrame.sizeof()
    elif frame_type == 'tdeeth':
        frame_size = fddetdataformats.TDEEthFrame.sizeof()
    elif frame_type == 'daphne':
        frame_size = fddetdataformats.DAPHNEFrame.sizeof()

    for filename in filenames:
        with open(filename, 'rb') as ff:
            frame_counter = 0
            first_timestamp = -1
            last_timestamp = -1
            while True: # Loop over frames in file
                frame = ff.read(frame_size)
                if not frame:
                    break
                frame_counter += 1

                if frame_type == 'wibeth':
                    wf = fddetdataformats.WIBEthFrame(frame)
                    timestamp = wf.get_timestamp()
                    if first_timestamp == -1 or timestamp < first_timestamp:
                        first_timestamp = timestamp
                    if last_timestamp == -1 or timestamp > last_timestamp:
                        last_timestamp = timestamp

                    #print header info
                    if print_headers:
                        print('\n\t==== WIBETH HEADER ====')
                        print_header(wf,prefix='\t\t')
                if frame_type == 'tdeeth':
                    tf = fddetdataformats.TDEEthFrame(frame)
                    timestamp = tf.get_timestamp()

                    if first_timestamp == -1 or timestamp < first_timestamp:
                        first_timestamp = timestamp
                    if last_timestamp == -1 or timestamp > last_timestamp:
                        last_timestamp = timestamp

                    #print header info
                    if print_headers:
                        print('\n\t==== TDE ETH HEADER PRINT NOT SUPPORTED ====')
                if frame_type == 'daphne':
                    df = fddetdataformats.DAPHNEFrame(frame)
                    timestamp = df.get_timestamp()

                    if first_timestamp == -1 or timestamp < first_timestamp:
                        first_timestamp = timestamp
                    if last_timestamp == -1 or timestamp > last_timestamp:
                        last_timestamp = timestamp

                    #print header info
                    if print_headers:
                        print('\n\t==== DAPHNE HEADER PRINT NOT SUPPORTED ====')


            print(f'\n==== FILE {filename} SUMMARY ====')
            print(f'{frame_counter} frames, first timestamp {first_timestamp}, last timestamp {last_timestamp}')

    #end file loop
    print(f'Processed all requested frames')

    
if __name__ == '__main__':
    main()
