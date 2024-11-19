#!/usr/bin/env python3

from datetime import datetime
from hdf5libs import HDF5RawDataFile

import click
import time

@click.command()
@click.argument('filename', type=click.Path(exists=True))

def main(filename):

    h5_file = HDF5RawDataFile(filename)
    file_layout_version = h5_file.get_version()
    #print(f'file_layout_verion {file_layout_version}')

    attr_name = "creation_timestamp"
    if file_layout_version >= 6:
        attr_value = h5_file.get_int_attribute(attr_name)
        print(f'{attr_name} {attr_value}')
    else:
        attr_value = h5_file.get_attribute(attr_name)
        print(f'{attr_name} {attr_value}')
    
    attr_name = "closing_timestamp"
    if file_layout_version >= 6:
        attr_value = h5_file.get_int_attribute(attr_name)
        print(f'{attr_name} {attr_value}')
    else:
        attr_value = h5_file.get_attribute(attr_name)
        print(f'{attr_name} {attr_value}')

    attr_name = "offline_data_stream"
    try:
        attr_value = h5_file.get_attribute(attr_name)
    except RuntimeError:
        attr_value = "cosmics"
    print(f'{attr_name} {attr_value}')

    attr_name = "run_was_for_test_purposes"
    try:
        attr_value = h5_file.get_attribute(attr_name)
    except RuntimeError:
        attr_value = "false"
    print(f'{attr_name} {attr_value}')

    attr_name = "file_recovery_timestamp"
    try:
        attr_value = h5_file.get_int_attribute(attr_name)
        print(f'{attr_name} {attr_value}')
    except RuntimeError:
        pass

    records = h5_file.get_all_record_ids()

    print('=== start of record list')
    for r in records:
        print(f'{r[0]}.{r[1]}')
    print('=== end of record list')

if __name__ == '__main__':
    main()
