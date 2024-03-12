#!/usr/bin/env python3

from datetime import datetime
from hdf5libs import HDF5RawDataFile

import click
import time

@click.command()
@click.argument('filename', type=click.Path(exists=True))

def main(filename):

    h5_file = HDF5RawDataFile(filename)

    attr_name = "creation_timestamp"
    value_string = h5_file.get_attribute(attr_name)
    attr_value = datetime.utcfromtimestamp(float(value_string)/1000.0).isoformat()
    print(f'{attr_name} {attr_value}')
    
    attr_name = "closing_timestamp"
    value_string = h5_file.get_attribute(attr_name)
    attr_value = datetime.utcfromtimestamp(float(value_string)/1000.0).isoformat()
    print(f'{attr_name} {attr_value}')

    attr_name = "offline_data_stream"
    try:
        attr_value = h5_file.get_attribute(attr_name)
    except RuntimeError:
        attr_value = "cosmics"
    print(f'{attr_name} {attr_value}')

    attr_name = "run_is_for_test_purposes"
    try:
        attr_value = h5_file.get_attribute(attr_name)
    except RuntimeError:
        attr_value = "true"
    print(f'{attr_name} {attr_value}')

    records = h5_file.get_all_record_ids()

    print('=== start of record list')
    for r in records:
        print(f'{r[0]}.{r[1]}')
    print('=== end of record list')

if __name__ == '__main__':
    main()
