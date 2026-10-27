#!/usr/bin/env python3

from datetime import datetime
import rawdatafilelibs

import click
import sys
import time

@click.command()
@click.argument('filename', type=click.Path(exists=True))

def main(filename):

    h5_file = rawdatafilelibs.open(filename)
    metadata = h5_file.metadata

    # The timestamps are in ms, whether the file stores them as integers (layout version >= 6) or as strings.
    # 0 means the file doesn't have the value; fail then, as the earlier version did, so the file isn't used.
    for attr_name, attr_value in (("creation_timestamp", metadata.creation_timestamp_ms),
                                  ("closing_timestamp", metadata.closing_timestamp_ms)):
        if attr_value == 0:
            sys.exit(f'{filename} has no {attr_name}')
        print(f'{attr_name} {attr_value}')

    # Defaults for files written without these values
    attr_value = metadata.offline_data_stream if metadata.offline_data_stream != "" else "cosmics"
    print(f'offline_data_stream {attr_value}')

    attr_value = "true" if metadata.run_is_for_test_purposes else "false"
    print(f'run_was_for_test_purposes {attr_value}')

    # Only files recovered after being left unfinished have this value (0 = never recovered)
    if metadata.recovery_timestamp_ms != 0:
        print(f'file_recovery_timestamp {metadata.recovery_timestamp_ms}')

    records = h5_file.get_all_record_ids()

    print('=== start of record list')
    for r in records:
        print(f'{r[0]}.{r[1]}')
    print('=== end of record list')

if __name__ == '__main__':
    main()
