# rawdatautils

## WIB2	Utilities

### `wib2decoder.py`

```
Usage: wib2decoder.py [OPTIONS] FILENAME

Options:
  -n, --nrecords INTEGER  How many Trigger Records to process (default: all)
  --nskip INTEGER         How many Trigger Records to skip (default: 0)
  --channel-map TEXT      Channel map to load (default: None)
  --print-headers         Print WIB2Frame headers
  --print-adc-stats       Print ADC Pedestals/RMS
  --check-timestamps      Check WIB2 Frame Timestamps
  --help                  Show this message and exit.
```

For example, for HD coldbox data from v2.11 onward, you can do:
```
wib2decoder.py -n 1 --print-headers --print-adc-stats --check-timestamps --channel-map HDColdboxChannelMap <file_name>
```
to dump content of the headers, do some timestamp checks for the frames coming from the same WIB and across different WIBs, and to do some basic processing of the data.

## Unpack utilities for Python

There are fast unpackers of data for working in python. These unpackers will
take a Fragment and put the resulting values (ADCs or timestamps) in a numpy
array with shape `(number of frames, number of channels)` at a similar speed
compared to doing that in C++. This is much faster than doing a similar thing
frame by frame in a loop in python.

To use it import the functions first:
```
from rawdatautils.unpack.<format> import *
```
where `<format>` is one of the supported formats: `wib`, `wib2` or `daphne` for
the corresponding `WIBFrame`, `WIB2Frame` and `DAPHNEFrame` frame formats. Then
there are several functions available:
```
# assumming frag is a fragment
adc = np_array_adc(frag)
timestamp = np.array_timestamp(frag)
print(adc.shape)       # (number of frames in frag, 256 if using wib2)
print(timestamp.shape) # (number of frames in frag, 256 if using wib2)
```
`np_array_adc` and `np_array_timestamp` will unpack the whole fragment. It is also possible to unpack only a part of it:
```
# assumming frag is a fragment
adc = np_array_adc_data(frag.get_data(), 100)             # first 100 frames
timestamp = np_array_timestamp_data(frag.get_data(), 100) # first 100 frames
print(adc.shape)       # (100, 256 if using wib2)
print(timestamp.shape) # (100, 256 if using wib2)
```
Warning: `np_array_adc_data` and `np_array_timestamp_data` do not make any
checks on the number of frames so if passed a value larger than the actual
number of fragments it will try to read out of bounds. `np_array_adc` and
`np_array_timestamp` call `np_array_adc_data` and `np_array_timestamp_data`
under the hood with the correct checks on the number of frames.

