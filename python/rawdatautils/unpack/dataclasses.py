from dataclasses import dataclass, field
import typing
from datetime import datetime
import pytz
import numpy as np

import daqdataformats
import detdataformats
import fddetdataformats
import trgdataformats
import detchannelmaps

def dts_to_seconds(dts):
     return dts*16 //1e9

def dts_to_datetime(dts_timestamp):
    return datetime.fromtimestamp(dts_to_seconds(dts_timestamp), tz=pytz.timezone("UTC"))

## Sparsification and desparsifications for arrays

def sparsify_array_diff_locs_and_vals(arr):
    # Find indices where the value changes compared to the previous value, adjusting the first index to start from 0
    change_locations = np.insert(np.where(arr[1:] != arr[:-1])[0], 0, -1) + 1
    # Check if the array is not empty
    if len(arr) > 0:
        # Return locations of changes, values at these locations, and the length of the array
        return change_locations, arr[change_locations], len(arr)
    else:
        # Return empty results for an empty array
        return [], [], 0

def desparsify_array_diff_locs_and_vals(change_locations, change_values, arr_size):
    # Create an empty array of the original size
    reconstructed_arr = np.empty(arr_size, dtype=np.uint)
    # Loop through each change location
    for i in range(len(change_locations)):
        # Apply the change value from the current change location to the end or next change location
        if (i + 1) == len(change_locations):
            reconstructed_arr[change_locations[i]:] = change_values[i]
        else:
            reconstructed_arr[change_locations[i]:change_locations[i + 1]] = change_values[i]
    return reconstructed_arr

def sparsify_array_diff_of_diff_locs_and_vals(arr):
    # Store the first value of the array for later reconstruction
    arr_first = arr[0]
    # Compute the difference of consecutive elements
    arr_diff = np.diff(arr)
    # Use sparsify function to find locations and values of changes in the diff array
    arr_diff_locs, arr_diff_vals, _ = sparsify_array_diff_locs_and_vals(arr_diff)
    # Return the first value, change locations, change values, and array size for reconstruction
    return arr_first, arr_diff_locs, arr_diff_vals, len(arr)

def desparsify_array_diff_of_diff_locs_and_vals(arr_first, change_locations, change_values, arr_size):
    # Reconstruct the differential array from sparse representation
    arr_diff = desparsify_array_diff_locs_and_vals(change_locations, change_values, arr_size - 1)
    # Reconstruct the original array by cumulatively summing the differences and adding the first value
    arr = np.concatenate((np.array([0], dtype=np.uint), arr_diff)).cumsum() + arr_first
    return arr

@dataclass(order=True)
class RecordDataBase():
    run: int
    trigger: int
    sequence: int

    @classmethod
    def index_names(cls):
        return [ "run","trigger","sequence" ]

    def index_values(self):
        return [ self.run, self.trigger, self.sequence ]

@dataclass(order=True)
class SourceIDData(RecordDataBase):
    src_id: int
    subsystem: int
    subsystem_str: str
    version: int
    
@dataclass(order=True)
class FragmentDataBase(RecordDataBase):
    src_id: int

    @classmethod
    def index_names(cls):
        return [ "run","trigger","sequence","src_id" ]

    def index_values(self):
        return [ self.run, self.trigger, self.sequence, self.src_id ]
    

@dataclass(order=True)
class TriggerRecordData(RecordDataBase):

    trigger_timestamp_dts: int
    n_fragments: int
    n_requested_components: int
    error_bits: int
    trigger_type: int
    max_sequence_number: int
    total_size_bytes: int
    trigger_time : datetime = field(init=False)

    def __post_init__(self):
        self.trigger_time = dts_to_datetime(self.trigger_timestamp_dts)

    

@dataclass(order=True)
class FragmentHeaderData(FragmentDataBase):

    trigger_timestamp_dts: int
    window_begin_dts: int
    window_end_dts: int
    det_id: int
    error_bits: int
    fragment_type: int
    total_size_bytes: int
    data_size_bytes: int
    trigger_time : datetime = field(init=False)
    window_begin_time : datetime = field(init=False)
    window_end_time : datetime = field(init=False)

    def __post_init__(self):
        self.trigger_time = dts_to_datetime(self.trigger_timestamp_dts)
        self.window_begin_time = dts_to_datetime(self.window_begin_dts)
        self.window_end_time = dts_to_datetime(self.window_end_dts)

@dataclass(order=True)
class TriggerHeaderData(FragmentDataBase):

    n_obj: int
    version: int
    
@dataclass(order=True)
class TriggerPrimitiveData(FragmentDataBase):

    time_start: int
    time_peak: int
    time_over_threshold: int
    channel: int
    adc_integral: int
    adc_peak: int
    detid: int
    tp_type: int
    algorithm: int
    flag: int

@dataclass(order=True)
class DAQHeaderData(FragmentDataBase):

    n_obj: int
    daq_header_version: int
    det_data_version: int
    det_id: int
    crate_id: int
    slot_id: int
    stream_id: int
    timestamp_first_dts: int
    timestamp_first_time: datetime = field(init=False)

    def __post_init__(self):
        self.timestamp_first_time = dts_to_datetime(self.timestamp_first_dts)

@dataclass(order=True)
class WIBEthHeaderData(FragmentDataBase):

    #first frame only
    femb_id: int
    colddata_id: int
    version: int

    #_idx arrays contain indices where value has changed from previous
    #_vals arrays contain the values at those indices
    pulser_vals: np.ndarray
    pulser_idx: np.ndarray    
    calibration_vals: np.ndarray
    calibration_idx: np.ndarray
    ready_vals: np.ndarray
    ready_idx: np.ndarray
    context_vals: np.ndarray
    context_idx: np.ndarray

    wib_sync_vals: np.ndarray
    wib_sync_idx: np.ndarray
    femb_sync_vals: np.ndarray
    femb_sync_idx: np.ndarray

    cd_vals: np.ndarray
    cd_idx: np.ndarray
    crc_err_vals: np.ndarray
    crc_err_idx: np.ndarray
    link_valid_vals: np.ndarray
    link_valid_idx: np.ndarray
    lol_vals: np.ndarray
    lol_idx: np.ndarray

    #these take differences between successive values,
    #and then, as above, look for differences in those differences
    #store first value so the full array can be reconstructed
    colddata_timestamp_0_diff_vals: np.ndarray
    colddata_timestamp_0_diff_idx: np.ndarray
    colddata_timestamp_0_first: int

    colddata_timestamp_1_diff_vals: np.ndarray
    colddata_timestamp_1_diff_idx: np.ndarray
    colddata_timestamp_1_first: int

    timestamp_dts_diff_vals: np.ndarray
    timestamp_dts_diff_idx: np.ndarray
    timestamp_dts_first: int
    
    n_frames: int
    n_channels: int
    sampling_period: int

@dataclass(order=True)
class WIBEthChannelDataBase(FragmentDataBase):
    
    channel: int
    plane: int
    apa: str
    wib_chan: int
    
    @classmethod
    def index_names(cls):
        return [ "run","trigger","sequence","src_id","channel" ]

    def index_values(self):
        return [ self.run, self.trigger, self.sequence, self.src_id, self.channel ]

@dataclass(order=True)
class WIBEthAnalysisData(WIBEthChannelDataBase):
    
    adc_mean: float
    adc_rms: float
    adc_max: int
    adc_min: int
    adc_median: float

@dataclass(order=True)
class WIBEthWaveformData(WIBEthChannelDataBase):

    timestamps: np.ndarray
    adcs: np.ndarray
    fft_mag: np.ndarray

@dataclass(order=True)
class DAPHNEStreamHeaderData(FragmentDataBase):

    n_channels: int
    sampling_period: int
    ts_diffs_vals: np.ndarray
    ts_diffs_counts: np.ndarray

@dataclass(order=True)
class DAPHNEChannelDataBase(FragmentDataBase):
    
    channel: int
    daphne_chan: int
    
    @classmethod
    def index_names(cls):
        return [ "run","trigger","sequence","src_id","channel" ]

    def index_values(self):
        return [ self.run, self.trigger, self.sequence, self.src_id, self.channel ]


@dataclass(order=True)
class DAPHNEStreamAnalysisData(DAPHNEChannelDataBase):

    adc_mean: float
    adc_rms: float
    adc_max: int
    adc_min: int
    adc_median: float
    
@dataclass(order=True)
class DAPHNEStreamWaveformData(DAPHNEChannelDataBase):

    timestamps: np.ndarray
    adcs: np.ndarray
    fft_mag: np.ndarray


@dataclass(order=True)
class DAPHNEAnalysisData(DAPHNEChannelDataBase):

    timestamp_dts: int
    trigger_sample_value: int
    threshold: float
    baseline: float
    adc_mean: float
    adc_rms: float
    adc_max: int
    adc_min: int
    adc_median: float
    timestamp_max_dts: int
    timestamp_min_dts: int
    
@dataclass(order=True)
class DAPHNEWaveformData(DAPHNEChannelDataBase):

    timestamp_dts: int
    timestamps: np.ndarray
    adcs: np.ndarray
