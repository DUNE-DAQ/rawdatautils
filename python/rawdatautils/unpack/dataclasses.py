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

    def __str__(self):
        return f"{self.__class__.__name__}({', '.join(f'{name}={getattr(self, name)}' for name in self.index_names())})"

@dataclass(order=True)
class SourceIDData(RecordDataBase):
    src_id: int
    subsystem: int
    subsystem_str: str
    version: int
    
    def __str__(self):
        base_str = super().__str__()

        additional_fields = [f"src_id={self.src_id}",
                             f"subsystem={self.subsystem} ('{self.subsystem_str}')",
                             f"version={self.version}"]
        return f"{base_str}: [{', '.join(additional_fields)}]"

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
    status_bits: int
    trigger_type: int
    max_sequence_number: int
    total_size_bytes: int
    trigger_time : datetime = field(init=False)
    trigger_type_bits: list[int] = field(init=False)

    def __post_init__(self):
        self.trigger_time = dts_to_datetime(self.trigger_timestamp_dts)
        self.trigger_type_bits = [ trgdataformats.TriggerCandidateData.Type(i) for i in range(64) if (self.trigger_type & (1<<i))!=0 ]
    
    def __str__(self):
        base_str = super().__str__()

        additional_fields = [f"trigger_timestamp={self.trigger_timestamp_dts} ({self.trigger_time})", 
                             f"trigger_type={self.trigger_type} ({self.trigger_type_bits})",
                             f"n_fragments={self.n_fragments}",
                             f"n_requested_components={self.n_requested_components}",
                             f"max_sequence_number={self.max_sequence_number}",
                             f"total_size_bytes={self.total_size_bytes}",
                             f"status_bits={self.status_bits}"]
        return f"{base_str}: [{', '.join(additional_fields)}]"
    

@dataclass(order=True)
class FragmentHeaderData(FragmentDataBase):

    trigger_timestamp_dts: int
    window_begin_dts: int
    window_end_dts: int
    det_id: int
    status_bits: int
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

    def __str__(self):
        base_str = super().__str__()

        fr_type = daqdataformats.FragmentType(self.fragment_type)
        subdet = detdataformats.DetID.subdetector_to_string(detdataformats.DetID.Subdetector(self.det_id))
        additional_fields = [f"trigger_timestamp={self.trigger_timestamp_dts}",
                             f"window [begin,end)=[{self.window_begin_dts},{self.window_end_dts})",
                             f"det_id={self.det_id} ('{subdet}')",
                             f"fragment_type={self.fragment_type} ('{daqdataformats.fragment_type_to_string(fr_type)}')",
                             f"total_size_bytes={self.total_size_bytes}",
                             f"data_size_bytes={self.data_size_bytes}",
                             f"status_bits={self.status_bits}"]
        return f"{base_str}: [{', '.join(additional_fields)}]"


@dataclass(order=True)
class TriggerHeaderData(FragmentDataBase):

    n_obj: int
    version: int
    
@dataclass(order=True)
class TriggerPrimitiveData(FragmentDataBase):

    time_start: int
    samples_to_peak: int
    samples_over_threshold: int
    channel: int
    plane: int
    element: int
    adc_integral: int
    adc_peak: int
    detid: int
    flag: int
    id_ta: int

    def __str__(self):
        base_str = super().__str__()
        subdet = detdataformats.DetID.subdetector_to_string(detdataformats.DetID.Subdetector(self.detid))

        additional_fields = [f"channel={self.channel}",
                             f"(plane,element)=({self.plane},{self.element})",
                             f"time_start={self.time_start}",
                             f"samples_to_peak={self.samples_to_peak}",
                             f"samples_over_threshold={self.samples_over_threshold}",
                             f"adc_integral={self.adc_integral}",
                             f"adc_peak={self.adc_peak}",
                             f"detid={self.detid} ('{subdet}')",
                             f"flag={self.flag}",
                             f"id_ta={self.id_ta}"]
        return f"{base_str}: [{', '.join(additional_fields)}]"

@dataclass(order=True)
class TriggerActivityData(FragmentDataBase):

    time_start: int
    time_end: int
    time_peak: int
    time_activity: int
    channel_start: int
    channel_end: int
    channel_peak: int
    plane: int
    element: int
    adc_integral: int
    adc_peak: int
    detid: int
    ta_type: int
    algorithm: int
    n_tps: int
    id: int
    id_tc: int

    def __str__(self):
        base_str = super().__str__()
        subdet = detdataformats.DetID.subdetector_to_string(detdataformats.DetID.Subdetector(self.detid))
        tatype = trgdataformats.TriggerActivityData.Type(self.ta_type)
        taalg = trgdataformats.TriggerActivityData.Algorithm(self.algorithm)

        additional_fields = [f"id={self.id}",
                             f"channel (start,peak,end)=({self.channel_start},{self.channel_peak},{self.channel_end})",
                             f"(plane,element)=({self.plane},{self.element})",
                             f"time_activity={self.time_activity}",
                             f"time (start,peak,end)=({self.time_start},{self.time_peak},{self.time_end})",
                             f"adc_integral={self.adc_integral}",
                             f"adc_peak={self.adc_peak}",
                             f"detid={self.detid} ('{subdet}')",
                             f"ta_type={self.ta_type} ('{tatype})",
                             f"algorithm={self.algorithm} ('{taalg})",
                             f"n_tps={self.n_tps}",
                             f"id_tc={self.id_tc}"]
        return f"{base_str}: [{', '.join(additional_fields)}]"

@dataclass(order=True)
class TriggerCandidateData(FragmentDataBase):

    time_start: int
    time_end: int
    time_candidate: int
    detid: int
    tc_type: int
    algorithm: int
    n_tas: int
    id: int

    def __str__(self):
        base_str = super().__str__()
        subdet = detdataformats.DetID.subdetector_to_string(detdataformats.DetID.Subdetector(self.detid))
        tctype = trgdataformats.TriggerCandidateData.Type(self.tc_type)
        tcalg = trgdataformats.TriggerCandidateData.Algorithm(self.algorithm)

        additional_fields = [f"id={self.id}",
                             f"time_candidate={self.time_candidate}",
                             f"time (start,end)=({self.time_start},{self.time_end})",
                             f"detid={self.detid} ('{subdet}')",
                             f"tc_type={self.ta_type} ('{tctype})",
                             f"algorithm={self.algorithm} ('{tcalg})",
                             f"n_tas={self.n_tas}"]
        return f"{base_str}: [{', '.join(additional_fields)}]"

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

    def __str__(self):
        base_str = super().__str__()
        subdet = detdataformats.DetID.subdetector_to_string(detdataformats.DetID.Subdetector(self.det_id))
        additional_fields = [f"n_obj={self.n_obj}",
                             f"first_timestamp={self.timestamp_first_dts}",
                             f"det_id={self.det_id} ('{subdet}')",
                             f"(crate_id,slot_id,stream_id)=({self.crate_id},{self.slot_id},{self.stream_id})",
                             f"daq_header_version={self.daq_header_version}",
                             f"det_data_version={self.det_data_version}"]
        return f"{base_str}: [{', '.join(additional_fields)}]"

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

    def __str__(self):
        base_str = super().__str__()
        additional_fields = [f"n_frames={self.n_frames}",
                             f"n_channels={self.n_channels}",
                             f"sampling_period={self.sampling_period}",
                             f"femb_id={self.femb_id}",
                             f"coldata_id={self.colddata_id}",
                             f"version={self.version}",
                             f"first_timestamp={self.timestamp_dts_first}"]
        additional_field_names = ["timestamp_dts_diff",
                                  "colddata_timestamp_0_diff","colddata_timestamp_1_diff",
                                  "cd","crc_err","link_valid","lol","wib_sync","femb_sync",
                                  "pulser","calibration","ready","context"]
        for name in additional_field_names:
            vals_name = f'{name}_vals'
            idx_name = f'{name}_idx'
            additional_fields.append(f"{name}={getattr(self,vals_name)} (idx={getattr(self,idx_name)})")                         
        return f"{base_str}: [{', '.join(additional_fields)}]"

@dataclass(order=True)
class WIBEthChannelDataBase(FragmentDataBase):
    
    channel: int
    plane: int
    element: int
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

    def __str__(self):
        base_str = super().__str__()
        additional_fields = [f"adc_mean={self.adc_mean}",
                             f"adc_rms={self.adc_rms}",
                             f"adc_max={self.adc_max}",
                             f"adc_min={self.adc_min}",
                             f"adc_median={self.adc_median}"]
        return f"{base_str}: [{', '.join(additional_fields)}]"

@dataclass(order=True)
class WIBEthWaveformData(WIBEthChannelDataBase):

    timestamps: np.ndarray
    adcs: np.ndarray
    fft_mag: np.ndarray

    def __str__(self):
        base_str = super().__str__()
        additional_fields = [f"timestamps={self.timestamps}",
                             f"adcs={self.adcs}",
                             f"fft_mag={self.fft_mag}"]
        return f"{base_str}: [{', '.join(additional_fields)}]"

@dataclass(order=True)
class TDEEthHeaderData(FragmentDataBase):

    #first frame only
    channel_id: int
    tde_header: int
    version: int

    #_idx arrays contain indices where value has changed from previous
    #_vals arrays contain the values at those indices
    errors_vals: np.ndarray
    errors_idx: np.ndarray

    #these take differences between successive values,
    #and then, as above, look for differences in those differences
    #store first value so the full array can be reconstructed
    timestamp_dts_diff_vals: np.ndarray[int, np.float128]
    timestamp_dts_diff_idx: np.ndarray
    timestamp_dts_first: int

    tai_time_diff_vals: np.ndarray[int, np.float128]
    tai_time_diff_idx: np.ndarray
    tai_time_first: int

    n_frames: int
    n_channels: int
    sampling_period: int

    def __str__(self):
        base_str = super().__str__()
        additional_fields = [f"n_frames={self.n_frames}",
                             f"n_channels={self.n_channels}",
                             f"sampling_period={self.sampling_period}",
                             f"channel_id={self.channel_id}",
                             f"tde_header={self.tde_header}",
                             f"version={self.version}",
                             f"first_timestamp={self.timestamp_first_dts}",
                             f"tai_time_first={self.tai_time_first}"]
        additional_field_names = ["timestamp_dts_diff","tai_time_diff","errors"]
        for name in additional_field_names:
            vals_name = f'{name}_vals'
            idx_name = f'{name}_idx'
            additional_fields.append(f"{name}={getattr(self,vals_name)} (idx={getattr(self,idx_name)})")                         
        return f"{base_str}: [{', '.join(additional_fields)}]"

@dataclass(order=True)
class TDEEthChannelDataBase(FragmentDataBase):

    channel: int
    plane: int
    element: int
    tde_chan: int

    @classmethod
    def index_names(cls):
        return [ "run","trigger","sequence","src_id","channel" ]

    def index_values(self):
        return [ self.run, self.trigger, self.sequence, self.src_id, self.channel ]

@dataclass(order=True)
class TDEEthAnalysisData(TDEEthChannelDataBase):

    adc_mean: float
    adc_rms: float
    adc_max: int
    adc_min: int
    adc_median: float

    def __str__(self):
        base_str = super().__str__()
        additional_fields = [f"adc_mean={self.adc_mean}",
                             f"adc_rms={self.adc_rms}",
                             f"adc_max={self.adc_max}",
                             f"adc_min={self.adc_min}",
                             f"adc_median={self.adc_median}"]
        return f"{base_str}: [{', '.join(additional_fields)}]"

@dataclass(order=True)
class TDEEthWaveformData(TDEEthChannelDataBase):

    timestamps: np.ndarray[int, np.float128]
    adcs: np.ndarray
    fft_mag: np.ndarray

    def __str__(self):
        base_str = super().__str__()
        additional_fields = [f"timestamps={self.timestamps}",
                             f"adcs={self.adcs}",
                             f"fft_mag={self.fft_mag}"]
        return f"{base_str}: [{', '.join(additional_fields)}]"

@dataclass(order=True)
class DAPHNEStreamHeaderData(FragmentDataBase):

    n_channels: int
    sampling_period: int
    ts_diffs_vals: np.ndarray
    ts_diffs_counts: np.ndarray

    def __str__(self):
        base_str = super().__str__()
        additional_fields = [f"n_channels={self.n_channels}",
                             f"sampling_period={self.sampling_period}",
                             f"ts_diffs_vals={self.ts_diffs_vals} (counts={self.ts_diffs_counts})"]
        return f"{base_str}: [{', '.join(additional_fields)}]"

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

    def __str__(self):
        base_str = super().__str__()
        additional_fields = [f"adc_mean={self.adc_mean}",
                             f"adc_rms={self.adc_rms}",
                             f"adc_max={self.adc_max}",
                             f"adc_min={self.adc_min}",
                             f"adc_median={self.adc_median}"]
        return f"{base_str}: [{', '.join(additional_fields)}]"

@dataclass(order=True)
class DAPHNEStreamWaveformData(DAPHNEChannelDataBase):

    timestamps: np.ndarray
    adcs: np.ndarray
    fft_mag: np.ndarray

    def __str__(self):
        base_str = super().__str__()
        additional_fields = [f"timestamps={self.timestamps}",
                             f"adcs={self.adcs}",
                             f"fft_mag={self.fft_mag}"]
        return f"{base_str}: [{', '.join(additional_fields)}]"
    
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

    def __str__(self):
        base_str = super().__str__()
        additional_fields = [f"timestamp_dts={self.timestamp_dts}",
                             f"trigger_sample_value={self.trigger_sample_value}",
                             f"baseline={self.baseline}",
                             f"threshold={self.threshold}",
                             f"adc_mean={self.adc_mean}",
                             f"adc_rms={self.adc_rms}",
                             f"adc_max={self.adc_max} (timestamp={self.timestamp_max_dts})",
                             f"adc_min={self.adc_min} (timestamp={self.timestamp_min_dts})",
                             f"adc_median={self.adc_median}"]
        return f"{base_str}: [{', '.join(additional_fields)}]"

@dataclass(order=True)
class DAPHNEWaveformData(DAPHNEChannelDataBase):

    timestamp_dts: int
    timestamps: np.ndarray
    adcs: np.ndarray

    def __str__(self):
        base_str = super().__str__()
        additional_fields = [f"timestamp_dts={self.timestamp_dts}",
                             f"timestamps={self.timestamps}",
                             f"adcs={self.adcs}"]
        return f"{base_str}: [{', '.join(additional_fields)}]"

@dataclass(order=True)
class DAPHNEEthAnalysisData(DAPHNEAnalysisData):

    peak_found:                 np.ndarray  # shape (s_max_peaks,), bool
    peak_adc_integral:          np.ndarray  # shape (s_max_peaks,), uint32
    peak_adc_max:               np.ndarray  # shape (s_max_peaks,), uint16
    peak_sample_max:            np.ndarray  # shape (s_max_peaks,), uint16
    peak_samples_over_baseline: np.ndarray  # shape (s_max_peaks,), uint16
    peak_sample_start:          np.ndarray  # shape (s_max_peaks,), uint16
    peak_num_subpeaks:          np.ndarray  # shape (s_max_peaks,), uint8

    def __str__(self):
        base_str = super().__str__()
        peak_strs = []
        for i_p in range(len(self.peak_found)):
            if self.peak_found[i_p]:
                peak_strs.append(
                    f"peak[{i_p}](integral={self.peak_adc_integral[i_p]}, "
                    f"adc_max={self.peak_adc_max[i_p]}, "
                    f"sample_start={self.peak_sample_start[i_p]}, "
                    f"sample_max={self.peak_sample_max[i_p]}, "
                    f"t_over_baseline={self.peak_samples_over_baseline[i_p]}, "
                    f"n_subpeaks={self.peak_num_subpeaks[i_p]})"
                )
        peaks_str = f"peaks=[{', '.join(peak_strs)}]" if peak_strs else "peaks=[]"
        return f"{base_str}, {peaks_str}"
