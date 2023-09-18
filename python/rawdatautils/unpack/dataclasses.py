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

def dts_to_datetime(dts_timestamp):
    return datetime.fromtimestamp(dts_timestamp*16 // 1e9, tz=pytz.timezone("UTC"))

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

    femb_id: int
    coldata_id: int
    version: int
    pulser: int
    calibration: int
    context: int
    n_channels: int
    sampling_period: int
    ts_diffs_vals: np.ndarray
    ts_diffs_counts: np.ndarray

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
