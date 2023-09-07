#general imports
import sys

#dunedaq imports
import daqdataformats
import detdataformats
import fddetdataformats
import trgdataformats
import detchannelmaps

#unpacker imports
import rawdatautils.unpack.wibeth
import rawdatautils.unpack.daphne
import h5py

#analysis imports
import numpy as np
import numpy.fft

def CreateDFIndexDict(run_number,rid):
    '''
    Create the standard dataframe index dictionary from run and record ID

    Parameters:
        run_number (int): run number
        rid (tuple): record ID tuple of (record number, sequence number)

    Returns:
        df_dict_idx (dict)

    '''

    DF_IDX_COLS = ["run_idx","record_idx","sequence_idx"]
    df_dict_idx = dict.fromkeys(DF_IDX_COLS)

    df_dict_idx["run_idx"] = run_number
    df_dict_idx["record_idx"] = rid[0]
    df_dict_idx["sequence_idx"] = rid[1]

    return df_dict_idx


def CreateTriggerRecordHeaderDict(trh,df_dict_idx):
    '''
    Create TriggerRecord information dictionary

    Parameters:
        trh (daqdataformats.TriggerRecordHeader)
        df_dict_idx (dict): standard dataframe index dictionary (from CreateDFIndexDict)

    Returns
        dict_trh (dict): dictionary including trigger record header information
        n_rows (int=1): number of objects per dictionary entry, always 1
        dict_idx_keys (list): list of dictionary keys to use as index in dataframe
    '''
    
    DF_TRH_COLS = [
        "run_number",
        "trigger_number",
        "sequence_number",
        "trigger_timestamp",
        "n_fragments","n_requested_components",
        "error_bits","trigger_type"
        "max_sequence_number",
        "total_size_bytes",
        "trigger_record_header_marker",
        "trigger_record_header_version"]

    dict_trh = dict.fromkeys(DF_TRH_COLS)

    dict_trh["run_number"] = trh.get_run_number()
    dict_trh["trigger_number"] = trh.get_trigger_number()
    dict_trh["sequence_number"] = trh.get_sequence_number()
    dict_trh["max_sequence_number"] = trh.get_max_sequence_number()
    dict_trh["trigger_timestamp"] = trh.get_trigger_timestamp()
    dict_trh["trigger_type"] = trh.get_trigger_type()
    
    #dict_trh["n_fragments"] #must be done outside of this
    dict_trh["n_requested_components"] = trh.get_num_requested_components()
    dict_trh["error_bits"] = trh.get_header().error_bits
    dict_trh["total_size_bytes"] = trh.get_total_size_bytes()

    dict_trh["trigger_record_header_marker"] = trh.get_header().trigger_record_header_marker
    dict_trh["trigger_record_header_version"] = trh.get_header().version
    
    return (dict_trh | df_dict_idx), 1, list(df_dict_idx.keys())


def CreateSourceIDDict(sid,dict_idx):
    DF_SID_COLS = ["id","subsystem","subsystem_str"]
    dict_sid = dict.fromkeys(DF_SID_COLS)
    dict_sid["id"]=int(sid.id)
    dict_sid["subsystem"]=int(sid.subsystem)
    dict_sid["subsystem_str"]=daqdataformats.SourceID.subsystem_to_string(sid.subsystem)
    return (dict_sid | dict_idx), 1, list(dict_idx.keys())

def CreateFragmentHeaderDict(frh,dict_idx):
    DF_FRH_COLS = [
        "run_number",
        "trigger_number",
        "sequence_number",
        "trigger_timestamp",
        "window_begin",
        "window_end",
        "src_id",
        "det_id",
        "error_bits",
        "fragment_type",
        "fragment_type_str",
        "size",
        "data_size"
    ]
    dict_frh = dict.fromkeys(DF_FRH_COLS)

    dict_frh["run_number"] = frh.run_number
    dict_frh["trigger_number"] = frh.trigger_number
    dict_frh["sequence_number"] = frh.sequence_number
    dict_frh["trigger_timestamp"] = frh.trigger_timestamp
    dict_frh["window_begin"] = frh.window_begin
    dict_frh["window_end"] = frh.window_end
    dict_frh["src_id"] = frh.element_id.id
    dict_frh["det_id"] = frh.detector_id
    dict_frh["error_bits"] = frh.error_bits
    dict_frh["fragment_type"] = frh.fragment_type
    dict_frh["fragment_type_str"] = str(frh.fragment_type)
    dict_frh["size"] = frh.size
    #dict_frh["data_size"] #get outside of this func

    return (dict_frh | dict_idx), 1, list(dict_idx.keys())

class FragmentUnpacker:
    
    is_detector_unpacker = False
    is_trigger_unpacker = False

    def __init__(self,dict_idx):
        self.dict_idx = dict_idx

    def get_n_obj(self,frag):
        return None

class TriggerDataUnpacker(FragmentUnpacker):
    
    is_trigger_unpacker = True

    DICT_TRGH_COLS = [
        "n_obj",
        "version"        
    ]
    trg_header_dict_name = "trgh"
    
    def __init__(self,dict_idx):
        super().__init__(dict_idx)

    def get_trg_data_version(self,frag):
        return None

    def get_trg_data_dict(self,frag):
        return None,None

    def get_trg_header_dict(self,frag):
        dict_trgh = dict.fromkeys(self.DICT_TRGH_COLS)
        dict_trgh["version"] = self.get_trg_data_version(frag)
        dict_trgh["n_obj"] = self.get_n_obj(frag)
        return (dict_trgh | self.dict_idx), 1, list(self.dict_idx.keys())


class TriggerPrimitiveUnpacker(TriggerDataUnpacker):

    trg_obj = trgdataformats.TriggerPrimitive
    
    DICT_TP_COLS = [
        "time_start",
        "time_peak",
        "time_over_threshold",
        "channel",
        "adc_integral",
        "adc_peak",
        "detid",
        "type",
        "algorithm",
        "flag"
    ]
    
    def __init__(self,dict_idx):
        super().__init__(dict_idx)

    def get_n_obj(self,frag):
        return frag.get_data_size()/trg_obj.sizeof()
    
    def get_trg_data_dict(self,frag):
        dict_trgd = dict.fromkeys(self.DICT_TP_COLS)

        for i in range(self.n_obj(frag)):
            tp = trg_obj(frag.get_data(i*trg_obj.sizeof()))
        
            dict_trgd["time_start"] = tp.time_start
            dict_trgd["time_peak"] = tp.time_peak
            dict_trgd["time_over_threshold"] = tp.time_over_threshold
            dict_trgd["channel"] = tp.channel
            dict_trgd["adc_integral"] = tp.adc_integral
            dict_trgd["adc_peak"] = tp.adc_peak
            dict_trgd["detid"] = tp.detid
            dict_trgd["type"] = tp.type
            dict_trgd["algorithm"] = tp.algorithm
            dict_trgd["flag"] = tp.flag
            
            return (dict_trgd | self.dict_idx), self.n_obj(frag), list(self.dict_idx.keys())+["channel"]


class DetectorFragmentUnpacker(FragmentUnpacker):

    DICT_DAQH_COLS = [
        "n_obj",
        "daq_header_version",
        "det_data_version",
        "det_id",
        "crate_id",
        "slot_id",
        "stream_id",
        "timestamp_first"
    ]
    daq_header_dict_name = "daqh"
    
    is_detector_unpacker = True

    def __init__(self,dict_idx):
        super().__init__(dict_idx)

    def get_daq_header_version(self,frag):
        return None
    
    def get_det_data_version(self,frag):
        return None

    def get_timestamp_first(self,frag):
        return None
    
    def get_det_crate_slot_stream(self,frag):
        return None, None, None, None

    def get_daq_header_dict(self,frag):
        dict_datah = dict.fromkeys(self.DICT_DAQH_COLS)
        dict_datah["n_obj"] = self.get_n_obj(frag)
        dict_datah["daq_header_version"] = self.get_daq_header_version(frag)
        dict_datah["det_data_version"] = self.get_det_data_version(frag)
        dict_datah["det_id"], dict_datah["crate_id"], dict_datah["slot_id"], dict_datah["stream_id"] = self.get_det_crate_slot_stream(frag)
        dict_datah["timestamp_first"] = self.get_timestamp_first(frag)
        return (dict_datah | self.dict_idx), 1, list(self.dict_idx.keys())

    def get_det_header_dict(self,frag):
        return None

    def get_det_data_dict(self,frag):
        return None        


class WIBEthUnpacker(DetectorFragmentUnpacker):

    unpacker = rawdatautils.unpack.wibeth
    frame_obj = fddetdataformats.WIBEthFrame
    
    DICT_DET_HEADER_COLS = [
        "femb_id",
        "coldata_id",
        "version",
        "cd",
        "pulser",
        "calibration",
        "context",
        "n_channels",
        "ts_diffs_vals",
        "ts_diffs_counts",
        "sampling_period"
    ]
    DICT_DET_DATA_COLS = [
        "channel",
        "plane",
        "wib_chan",
        "mean",
        "rms",
        "max",
        "min",
        "median"
    ]
    SAMPLING_PERIOD = 32
    N_CHANNELS_PER_FRAME = 64
    
    def __init__(self,dict_idx,channel_map):
        super().__init__(dict_idx)
        self.channel_map = detchannelmaps.make_map(channel_map)
        
    def get_n_obj(self,frag):
        return self.unpacker.get_n_frames(frag)

    def get_daq_header_version(self,frag):
        return self.frame_obj(frag.get_data()).get_daqheader().version

    def get_timestamp_first(self,frag):
        return self.frame_obj(frag.get_data()).get_timestamp()
    
    def get_det_data_version(self,frag):
        return self.frame_obj(frag.get_data()).get_wibheader().version

    def get_det_crate_slot_stream(self,frag):
        dh = self.frame_obj(frag.get_data()).get_daqheader()
        return dh.det_id, dh.crate_id, dh.slot_id, dh.stream_id

    def get_det_header_dict(self,frag):
        dict_deth = dict.fromkeys(self.DICT_DET_HEADER_COLS)
        wh = self.frame_obj(frag.get_data()).get_wibheader()
        dict_deth["femb_id"] = (wh.channel>>1)&0x3
        dict_deth["coldata_id"] = wh.channel&0x1
        dict_deth["version"] = wh.version
        #dict_deth["cd"] = wh.cd
        dict_deth["pulser"] = wh.pulser
        dict_deth["calibration"] = wh.calibration
        dict_deth["context"] = wh.context
        dict_deth["n_channels"] = self.N_CHANNELS_PER_FRAME

        ts_diff_vals, ts_diff_counts = np.unique(np.diff(self.unpacker.np_array_timestamp(frag)),return_counts=True)
        dict_deth["ts_diffs_vals"] = ts_diff_vals
        dict_deth["ts_diffs_counts"] = ts_diff_counts

        dict_deth["sampling_period"] = self.SAMPLING_PERIOD

        return (dict_deth | self.dict_idx), 1, list(self.dict_idx.keys())

    def get_det_data_dict(self,frag):
        dict_detd = dict.fromkeys(self.DICT_DET_DATA_COLS)
        dict_detd["wib_chan"] = range(self.N_CHANNELS_PER_FRAME)
        
        _, crate, slot, stream = self.get_det_crate_slot_stream(frag)
        dict_detd["channel"] = [ self.channel_map.get_offline_channel_from_crate_slot_stream_chan(crate, slot, stream, c) for c in range(self.N_CHANNELS_PER_FRAME) ]
        dict_detd["plane"] = [ self.channel_map.get_plane_from_offline_channel(uc) for uc in dict_detd["channel"] ]
        
        adcs = self.unpacker.np_array_adc(frag)
        dict_detd["mean"] = np.mean(adcs,axis=0)
        dict_detd["rms"] = np.std(adcs,axis=0)
        dict_detd["max"] = np.max(adcs,axis=0)
        dict_detd["min"] = np.min(adcs,axis=0)
        dict_detd["median"] = np.median(adcs,axis=0)
        
        for key,value in self.dict_idx.items():
            dict_detd[key] = [ value for i in range(self.N_CHANNELS_PER_FRAME) ]

        return dict_detd, self.N_CHANNELS_PER_FRAME, list(self.dict_idx.keys())+["channel"]


class DAPHNEStreamUnpacker(DetectorFragmentUnpacker):

    unpacker = rawdatautils.unpack.daphne
    frame_obj = fddetdataformats.DAPHNEStreamFrame

    DICT_DET_HEADER_COLS = [
        "n_channels",
        "ts_diffs_vals",
        "ts_diffs_counts",
        "sampling_period"
    ]
    DICT_DET_DATA_COLS = [
        "channel",
        "daphne_chan",
        "ped",
        "rms"
    ]
    SAMPLING_PERIOD = 1
    N_CHANNELS_PER_FRAME = 4
    
    def __init__(self,dict_idx):
        super().__init__(dict_idx)
        
    def get_n_obj(self,frag):
        return self.unpacker.get_n_frames_stream(frag)

    def get_daq_header_version(self,frag):
        return self.frame_obj(frag.get_data()).get_daqheader().version

    def get_timestamp_first(self,frag):
        return self.frame_obj(frag.get_data()).get_timestamp()
    
    def get_det_data_version(self,frag):
        return 0

    def get_det_crate_slot_stream(self,frag):
        dh = self.frame_obj(frag.get_data()).get_daqheader()
        return dh.det_id, dh.crate_id, dh.slot_id, dh.link_id

    def get_det_header_dict(self,frag):
        dict_deth = dict.fromkeys(self.DICT_DET_HEADER_COLS)
        dh = self.frame_obj(frag.get_data()).get_header()
        dict_deth["n_channels"] = self.N_CHANNELS_PER_FRAME

        ts_diff_vals, ts_diff_counts = np.unique(np.diff(self.unpacker.np_array_timestamp_stream(frag)),return_counts=True)
        dict_deth["ts_diffs_vals"] = [ ts_diff_vals ]
        dict_deth["ts_diffs_counts"] = [ ts_diff_counts ]

        dict_deth["sampling_period"] = self.SAMPLING_PERIOD

        return (dict_deth | self.dict_idx), 1, list(self.dict_idx.keys())

    def get_det_data_dict(self,frag):
        dict_detd = dict.fromkeys(self.DICT_DET_DATA_COLS)
        dict_detd["daphne_chan"] = range(self.N_CHANNELS_PER_FRAME)

        dh = self.frame_obj(frag.get_data()).get_header()
        dict_detd["channel"] = [ dh.channel_0, dh.channel_1, dh.channel_2, dh.channel_3 ]
        
        adcs = self.unpacker.np_array_adc_stream(frag)
        dict_detd["ped"] = np.mean(adcs,axis=0)
        dict_detd["rms"] = np.std(adcs,axis=0)

        for key,value in self.dict_idx.items():
            dict_detd[key] = [ value for i in range(self.N_CHANNELS_PER_FRAME) ]

        return dict_detd, self.N_CHANNELS_PER_FRAME, list(self.dict_idx.keys())+["channel"]


class DAPHNEUnpacker(DetectorFragmentUnpacker):

    unpacker = rawdatautils.unpack.daphne
    frame_obj = fddetdataformats.DAPHNEFrame

    DICT_DET_HEADER_COLS = []
    DICT_DET_DATA_COLS = [
        "channel",
        "daphne_chan",
        "timestamp",
        "trigger_sample_value",
        "threshold",
        "baseline",
        "ped",
        "rms",
        "max",
        "min",
        "timestamp_max",
        "timestamp_min"
    ]
    SAMPLING_PERIOD = 1
    N_CHANNELS_PER_FRAME = 1
    
    def __init__(self,dict_idx):
        super().__init__(dict_idx)
        
    def get_n_obj(self,frag):
        return self.unpacker.get_n_frames(frag)

    def get_daq_header_version(self,frag):
        return self.frame_obj(frag.get_data()).get_daqheader().version

    def get_timestamp_first(self,frag):
        return self.frame_obj(frag.get_data()).get_timestamp()
    
    def get_det_data_version(self,frag):
        return 0

    def get_det_crate_slot_stream(self,frag):
        dh = self.frame_obj(frag.get_data()).get_daqheader()
        return dh.det_id, dh.crate_id, dh.slot_id, dh.link_id

    def get_det_header_dict(self,frag):
        return None

    def get_det_data_dict(self,frag):
        dict_detd = dict.fromkeys(self.DICT_DET_DATA_COLS)

        n_frames = self.get_n_obj(frag)

        for iframe in range(n_frames):
            dh = self.frame_obj(frag.get_data(iframe*self.frame_obj.sizeof())).get_header()
            dict_detd["daphne_chan"] = dh.channel
            dict_detd["channel"] = dh.channel
            dict_detd["trigger_sample_value"] = dh.trigger_sample_value
            dict_detd["threshold"] = dh.threshold
            dict_detd["baseline"] = dh.baseline
            
        dict_detd["timestamp"] = self.unpacker.np_array_timestamp(frag)
        
        adcs = self.unpacker.np_array_adc(frag)           
        dict_detd["ped"] = np.mean(adcs,axis=0)
        dict_detd["rms"] = np.std(adcs,axis=0)
        dict_detd["max"] = np.max(adcs,axis=0)
        dict_detd["min"] = np.max(adcs,axis=0)
        dict_detd["timestamp_max"] = np.argmax(adcs,axis=0)*self.SAMPLING_PERIOD + dict_detd["timestamp"]
        dict_detd["timestamp_min"] = np.argmax(adcs,axis=0)*self.SAMPLING_PERIOD + dict_detd["timestamp"]
        
        for key,value in self.dict_idx.items():
            dict_detd[key] = [ value for i in range(n_frames) ]

        return dict_detd, n_frames, list(self.dict_idx.keys())+["channel"]


def GetUnpacker(frag_type,det_id,dict_idx):
    if(frag_type==daqdataformats.FragmentType.kWIBEth and det_id==detdataformats.DetID.Subdetector.kHD_TPC.value):
        return WIBEthUnpacker(dict_idx,"PD2HDChannelMap")
    elif(frag_type==daqdataformats.FragmentType.kDAPHNEStream):
        return DAPHNEStreamUnpacker(dict_idx)
    elif(frag_type==daqdataformats.FragmentType.kDAPHNE):
        return DAPHNEUnpacker(dict_idx)
    else:
        return None

