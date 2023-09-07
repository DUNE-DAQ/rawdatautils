import rawdatautils.unpack.utils
import detdataformats
import hdf5libs
import h5py

import numpy as np
import numpy.fft

from datetime import datetime
import pytz
import concurrent.futures

#non-standard imports
try:
    import pandas as pd
except ModuleNotFoundError as err:
    print(err)
    print("\n\n")
    print("Missing module is likely not part of standard dunedaq releases.")
    print("\n")
    print("Please install the missing module and try again.")
    sys.exit(1)
except:
    raise


def CreateDataFrame(dict,nrows,idx_list):
    return pd.DataFrame(dict,index=range(nrows)).set_index(idx_list)

def ProcessSourceID(h5_file,rid,sid,dict_idx):

    return_dict = {}

    sid_dict, sid_nrows, sid_idx = rawdatautils.unpack.utils.CreateSourceIDDict(sid,dict_idx)
    #return_dict["sid"] = CreateDataFrame(sid_dict, sid_nrows, sid_idx)
    return_dict["sid"] = (sid_dict, sid_nrows, sid_idx)

    dict_idx_frags = dict_idx.copy()
    dict_idx_frags["src_id"]=sid

    frag = h5_file.get_frag(rid,sid)
    frh_dict, frh_nrows, frh_idx = rawdatautils.unpack.utils.CreateFragmentHeaderDict(frag.get_header(),dict_idx_frags)
    frh_dict["data_size"] = frag.get_data_size()
    #return_dict["frh"] = CreateDataFrame(frh_dict,frh_nrows,frh_idx)
    return_dict["frh"] = (frh_dict,frh_nrows,frh_idx)

    type_string = f'{detdataformats.DetID.Subdetector(frag.get_detector_id()).name}_{frag.get_fragment_type().name}'
    unpacker = rawdatautils.unpack.utils.GetUnpacker(frag.get_fragment_type(),frag.get_detector_id(),dict_idx_frags)
    
    if unpacker is None:
        print(f'Unknown fragment {type_string}. Source ID {sid}')
        return return_dict

    if unpacker.is_detector_unpacker:
        daqh_dict, daqh_nrows, daqh_idx = unpacker.get_daq_header_dict(frag)
        deth_dict, deth_nrows, deth_idx = unpacker.get_det_header_dict(frag)
        detd_dict, detd_nrows, detd_idx = unpacker.get_det_data_dict(frag)
        
        if daqh_dict is not None:
            #return_dict[unpacker.daq_header_dict_name] = CreateDataFrame(daqh_dict, daqh_nrows, daqh_idx)
            return_dict[unpacker.daq_header_dict_name] = (daqh_dict, daqh_nrows, daqh_idx)
        if deth_dict is not None:
            #return_dict[f'det_head_{type_string}'] = CreateDataFrame(deth_dict,deth_nrows, deth_idx)
            return_dict[f'det_head_{type_string}'] = (deth_dict,deth_nrows, deth_idx)
        if detd_dict is not None:
            #return_dict[f'det_data_{type_string}'] = CreateDataFrame(detd_dict, detd_nrows, detd_idx)
            return_dict[f'det_data_{type_string}'] = (detd_dict, detd_nrows, detd_idx)

    if unpacker.is_trigger_unpacker:
        trgh_dict, trgh_nrows, trgh_idx = unpacker.get_trg_header_dict(frag)
        trgd_dict, trgd_nrows, trgd_idx = unpacker.get_trg_data_dict(frag)

        if trgh_dict is not None:
            #return_dict[unpacker.trg_header_dict_name] = CreateDataFrame(trgh_dict, trgh_nrows, trgh_idx)
            return_dict[unpacker.trg_header_dict_name] = (trgh_dict, trgh_nrows, trgh_idx)
        if trgd_dict is not None:
            #return_dict[f'trg_data_{type_string}'] = CreateDataFrame(trgh_dict, trgh_nrows, trgh_idx)
            return_dict[f'trg_data_{type_string}'] = (trgh_dict, trgh_nrows, trgh_idx)

    return return_dict

def ProcessRecord(h5_file,rid,df_dict,MAX_WORKERS=10):

    with h5py.File(h5_file.get_file_name(), 'r') as f:
        run_number = f.attrs["run_number"]
    
    dict_idx = rawdatautils.unpack.utils.CreateDFIndexDict(run_number,rid)
    
    trh_dict, trh_nrows, trh_idx = rawdatautils.unpack.utils.CreateTriggerRecordHeaderDict(h5_file.get_trh(rid),dict_idx)
    trh_dict["n_fragments"] = len(h5_file.get_fragment_dataset_paths(rid))
    if 'trh' not in df_dict.keys():
        df_dict['trh'] = []
    #df_dict["trh"].append( CreateDataFrame(trh_dict,trh_nrows,trh_idx) )
    df_dict["trh"].append((trh_dict,trh_nrows,trh_idx))

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_sid = {executor.submit(ProcessSourceID,h5_file,rid,sid,dict_idx): sid for sid in h5_file.get_source_ids(rid)}
        for future in concurrent.futures.as_completed(future_to_sid):
            sid = future_to_sid[future]
            res = future.result()
            for key, df in res.items():
                if key not in df_dict.keys():
                    df_dict[key] = []
                df_dict[key].append(df)

    return df_dict

def SelectRecord(df,run=None,trigger=None,sequence=None):
    if (run is None) and (trigger is None) and (sequence is None):
        index = df.index[0][0:3]
    else:
        qstr=''
        if run is not None: qstr = qstr+f'run_idx=={run}'
        if trigger is not None: qstr = qstr+f'record_idx=={trigger}'
        if sequence is not None: qstr = qstr+f'sequence_idx=={sequence}'
        index = df.query(qstr).index[0][0:3]
    
    try:
        return df.loc[index], index
    except KeyError as err:
        print(f'index {index[0:3]} not found.')
        return None, None
    except:
        raise


def ConcatenateDataFrames(df_dict):
    for key, df_list in df_dict.items():
        df_idx = df_list[0][2]
        nrows = df_list[0][1]
        df_dict[key] = pd.DataFrame([d[0] for d in df_list])
        if nrows>1:
            df_dict[key] = df_dict[key].explode(list(df_dict[key].columns.values))
        df_dict[key] = df_dict[key].set_index(df_idx)
    
    return df_dict
    
