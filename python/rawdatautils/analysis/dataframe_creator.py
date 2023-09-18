import rawdatautils.unpack.utils
import detdataformats
import hdf5libs
import h5py

import numpy as np
import numpy.fft

from datetime import datetime
import pytz
import concurrent.futures

from rawdatautils.unpack.dataclasses import *

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


#def CreateDataFrame(dict,nrows,idx_list):
#    return pd.DataFrame(dict,index=range(nrows)).set_index(idx_list)

def GetFragmentUnpacker(frag_type,det_id):
    
    if(frag_type==daqdataformats.FragmentType.kWIBEth and det_id==detdataformats.DetID.Subdetector.kHD_TPC.value):
        return rawdatautils.unpack.utils.WIBEthUnpacker("PD2HDChannelMap")
    elif(frag_type==daqdataformats.FragmentType.kWIBEth and det_id==detdataformats.DetID.Subdetector.kVD_BottomTPC.value):
        return rawdatautils.unpack.utils.WIBEthUnpacker("PD2VDBottomTPCChannelMap")
    elif(frag_type==daqdataformats.FragmentType.kDAPHNEStream):
        return rawdatautils.unpack.utils.DAPHNEStreamUnpacker()
    elif(frag_type==daqdataformats.FragmentType.kDAPHNE):
        return rawdatautils.unpack.utils.DAPHNEUnpacker()
    else:
        return None
    
def ProcessSourceID(h5_file,sid,record_index):

    sid_unpacker = rawdatautils.unpack.utils.SourceIDUnpacker(record_index)
    return_dict = sid_unpacker.get_all_data(sid)

    if(sid.subsystem==daqdataformats.SourceID.Subsystem.kTRBuilder):
        trh = h5_file.get_trh(record_index.trigger,record_index.sequence)
        n_frags = len(h5_file.get_fragment_dataset_paths(record_index.trigger,record_index.sequence))
        return (return_dict | rawdatautils.unpack.utils.TriggerRecordHeaderUnpacker().get_all_data((trh,n_frags)) )

    if(sid.subsystem==daqdataformats.SourceID.Subsystem.kDetectorReadout):
        frag = h5_file.get_frag((record_index.trigger,record_index.sequence),sid)

        frag_type=frag.get_fragment_type()
        det_id=frag.get_detector_id()
        type_string = f'{detdataformats.DetID.Subdetector(det_id).name}_{frag_type.name}'

        fragment_unpacker = GetFragmentUnpacker(frag_type,det_id)
        if fragment_unpacker is None:
            print(f'Unknown fragment {type_string}. Source ID {sid}')
            return return_dict

        return (return_dict | fragment_unpacker.get_all_data(frag) )
        
    return return_dict

def ProcessRecord(h5_file,rid,df_dict,MAX_WORKERS=10):

    with h5py.File(h5_file.get_file_name(), 'r') as f:
        record_index = RecordDataBase(run=f.attrs["run_number"],trigger=rid[0],sequence=rid[1])
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_sid = {executor.submit(ProcessSourceID,h5_file,sid,record_index): sid for sid in h5_file.get_source_ids(rid)}
        for future in concurrent.futures.as_completed(future_to_sid):
            sid = future_to_sid[future]
            res = future.result()
            for key, df in res.items():
                if key not in df_dict.keys():
                    df_dict[key] = []
                df_dict[key].extend(df)

    return df_dict

def SelectRecord(df,run=None,trigger=None,sequence=None):
    if (run is None) and (trigger is None) and (sequence is None):
        index = df.index[0][0:3]
    else:
        qstr=''
        if run is not None: qstr = qstr+f'run=={run}'
        if trigger is not None: qstr = qstr+f'trigger=={trigger}'
        if sequence is not None: qstr = qstr+f'sequence=={sequence}'
        index = df.query(qstr).index[0][0:3]
    
    try:
        return df.loc[index], RecordDataBase(run=index[0],trigger=index[1],sequence=index[2])
    except KeyError as err:
        print(f'index {index[0:3]} not found.')
        return None, None
    except:
        raise


def ConcatenateDataFrames(df_dict):
    for key, dc_list in df_dict.items():
        idx = dc_list[0].index_names()
        df_dict[key] = pd.DataFrame(dc_list)
        df_dict[key] = df_dict[key].set_index(idx)
    
    return df_dict
    
