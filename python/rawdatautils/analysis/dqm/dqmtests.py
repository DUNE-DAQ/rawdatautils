from rawdatautils.analysis.dqm.dqmtools import *

import numpy as np
import operator

class CheckAllExpectedFragmentsTest(DQMTest):

    def __init__(self):
        super().__init__()
        self.name = 'CheckAllExpectedFragmentsTest'

    def run_test(self,df_dict):
        df_tmp = (df_dict["trh"]["n_fragments"]!=df_dict["trh"]["n_requested_components"])
        n_not_matched = df_tmp.sum()
        if n_not_matched==0:
            return DQMTestResult(DQMResultEnum.OK,f'OK')
        else:
            return DQMTestResult(DQMResultEnum.BAD,f'{n_not_matched} / {len(df_tmp)} records missing fragments.')

class CheckTimestampDiffs_HD_TPC(DQMTest):

    def __init__(self):
        super().__init__()
        self.name = 'CheckTimestampDiffs_HD_TPC'

    def run_test(self,df_dict):
        df_tmp = df_dict["det_head_kHD_TPC_kWIBEth"]
        df_tmp["ts_diff_wrong"] = df_tmp.apply(lambda x: (x.ts_diffs_vals!=x.sampling_period).sum(), axis=1)
        n_ts_diff_wrong = df_tmp["ts_diff_wrong"].sum()
        if n_ts_diff_wrong==0:
            return DQMTestResult(DQMResultEnum.OK,f'OK')
        else:
            return DQMTestResult(DQMResultEnum.BAD,
                                 f'{n_ts_diff_wrong} / {len(df_tmp)} fragments have bad timestamp differences.')


class CheckTimestampsAligned(DQMTest):

    def __init__(self,det_id):
        super().__init__()
        self.det_id = det_id
        self.name = f'CheckTimestampsAligned_{det_id}'

    def any_different(arr):
        return (arr.values!=arr.values[0]).sum()
    
    def run_test(self,df_dict):
        df_tmp = df_dict["daqh"].loc[df_dict["daqh"]["det_id"]==self.det_id]
        if len(df_tmp)==0:
            return DQMTestResult(DQMResultEnum.WARNING,f'WARNING: No components found with detector id {det_id}.')
        df_tmp = df_tmp.groupby(by=["run_idx","record_idx","sequence_idx"])
        n_different = df_tmp["timestamp_first"].agg(CheckTimestampsAligned.any_different).sum()
        if n_different==0:
            return DQMTestResult(DQMResultEnum.OK,f'OK')
        else:
            return DQMTestResult(DQMResultEnum.BAD,
                                 f'{n_different} / {len(df_tmp)} records have timestamp misalignment for det_id {det_id}.')

class CheckNFramesWIBEth(DQMTest):

    def __init__(self):
        super().__init__()
        self.name = "CheckNFramesWIBEth"

    def run_test(self,df_dict):
        df_tmp = df_dict["frh"].loc[df_dict["frh"]["fragment_type"]==12][["window_begin","window_end"]]
        if len(df_tmp)==0:
            return DQMTestResult(DQMResultEnum.WARNING,f'WARNING: No WIBEth components found.')
        df_tmp["expected_frames"] = np.floor((df_tmp["window_end"]-df_tmp["window_begin"])/(32*64))+1
        df_tmp = df_tmp.join(df_dict["daqh"][["n_obj"]])
        n_frames_wrong = (df_tmp["expected_frames"]!=df_tmp["n_obj"]).sum()
        if n_frames_wrong==0:
            return DQMTestResult(DQMResultEnum.OK,f'OK')
        else: 
            return DQMTestResult(DQMResultEnum.BAD,
                                 f'{n_frames_wrong} / {len(df_tmp)} WIBEth fragments have the wrong number of frames.')


class CheckRMS_HD_TPC(DQMTest):

    def __init__(self,threshold=100,operator=operator.gt,verbose=False):
        super().__init__()
        self.name = 'CheckRMS_HD_TPC'
        if not isinstance(threshold,list): #one value for all planes
            self.df_threshold = pd.DataFrame({"plane":[0,1,2],"threshold":np.full(3,threshold)})
        elif len(threshold)==1: #one value for all planes
            self.df_threshold = pd.DataFrame({"plane":[0,1,2],"threshold":np.full(3,threshold[0])})
        elif len(threshold)==2: #two values, first induction, second collection
            self.df_threshold = pd.DataFrame({"plane":[0,1,2],"threshold":np.array([threshold[0],threshold[0],threshold[1]])})
        elif len(threshold)==3: #three values, one for each plane
            self.df_threshold = pd.DataFrame({"plane":[0,1,2],"threshold":np.array(threshold)})
        else:
            print(f'Threshold length {len(threshold)} is not valid.',threshold)
            raise ValueError
        self.operator = operator
        self.verbose = verbose
        

    def run_test(self,df_dict):
        df_tmp = df_dict["det_data_kHD_TPC_kWIBEth"].reset_index()[["channel","rms","plane"]].merge(self.df_threshold,on=["plane"])
        df_tmp = df_tmp.loc[self.operator(df_tmp["rms"],df_tmp["threshold"])]      
        n_rms_bad = len(df_tmp)
        if n_rms_bad==0:
            return DQMTestResult(DQMResultEnum.OK,f'OK')
        else:
            if self.verbose:
                print("CHANNELS FAILING RMS CHECK")
                print(tabulate(df_tmp.reset_index()[["channel","rms","plane","threshold"]],
                               headers=["Channel","RMS","Plane","Threshold"],
                               showindex=False,tablefmt='pretty'))
            return DQMTestResult(DQMResultEnum.BAD,
                                 f'{n_rms_bad}  channels have RMS outside of range.')
