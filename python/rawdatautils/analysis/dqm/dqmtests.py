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

class CheckTimestampDiffs_WIBEth(DQMTest):
    
    def __init__(self,det_name):
        super().__init__()
        self.name = f'CheckTimestampDiffs_WIBEth_{det_name}'
        self.det_head_key=f'deth_k{det_name}_kWIBEth'
        
    def run_test(self,df_dict):

        if self.det_head_key not in df_dict.keys():
            return DQMTestResult(DQMResultEnum.WARNING,f'Could not find {self.det_head_key} in DataFrame dict.')
        
        df_tmp = df_dict[self.det_head_key]
        df_tmp["ts_diff_wrong"] = df_tmp.apply(lambda x: (x.ts_diffs_vals!=x.sampling_period).sum(), axis=1)
        n_ts_diff_wrong = df_tmp["ts_diff_wrong"].sum()
        if n_ts_diff_wrong==0:
            return DQMTestResult(DQMResultEnum.OK,f'OK')
        else:
            return DQMTestResult(DQMResultEnum.BAD,
                                 f'{n_ts_diff_wrong} / {len(df_tmp)} fragments have bad timestamp differences.')


class CheckTimestampsAligned(DQMTest):

    def __init__(self,det_id,verbose=True):
        super().__init__()
        self.det_id = det_id
        self.name = f'CheckTimestampsAligned_{det_id}'
        self.verbose = verbose
        
    def any_different(arr):
        return (arr.values!=arr.values[0]).sum()

    def unique(arr):
        return np.unique(arr.values,return_counts=True)
    
    def run_test(self,df_dict):
        df_tmp = df_dict["daqh"].loc[df_dict["daqh"]["det_id"]==self.det_id]
        if len(df_tmp)==0:
            return DQMTestResult(DQMResultEnum.WARNING,f'WARNING: No components found with detector id {self.det_id}.')
        df_tmp_gb = df_tmp.groupby(by=["run","trigger","sequence"])["timestamp_first_dts"].agg(CheckTimestampsAligned.unique)
        df_tmp_gb_n = df_tmp_gb.apply(lambda x: len(x[1]))
        n_different = (df_tmp_gb_n!=1).sum()
        if n_different==0:
            return DQMTestResult(DQMResultEnum.OK,f'OK')
        else:
            if self.verbose:
                df_tmp_gb_mode = df_tmp_gb.apply(lambda x: x[0][np.argmax(x[1])])
                df_tmp = df_tmp.join(df_tmp_gb_mode,rsuffix='_majority')
                df_tmp = df_tmp.loc[(df_tmp["timestamp_first"]!=df_tmp["timestamp_first_majority"])]
                df_tmp["timestamp_diff"] = df_tmp["timestamp_first"]-df_tmp["timestamp_first_majority"]
                n_different = len(np.unique(df_tmp.reset_index()["src_id"].apply(lambda x: int(x.id))))
                print("FRAGMENTS FAILING TIMESTAMP ALIGNMENT")
                print(tabulate(df_tmp.reset_index()[["record_idx","sequence_idx","crate_id","slot_id","stream_id","timestamp_first","timestamp_first_majority","timestamp_diff"]],
                               headers=["Record","Seq.","Crate","Slot","Stream","Timestamp (first)","Majority timestamp","Difference"],
                               showindex=False,tablefmt='pretty'))
            return DQMTestResult(DQMResultEnum.BAD,
                                 f'{n_different} sources have some timestamp misalignment for det_id {self.det_id}.')

class CheckNFrames_WIBEth(DQMTest):

    def __init__(self):
        super().__init__()
        self.name = "CheckNFrames_WIBEth"

    def run_test(self,df_dict):
        df_tmp = df_dict["frh"].loc[df_dict["frh"]["fragment_type"]==12][["window_begin_dts","window_end_dts"]]
        if len(df_tmp)==0:
            return DQMTestResult(DQMResultEnum.WARNING,f'WARNING: No WIBEth components found.')
        df_tmp["expected_frames"] = np.floor((df_tmp["window_end_dts"]-df_tmp["window_begin_dts"])/(32*64))+1
        df_tmp = df_tmp.join(df_dict["daqh"][["n_obj"]])
        n_frames_wrong = (df_tmp["expected_frames"]!=df_tmp["n_obj"]).sum()
        if n_frames_wrong==0:
            return DQMTestResult(DQMResultEnum.OK,f'OK')
        else:            
            return DQMTestResult(DQMResultEnum.BAD,
                                 f'{n_frames_wrong} / {len(df_tmp)} WIBEth fragments have the wrong number of frames.')


class CheckRMS_WIBEth(DQMTest):

    def __init__(self,det_name,threshold=100,operator=operator.gt,verbose=False):
        super().__init__()
        self.name = 'CheckRMS_{det_name}'
        self.det_data_key=f'detd_k{det_name}_kWIBEth'

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

        if self.det_data_key not in df_dict.keys():
            return DQMTestResult(DQMResultEnum.WARNING,f'Could not find {self.det_data_key} in DataFrame dict.')
        
        df_tmp = df_dict[self.det_data_key].reset_index().merge(self.df_threshold,on=["plane"])
        df_tmp = df_tmp[["channel","adc_rms","threshold"]].groupby(by="channel").mean().reset_index()
        df_tmp = df_tmp.loc[self.operator(df_tmp["adc_rms"],df_tmp["threshold"])]
        n_rms_bad = len(np.unique(df_tmp["channel"]))

        if n_rms_bad==0:
            return DQMTestResult(DQMResultEnum.OK,f'OK')

        else:
            if self.verbose:
                print("CHANNELS FAILING RMS CHECK")
                df_tmp = df_tmp.merge(df_dict[self.det_data_key].reset_index()[["channel","apa","plane"]].drop_duplicates(["channel"]),on=["channel"])
                print(tabulate(df_tmp.reset_index()[["channel","adc_rms","apa","plane","threshold"]],
                               headers=["Channel","RMS","APA/CRP","Plane","Threshold"],
                               showindex=False,tablefmt='pretty',floatfmt=".2f"))
            return DQMTestResult(DQMResultEnum.BAD,
                                 f'{n_rms_bad} channels have RMS outside of range.')

class CheckPedestal_WIBEth(DQMTest):

    def __init__(self,det_name,lower_bound=[7500,200],upper_bound=[9500,2000],verbose=False):
        super().__init__()
        self.name = 'CheckPedestal_{det_name}'
        self.det_data_key=f'detd_k{det_name}_kWIBEth'

        if not isinstance(lower_bound,list): #one value for all planes
            self.df_lower_bound = pd.DataFrame({"plane":[0,1,2],"lower_bound":np.full(3,lower_bound)})
        elif len(lower_bound)==1: #one value for all planes
            self.df_lower_bound = pd.DataFrame({"plane":[0,1,2],"lower_bound":np.full(3,lower_bound[0])})
        elif len(lower_bound)==2: #two values, first induction, second collection
            self.df_lower_bound = pd.DataFrame({"plane":[0,1,2],"lower_bound":np.array([lower_bound[0],lower_bound[0],lower_bound[1]])})
        elif len(lower_bound)==3: #three values, one for each plane
            self.df_lower_bound = pd.DataFrame({"plane":[0,1,2],"lower_bound":np.array(lower_bound)})
        else:
            print(f'Lower_Bound length {len(lower_bound)} is not valid.',lower_bound)
            raise ValueError

        if not isinstance(upper_bound,list): #one value for all planes
            self.df_upper_bound = pd.DataFrame({"plane":[0,1,2],"upper_bound":np.full(3,upper_bound)})
        elif len(upper_bound)==1: #one value for all planes
            self.df_upper_bound = pd.DataFrame({"plane":[0,1,2],"upper_bound":np.full(3,upper_bound[0])})
        elif len(upper_bound)==2: #two values, first induction, second collection
            self.df_upper_bound = pd.DataFrame({"plane":[0,1,2],"upper_bound":np.array([upper_bound[0],upper_bound[0],upper_bound[1]])})
        elif len(upper_bound)==3: #three values, one for each plane
            self.df_upper_bound = pd.DataFrame({"plane":[0,1,2],"upper_bound":np.array(upper_bound)})
        else:
            print(f'Upper_Bound length {len(upper_bound)} is not valid.',upper_bound)
            raise ValueError

        self.verbose = verbose
        

    def run_test(self,df_dict):

        if self.det_data_key not in df_dict.keys():
            return DQMTestResult(DQMResultEnum.WARNING,f'Could not find {self.det_data_key} in DataFrame dict.')
        
        df_tmp = df_dict[self.det_data_key].reset_index().merge(self.df_lower_bound,on=["plane"]).merge(self.df_upper_bound,on=["plane"])
        df_tmp = df_tmp[["channel","adc_mean","lower_bound","upper_bound"]].groupby(by="channel").mean().reset_index()
        df_tmp = df_tmp.loc[(df_tmp["adc_mean"]<df_tmp["lower_bound"])|(df_tmp["adc_mean"]>df_tmp["upper_bound"])]
        n_bad = len(np.unique(df_tmp["channel"]))
        if n_bad==0:
            return DQMTestResult(DQMResultEnum.OK,f'OK')
        else:
            if self.verbose:
                print("CHANNELS FAILING PEDESTAL CHECK")
                df_tmp = df_tmp.merge(df_dict[self.det_data_key].reset_index()[["channel","apa","plane"]].drop_duplicates(["channel"]),on=["channel"])
                print(tabulate(df_tmp.reset_index()[["channel","adc_mean","apa","plane","lower_bound","upper_bound"]],
                               headers=["Channel","Pedestal","APA/CRP","Plane","Lower Bound","Upper Bound"],
                               showindex=False,tablefmt='pretty',floatfmt=".2f"))
            return DQMTestResult(DQMResultEnum.BAD,
                                 f'{n_bad} channels have pedestal outside of range.')
