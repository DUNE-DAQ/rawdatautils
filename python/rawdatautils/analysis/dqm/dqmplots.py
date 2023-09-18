import sys
from rawdatautils.analysis.dqm.dqmtools import *

import rawdatautils.unpack.utils
import rawdatautils.analysis.dataframe_creator as dfc
from rawdatautils.analysis.dqm.dqmtools import *
from rawdatautils.analysis.dqm.dqmtests import *

try:
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
except ModuleNotFoundError as err:
    print(err)
    print("\n\n")
    print("Missing module is likely not part of standard dunedaq releases.")
    print("\n")
    print("Please install the missing module and try again.")
    sys.exit(1)
except:
    raise

def get_CERN_timestamp(df_dict,index):
    trigger_time = df_dict['trh'].loc[(index.run,index.trigger,index.sequence)]["trigger_time"]
    return trigger_time.astimezone(pytz.timezone("Europe/Zurich"))

def plot_WIBEth_by_channel(df_dict,var,det_name,run=None,trigger=None,seq=None,yrange=None,jpeg_base=None):
    
    df_tmp, index = dfc.SelectRecord(df_dict[f"detd_k{det_name}_kWIBEth"],run,trigger,seq)
    df_tmp = df_tmp.reset_index()
    df_tmp["apa_plane_label"] = df_tmp[["apa","plane"]].apply(lambda x: f'{x.apa}, Plane {x.plane}',axis=1)
    
    trigger_time = get_CERN_timestamp(df_dict,index);

    fig = px.scatter(df_tmp,x="channel",y=var,color="apa_plane_label",width=1000,height=600)
    fig.update_layout(xaxis_title='Channel',yaxis_title=var,legend_title='APA/CRP, Plane',
                          title=f'Run {index.run}, Record ({index.trigger,index.sequence}), Time {trigger_time}')
    if yrange is not None:
       fig.update_yaxes(range=yrange)
    if jpeg_base is not None:
        fig.write_image(f"{jpeg_base}_run{index.run}_trigger{index.trigger}_seq{index.sequence}.jpeg")
    return fig

def plot_WIBEth_pulser_by_channel(df_dict,det_name,run=None,trigger=None,seq=None,jpeg_base=None):
    
    df_tmp, index = dfc.SelectRecord(df_dict[f"detd_k{det_name}_kWIBEth"],run,trigger,seq)
    df_tmp= df_tmp.reset_index()
    
    trigger_time = get_CERN_timestamp(df_dict,index);
    
    fig = px.scatter(df_tmp,x="channel",y=["adc_max","adc_min","adc_median"],
                     width=1000,height=600)
    fig.update_layout(xaxis_title='Channel',
                      yaxis_title="ADC value",
                      legend_title=None,
                      title=f'WIB Pulser Check: Run {index.run}, Record ({index.trigger,index.sequence}), Time {trigger_time}')
    if jpeg_base is not None:
        fig.write_image(f"{jpeg_base}_run{index.run}_trigger{index.trigger}_seq{index.sequence}.jpeg")
    return fig

