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
    trigger_time = df_dict['trh'].loc[index[0:3]]["trigger_timestamp"]
    return datetime.fromtimestamp(trigger_time*16 // 1e9).astimezone(pytz.timezone("Europe/Zurich"))

def plot_HD_TPC_by_channel(df_dict,var,run=None,trigger=None,seq=None,yrange=None,jpeg_base=None):
    
    df_tmp, index = dfc.SelectRecord(df_dict["det_data_kHD_TPC_kWIBEth"],run,trigger,seq)
    
    df_tmp = df_tmp.join(df_dict['daqh'][["crate_id"]]).reset_index()
    df_tmp["apa_plane_label"] = df_tmp[["crate_id","plane"]].apply(lambda x: f'APA {x.crate_id}, Plane {x.plane}',axis=1)
    
    trigger_time = get_CERN_timestamp(df_dict,index);
    
    fig = px.scatter(df_tmp,x="channel",y=var,color="apa_plane_label",width=1000,height=600)
    fig.update_layout(xaxis_title='Channel',yaxis_title=var,legend_title='APA/Plane',title=f'Run {index[0]}, Record ({index[1],index[2]}), Time {trigger_time}')
    if yrange is not None:
       fig.update_yaxes(range=yrange)
    if jpeg_base is not None:
        fig.write_image(f"{jpeg_base}_run{index[0]}_trigger{index[1]}_seq{index[2]}.jpeg")
    return fig

def plot_HD_TPC_pulser_by_channel(df_dict,run=None,trigger=None,seq=None,jpeg_base=None):
    
    df_tmp, index = dfc.SelectRecord(df_dict["det_data_kHD_TPC_kWIBEth"],run,trigger,seq)
    df_tmp= df_tmp.reset_index()
    
    trigger_time = get_CERN_timestamp(df_dict,index);
    
    fig = px.scatter(df_tmp,x="channel",y=["max","min","median"],
                     width=1000,height=600)
    fig.update_layout(xaxis_title='Channel',
                      yaxis_title="ADC value",
                      legend_title=None,
                      title=f'Run {index[0]}, Record ({index[1],index[2]}), Time {trigger_time}')
    if jpeg_base is not None:
        fig.write_image(f"{jpeg_base}_run{index[0]}_trigger{index[1]}_seq{index[2]}.jpeg")
    return fig

