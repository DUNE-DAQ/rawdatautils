#!/usr/bin/env python3

import rawdatautils.unpack.utils
import rawdatautils.analysis.dataframe_creator as dfc
from rawdatautils.analysis.dqm.dqmtools import *
from rawdatautils.analysis.dqm.dqmtests import *
from rawdatautils.analysis.dqm.dqmplots import *

import hdf5libs

import click
@click.command()
@click.argument('filenames', nargs=-1, type=click.Path(exists=True))
@click.option('--nrecords', '-n', default=1, help='How many Trigger Records to process (default: 1)')
@click.option('--nworkers', default=10, help'How many thread workers to launch (default: 10)')
@click.option('--wibpulser', is_flag=True, help='WIBs in pulser mode')
@click.option('--make-plots',is_flag=True, help='Option to make plots')

def main(filenames, nrecords, nworkers, wibpulser, make_plots):

    #setup our tests
    dqm_test_suite = DQMTestSuite()

    dqm_test_suite.register_test(CheckAllExpectedFragmentsTest())
    dqm_test_suite.register_test(CheckTimestampDiffs_HD_TPC())
    dqm_test_suite.register_test(CheckTimestampsAligned(3),"CheckTimestampsAligned_HD_TPC")
    dqm_test_suite.register_test(CheckNFramesWIBEth())
    
    if(not wibpulser): dqm_test_suite.register_test(CheckRMS_HD_TPC(threshold=100.,verbose=True),name="CheckRMS_HD_TPC_High")
    if(not wibpulser): dqm_test_suite.register_test(CheckRMS_HD_TPC(threshold=[20.,15.],operator=operator.lt,verbose=True),name="CheckRMS_HD_TPC_Low")

    df_dict = {}
    n_processed_records = 0
    for filename in filenames:
        print(f'Processing file {filename}.')
        
        h5_file = hdf5libs.HDF5RawDataFile(filename)
        records = h5_file.get_all_record_ids()

        if nrecords==-1 or nrecords > (n_processed_records+len(records)):
            records_to_process = records
        else:
            records_to_process = records[:(nrecords-n_processed_records)]
        print(f'Will process {len(records_to_process)} of {len(records)} records.')

        for rid in records_to_process:
            print(f'Processing record {rid}')
            df_dict = dfc.ProcessRecord(h5_file,rid,df_dict,MAX_WORKERS=nworkers)
            n_processed_records += 1

    df_dict = dfc.ConcatenateDataFrames(df_dict)

    dqm_test_suite.do_all_tests(df_dict)
    print(dqm_test_suite.get_table())

    if(make_plots):
        if(not wibpulser):
            plot_HD_TPC_by_channel(df_dict,var="rms",jpeg_base="pdune2_hd_tpc_rms")
            plot_HD_TPC_by_channel(df_dict,var="rms",yrange=[-1,60],jpeg_base="pdune2_hd_tpc_rms_fixrange")
            plot_HD_TPC_by_channel(df_dict,var="mean",jpeg_base="pdune2_hd_tpc_mean")

        if(wibpulser):
            plot_HD_TPC_pulser_by_channel(df_dict,jpeg_base='pdune2_hd_tpc_pulser')

if __name__ == '__main__':
    main()
