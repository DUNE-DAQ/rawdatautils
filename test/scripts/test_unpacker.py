#!/usr/bin/env python


import logging
import pandas as pd
import hdf5libs
import click
import rawdatautils.rawdataunpacker as rdu
from rich import print
from rich.logging import RichHandler



CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])
@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('rawfile', type=click.Path(exists=True))
@click.option('-i', '--interactive', is_flag=True, default=False)
@click.option('-p', '--plot', type=str, default=False)
def main(rawfile, interactive, plot):


    # wf_up = rdu.WIBFragmentUnpacker('VDColdbox')
    wethf_up = rdu.WIBEthFragmentUnpacker(None)
    # tp_up = rdu.TPFragmentUnpacker('VDColdbox')

    u = rdu.Unpacker()

    u.add_unpacker('bde_eth', wethf_up)
    # u.add_unpacker('bde', wf_up)
    # u.add_unpacker('tp', tp_up)


    print(f"Opening {rawfile}")
    rdf = hdf5libs.HDF5RawDataFile(rawfile)

        
    trs = [ i for i,_ in rdf.get_all_trigger_record_ids()]
    # process only the first TR
    trs = trs[:1]

    for tr in trs:
        print(f"Reading Trigger Record {tr}")

        unpacked_tr = u.unpack(rdf, tr)

        if 'tp' in unpacked_tr:
            print("Assembling TPs")
            df_tp = pd.concat(unpacked_tr['tp'].values())
            df_tp = df_tp.sort_values(by=['time_start', 'channel'])
            print(f"TPs dataframe assembled {len(df_tp)}")

        
        if 'bde_eth' in unpacked_tr:
            print("Assembling WIBEth Frames")
            dfs_bde = {k:v for k,v in unpacked_tr['bde_eth'].items() if not v is None}

            idx = pd.Index([], dtype='uint64')
            for df in dfs_bde.values():
                idx = idx.union(df.index)

            df_tpc = pd.DataFrame(index=idx, dtype='uint16')
            for df in dfs_bde.values():
                df_tpc = df_tpc.join(df)
            df_tpc = df_tpc.reindex(sorted(df_tpc.columns), axis=1)
            
            print(f"TPC adcs dataframe assembled {len(df_tpc)}")


    if plot:
        import matplotlib.pyplot as plt

        print("Plotting the first 128 samples")
        # xticks = df_tpc.columns[::len(df_tpc.columns)//10]
        xpos = list(range(len(df_tpc.columns)))[::len(df_tpc.columns)//10]

        fig, ax = plt.subplots(figsize=(10,8))
        pcm = ax.pcolormesh(df_tpc.iloc[:128])
        fig.colorbar(pcm)
        ax.set_xticks(xpos, [df_tpc.columns[i] for i in xpos])
        ax.set_xlabel("channel id")
        ax.set_ylabel("Samples (since start of RO window)")
        fig.savefig(f'wibeth_frame__{plot}_0-127ticks.png')
        # fig.savefig('wibeth_frame_0-127ticks.pdf')

        print("Plotting the all samples")

        fig, ax = plt.subplots(figsize=(10,8))
        pcm = ax.pcolormesh(df_tpc)
        fig.colorbar(pcm)
        ax.set_xticks(xpos, [df_tpc.columns[i] for i in xpos])
        ax.set_xlabel("channel id")
        ax.set_ylabel("Samples (since start of RO window)")
        fig.savefig(f'wibeth_frame_{plot}.png')
        print("Plotting done")

        # # Assigning labels of x-axis 
        # # according to dataframe
        # # plt.xticks(range(len(df_tpc.columns)), df_tpc.columns)
        
        # # Assigning labels of y-axis 
        # # according to dataframe
        # # plt.yticks(range(len(df_tpc)), df_tpc.index)
        # plt.colorbar()
        # plt.savefig('wibeth_frame.png')




    if interactive:
        import IPython
        IPython.embed(colors='neutral')

if __name__ == "__main__":
        
	FORMAT = "%(message)s"
	logging.basicConfig(
    	level="INFO",
        format=FORMAT,
        datefmt="[%X]",
        handlers=[RichHandler()]
	)

	main()
