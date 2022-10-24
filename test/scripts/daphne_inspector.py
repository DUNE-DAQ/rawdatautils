#!/usr/bin/env python

from hdf5libs import HDF5RawDataFile

import daqdataformats
import detdataformats.daphne
# import detchannelmaps


import collections
import click
import rich
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

samples_per_frame = 64
chan_per_frame = 4

@click.command()
@click.option('-i', '--interactive', is_flag=True, default=False)
@click.option('-n', '--tr-num', type=int, default=1)
@click.argument('filename', type=click.Path(exists=True))
def cli(interactive, tr_num, filename):
    
    h5_file = HDF5RawDataFile(filename)

    tr_ids = h5_file.get_all_trigger_record_ids()

    rich.print(tr_ids)

    if (tr_num, 0) not in tr_ids:
        rich.print(f'[red]ERROR: Trigger record {tr_num} not found in {filename}[/red]')
        raise SystemExit(-1)

    # Find first record with DAPHNE fragments
    # for tr_id in tr_ids:
    #     pds_ids = h5_file.get_geo_ids_for_subdetector(tr_id, detdataformats.DetID.Subdetector.kHD_PDS)
    #     # rich.print([hex(i) for i in pds_ids])
    #     if pds_ids:
    #         break

    tr_id = (tr_num, 0)
    pds_ids = h5_file.get_geo_ids_for_subdetector(tr_id, detdataformats.DetID.Subdetector.kHD_PDS)

    rich.print(f'Processing (Record Number,Sequence Number)={tr_id}')
    
    ds_dfs = []
    for gid in pds_ids:
        rich.print(f'Processing geoid 0x{gid:016x}')

        frag = h5_file.get_frag(tr_id,gid)
        frag_hdr = frag.get_header()

        n_daphne_frames = (frag.get_size()-frag_hdr.sizeof())//detdataformats.daphne.DAPHNEStreamFrame.sizeof()
        rich.print(f'Found {n_daphne_frames} DAPHNE Stream Frames.')

        frag_ts = frag.get_trigger_timestamp()
        rich.print(f'Trigger timestamp for fragment is {frag_ts}')

        dsf_0 = detdataformats.daphne.DAPHNEStreamFrame(frag.get_data())
        ddh_0 = dsf_0.get_daqheader()
        dsh_0 = dsf_0.get_header()
        ts_0 = dsf_0.get_timestamp()
        rich.print(f"First timestamp: {ts_0}")
        rich.print(
                f"Det: {ddh_0.det_id} Crate: {ddh_0.crate_id} Slot: {ddh_0.slot_id} Link: {ddh_0.link_id}"
            )
        rich.print(
                f"ch_0: {dsh_0.channel_0} ch_1: {dsh_0.channel_1} ch_2: {dsh_0.channel_2} ch_2: {dsh_0.channel_2}"
            )

        chans = [ ddh_0.crate_id*40 + getattr(dsh_0, f'channel_{i}') for i in range(chan_per_frame)]
        rich.print(chans)

        # Numpy containers
        adcs = np.empty([chan_per_frame, n_daphne_frames*samples_per_frame], dtype=np.uint16)
        ts = np.empty(n_daphne_frames*samples_per_frame, dtype=np.uint64)

        # Unpack
        for j in range(n_daphne_frames):
            dsf = detdataformats.daphne.DAPHNEStreamFrame(frag.get_data(j*detdataformats.daphne.DAPHNEStreamFrame.sizeof()))
            ts_frag = dsf.get_timestamp()
            assert(ts_frag == ts_0+j*samples_per_frame)
            for ch in range(chan_per_frame):
                for i in range(samples_per_frame):
                    ts[j*samples_per_frame+i] = ts_frag+i
                    adcs[ch][j*samples_per_frame+i] = dsf.get_adc(i,ch)

        df = pd.DataFrame(collections.OrderedDict([('ts', ts)]+[(chans[k], adcs[k]) for k in range(chan_per_frame)]))
        df = df.set_index('ts')

        ds_dfs.append(df)


    ds_df = pd.concat(ds_dfs, axis=1)
    ds_df = ds_df.dropna()
    ds_df = ds_df.reindex(sorted(ds_df.columns), axis=1)

    min_ts = ds_df.index.min()
    
    rich.print(ds_df)


    colors = plt.rcParams["axes.prop_cycle"]()


    df = ds_df.copy(False)
    df.index = df.index-min_ts
    df = df[(df.index > 61000) & (df.index < 62500)]

    with PdfPages(f'daphne_streaming_tr_{tr_num}.pdf') as pdf:

        for p in range(len(df.columns)//chan_per_frame):

            fig = plt.figure(figsize=(15., 7.5))
            gs = fig.add_gridspec(4, 5)

            # for i in df.columns[p*chan_per_frame:(p+1)*chan_per_frame]:
            for i in range(chan_per_frame):
                c = next(colors)["color"]
                ax_left = fig.add_subplot(gs[i,0:4])
                ch = df.columns[i+p*chan_per_frame]
                rich.print(f"Plotting {ch}")
                ax_left.plot(df.index, df[ch],  color=c, label=f"{ch}")
                ax_left.legend()


            # Card
            ax_right = fig.add_subplot(gs[0:4,4])
            fig.tight_layout()
            fig.show()
            pdf.savefig()  # saves the current figure into a pdf page
            # plt.close()

    if interactive:
        import IPython
        IPython.embed(colors="neutral")


if __name__ == '__main__':
    cli()