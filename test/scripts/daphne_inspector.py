#!/usr/bin/env python

from hdf5libs import HDF5RawDataFile

import daqdataformats
import detdataformats.daphne
import detchannelmaps

import click
import rich
import numpy as np
from matplotlib import pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

samples_per_frame = 64
chan_per_frame = 4

@click.command()
@click.option('-i', '--interactive', is_flag=True, default=False)
@click.argument('filename', type=click.Path(exists=True))
def cli(interactive, filename):
    
    h5_file = HDF5RawDataFile(filename)

    tr_ids = h5_file.get_all_trigger_record_ids()

    rich.print(tr_ids)

    # Find first record with DAPHNE fragments
    for tr_id in tr_ids:
        pds_ids = h5_file.get_geo_ids_for_subdetector(tr_id, detdataformats.DetID.Subdetector.kHD_PDS)
        rich.print([hex(i) for i in pds_ids])
        if pds_ids:
            break

    rich.print(f'Processing (Record Number,Sequence Number)={tr_id}')
    
    gid = list(pds_ids)[0]
    rich.print(f'Processing geoid 0x{gid:016x}')

    frag = h5_file.get_frag(tr_id,gid)
    frag_hdr = frag.get_header()

    n_daphne_frames = (frag.get_size()-frag_hdr.sizeof())//detdataformats.daphne.DAPHNEStreamFrame.sizeof()
    rich.print(f'\tFound {n_daphne_frames} DAPHNE Stream Frames.')

    frag_ts = frag.get_trigger_timestamp()
    rich.print(f'\tTrigger timestamp for fragment is {frag_ts}')

    dsf_0 = detdataformats.daphne.DAPHNEStreamFrame(frag.get_data())
    ddh_0 = dsf_0.get_daqheader()
    dsh_0 = dsf_0.get_header()
    rich.print(
            f"Det: {ddh_0.det_id} Crate: {ddh_0.crate_id} Slot: {ddh_0.slot_id} Link: {ddh_0.link_id}"
        )
    rich.print(
            f"ch-0: {dsh_0.channel_0} ch-1: {dsh_0.channel_1} ch-2: {dsh_0.channel_2} ch-2: {dsh_0.channel_2}"
        )

    channel_ids = []


    ts_0 = dsf_0.get_timestamp()

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


    colors = plt.rcParams["axes.prop_cycle"]()

    with PdfPages('multipage_pdf.pdf') as pdf:

        fig = plt.figure(figsize=(15., 7.5))
        gs = fig.add_gridspec(4, 5)

        for i in range(4):
            c = next(colors)["color"]
            ax_left = fig.add_subplot(gs[i,0:4])
            ax_left.plot(ts, adcs[i],  color=c, label=f"{getattr(dsh_0, 'channel_'+str(i))}")
            # ax_left.plot(ts, adcs[1], label=f"{dsh_0.channel_1}")
            # ax_left.plot(ts, adcs[2], label=f"{dsh_0.channel_2}")
            # ax_left.plot(ts, adcs[3], label=f"{dsh_0.channel_3}")
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