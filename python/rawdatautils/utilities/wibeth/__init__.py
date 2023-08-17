import daqdataformats
import detdataformats

def print_header(wib_frame,prefix="\t"):
    dheader = wib_frame.get_daqheader()
    print(f'{prefix}DAQVersion: {dheader.version}')
    print(f'{prefix}Detector ID: {dheader.det_id}')
    print(f'{prefix}(Crate,Slot,Link): ({dheader.crate_id},{dheader.slot_id},{dheader.stream_id})')
    print(f'{prefix}Timestamp: {dheader.timestamp}')
    
    wheader = wib_frame.get_wibheader()
    print(f'{prefix}WIBVersion: {wheader.version}')
    print(f'{prefix}Channel (FEMB,COLDATA): {wheader.channel} ({(wheader.channel>>1)&0x3},{wheader.channel&0x1})')
    print(f'{prefix}Colddata Timestamp (0,1): ({wheader.colddata_timestamp_0},{wheader.colddata_timestamp_1})')
    print(f'{prefix}CRC Error: {wheader.crc_err}')
    print(f'{prefix}Link Valid: {wheader.link_valid}')
    print(f'{prefix}LOL: {wheader.lol}')
    print(f'{prefix}WIB_SYNC: {wheader.wib_sync}')
    print(f'{prefix}FEMB_SYNC: {wheader.femb_sync}')
    print(f'{prefix}Pulser: {wheader.pulser}')
    print(f'{prefix}Calibration: {wheader.calibration}')
    print(f'{prefix}Ready: {wheader.ready}')
    print(f'{prefix}Context: {wheader.context}')
