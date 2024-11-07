#!/bin/bash

# Kurt Biery, October 2021 - November 2023

# decode arguments
if [ "$1" == "--help" ] || [ "$1" == "-h" ] || [ "$1" == "-?" ]; then
  echo "Usage: $0 [data_disk_number]"
  echo "Where data_disk_number defaults to zero."
  echo "Example: \"$0 3\" looks for new HDF5 files on /data3"
  exit 1
fi

data_disk_number=0
if [ $# -gt 0 ]; then
  data_disk_number=$1
fi

# assign parameter values
dataDirs="/data${data_disk_number} /data${data_disk_number}/transfer_test /data${data_disk_number}/kurtMetadataTests"  # may be a space-separate list
minDataFileAgeMinutes=0
maxDataFileAgeMinutes=14400
filenamePrefixList=( "np04hd_raw" "np04hd_tp" "np02vd_raw" "np02vd_tp" "np04hdcoldbox_raw" "np04hdcoldbox_tp" "np02vdcoldbox_raw" "np02vdcoldbox_tp" )

lockFileDir="/tmp"
lockFileName=".mdFileCronjob_data${data_disk_number}.lock"
staleLockFileTimeoutMinutes=30

setupScriptPath="/nfs/home/np04daq/.cron/setupDuneDAQ"
ourHDF5DumpScript="print_trnumbers_for_json_metadata.py"
scratchFile="/tmp/metadata_scratch_$$.out"
requestedJSONFileOutputDir="."  # an empty or "." value puts JSON files in the same dirs as the ROOT files
logPath="/log/metadataFileCreator/createMDFile_data${data_disk_number}.log"
extraFieldCommand="python /nfs/home/np04daq/.cron/insert_extra_fields.py"
let debugLevel=2  # only zero, one, and two are useful, at the moment; two is for performance tracing
versionOfThisScript="v2.7.4"

# define a function to log messages
function logMessage() {
    local msgText=$1
    local pid=$$
    local timestamp=`date '+%Y-%m-%d %H:%M:%S'`
    if [[ "$logPath" != "" ]]; then
        echo "${timestamp} (${pid}) ${msgText}" >> $logPath
    else
        echo "${timestamp} (${pid}) ${msgText}"
    fi
}

# initialization
logMessage "Starting $0 ${versionOfThisScript} for ${dataDirs}."
cd $lockFileDir

# check if there is an instance of this script already running,
# as evidenced by the presence of a lock file.  To prevent a stale
# lock file from causing problems, we only look for lock files that
# are relatively newer than the "stale lock file timeout".
existingLockFile=`find ${lockFileDir} -name ${lockFileName} -mmin -${staleLockFileTimeoutMinutes} -print 2>/dev/null`
if [[ "${existingLockFile}" != "" ]]; then
    logMessage "Found lock file (${lockFileDir}/${lockFileName}); exiting early to prevent duplicate jobs."
    exit 1
fi
existingLockFile=`ls -alF ${lockFileDir}/${lockFileName} 2>/dev/null`
if [[ "${existingLockFile}" != "" ]]; then
    logMessage "Ignoring stale lock file (${existingLockFile})."
fi
touch ${lockFileDir}/${lockFileName}

dunedaqSetupAttempted="no"
processed_one_or_more_files="yes"
while [[ "${processed_one_or_more_files}" != "" ]]; do
    processed_one_or_more_files=""

    # 29-Oct-2021, KAB: added loop over filename prefixes
    for filenamePrefix in ${filenamePrefixList[@]}; do

	dataFileNamePattern="${filenamePrefix}_run??????_*.hdf5"
	if [[ "$filenamePrefix" == "np02_bde_coldbox" ]] || [[ "$filenamePrefix" == "vd_coldbox_bottom" ]]; then
            offlineRunTypeReallyOpEnv="vd-coldbox-bottom"
	elif [[ "$filenamePrefix" == "np04_hd" ]] || [[ "$filenamePrefix" == "np04hd_raw" ]] || [[ "$filenamePrefix" == "np04hd_tp" ]]; then
            offlineRunTypeReallyOpEnv="hd-protodune"
	elif [[ "$filenamePrefix" == "np02_vd" ]] || [[ "$filenamePrefix" == "np02vd_raw" ]] || [[ "$filenamePrefix" == "np02vd_tp" ]]; then
            offlineRunTypeReallyOpEnv="vd-protodune"
	elif [[ "$filenamePrefix" == "np04_coldbox" ]] || [[ "$filenamePrefix" == "np04hdcoldbox_raw" ]] || [[ "$filenamePrefix" == "np04hdcoldbox_tp" ]]; then
            offlineRunTypeReallyOpEnv="hd-coldbox"
	elif [[ "$filenamePrefix" == "np02vdcoldbox_raw" ]] || [[ "$filenamePrefix" == "np02vdcoldbox_tp" ]]; then
            offlineRunTypeReallyOpEnv="vd-coldbox"
	elif [[ "$filenamePrefix" == "np02_pds" ]]; then
            offlineRunTypeReallyOpEnv="vd-protodune-pds"
	else
            offlineRunTypeReallyOpEnv=${filenamePrefix}
	fi

	if [[ $debugLevel -ge 1 ]]; then
            logMessage "Searching for filenames like \"${dataFileNamePattern}\" in \"${dataDirs}\"."
            logMessage "Offline run_type is \"${offlineRunTypeReallyOpEnv}\"."
	fi

	# loop over all of the files that are found in the requested data directories
	let processed_file_count=0
	for volatileFileName in $(find ${dataDirs} -maxdepth 1 -name "${dataFileNamePattern}" -type f -mmin +${minDataFileAgeMinutes} -mmin -${maxDataFileAgeMinutes} -print 2>/dev/null | sort -r); do

	    # we assume that we need a periodic touch of the lock file
	    touch ${lockFileDir}/${lockFileName}

	    # determine the base filename for the current raw data file
	    baseFileName=`basename $volatileFileName`
	    fullFileName=${volatileFileName}

	    # determine the JSON file output dir, if not explicitly specified
	    jsonFileOutputDir=${requestedJSONFileOutputDir}
	    if [[ "$requestedJSONFileOutputDir" == "" ]] || [[ "$requestedJSONFileOutputDir" == "." ]]; then
		jsonFileOutputDir=`dirname $volatileFileName`
	    fi

	    # only do the work if the metadata file doesn't already exist
	    jsonFileName="${jsonFileOutputDir}/${baseFileName}.json"
	    if [[ ! -e "${jsonFileName}" ]] && [[ ! -e "${jsonFileName}.copied" ]]; then
		workingJSONFileName="${jsonFileName}.tmp"

		# if needed, setup dunedaq, etc. so that we can look inside the file
		if [[ "${dunedaqSetupAttempted}" == "no" ]]; then
		    if [[ $debugLevel -ge 1 ]]; then logMessage "Setting up dunedaq"; fi
		    currentDir=`pwd`
		    tmpDir=`dirname ${setupScriptPath}`
		    cd $tmpDir
		    source `basename ${setupScriptPath}` >/dev/null
		    cd $currentDir
		    hdf5DumpFullPath=`eval which ${ourHDF5DumpScript} 2>/dev/null`
		    if [[ "$hdf5DumpFullPath" == "" ]]; then
			logMessage "ERROR: The ${ourHDF5DumpScript} script was not found!"
			rm -f ${lockFileDir}/${lockFileName}
			exit 3
		    fi
		    dunedaqSetupAttempted="yes"
		fi

		# pull the run number out of the filename
		runNumber=`echo ${baseFileName} | sed 's/\(.*_run\)\([[:digit:]]\+\)\(_.*\)/\2/'`
		# strip off leading zeroes
		runNumber=`echo ${runNumber} | sed 's/^0*//'`
		# convert it to a number (may be needed later if we do range comparisons)
		let runNumber=$runNumber+0

		# if we don't have a copy of the our HDF5 dumper utility, do what we can...
		if [[ "$hdf5DumpFullPath" == "" ]]; then
		    logMessage "Creating ${jsonFileName} without using any extra tools."

		    echo "{" > ${workingJSONFileName}
		    echo "  \"data_stream\": \"test\"," >> ${workingJSONFileName}
		    if [[ "`echo ${filenamePrefix} | grep '_tp$'`" != "" ]]; then
			echo "  \"data_tier\": \"trigprim\"," >> ${workingJSONFileName}
		    else
			echo "  \"data_tier\": \"raw\"," >> ${workingJSONFileName}
		    fi
		    echo "  \"file_format\": \"hdf5\"," >> ${workingJSONFileName}
		    echo "  \"file_name\": \"${baseFileName}\"," >> ${workingJSONFileName}
		    echo "  \"file_type\": \"detector\"," >> ${workingJSONFileName}
		    if [[ "`echo ${fullFileName} | grep 'transfer_test'`" != "" ]]; then
			echo "  \"DUNE.campaign\": \"DressRehearsalNov2023\"," >> ${workingJSONFileName}
		    fi
		    echo "  \"runs\": [[${run_number},1,\"${offlineRunTypeReallyOpEnv}\"]]" >> ${workingJSONFileName}
		    echo "}" >> ${workingJSONFileName}
		else
		    if [[ $debugLevel -ge 1 ]]; then
			logMessage "Creating ${jsonFileName} from ${fullFileName} using ${hdf5DumpFullPath} and local modifications."
		    else
			logMessage "Creating ${jsonFileName} using ${ourHDF5DumpScript} and local modifications."
		    fi

		    if [[ $debugLevel -ge 2 ]]; then logMessage "Before the TR (event) numbers are determined"; fi
		    rm -f ${scratchFile}
		    #${hdf5DumpFullPath} -H ${fullFileName} | grep GROUP | grep TriggerRecord | sed 's/.*TriggerRecord//' | sed 's/\".*//' > ${scratchFile} 2>/dev/null
		    ${hdf5DumpFullPath} ${fullFileName} > ${scratchFile} 2>/dev/null
		    if [[ $debugLevel -ge 2 ]]; then logMessage "After the TR (event) numbers are determined"; fi

		    # if the dumper utility worked, process the results
		    if [[ $? == 0 ]]; then
			event_list=`cat ${scratchFile}`
			#logMessage "event list is ${event_list}"
			rm -f ${scratchFile}

			IFS=$'\n' trimmed_list1=($(sed 's/0$/TLZ/' <<<"${event_list[*]}"))
			IFS=$'\n' trimmed_list2=($(sed 's/^0+//' <<<"${trimmed_list1[*]}"))
			IFS=$'\n' trimmed_list3=($(sed 's/TLZ$/0/' <<<"${trimmed_list2[*]}"))
			IFS=$'\n' trimmed_list4=($(sed 's/\..*//' <<<"${trimmed_list3[*]}"))
			IFS=$'\n' sorted_list=($(sort -u -n <<<"${trimmed_list4[*]}"))
			IFS=$'\n' event_count=($(wc -l <<<"${sorted_list[*]}"))
			unset IFS

			min_event_num=${sorted_list[0]}
			max_event_num=${sorted_list[-1]}

			formatted_event_list=`echo "${sorted_list[*]}" | sed 's/ /,/g'`
			if [[ $debugLevel -ge 2 ]]; then logMessage "Midway through processing the TR (event) numbers"; fi

			echo "{" > ${workingJSONFileName}
			echo "  \"data_stream\": \"test\"," >> ${workingJSONFileName}
			if [[ "`echo ${filenamePrefix} | grep '_tp$'`" != "" ]]; then
			    echo "  \"data_tier\": \"trigprim\"," >> ${workingJSONFileName}
			else
			    echo "  \"data_tier\": \"raw\"," >> ${workingJSONFileName}
			fi
			echo "  \"event_count\": ${event_count}," >> ${workingJSONFileName}
			echo "  \"events\": [${formatted_event_list}]," >> ${workingJSONFileName}
			echo "  \"file_format\": \"hdf5\"," >> ${workingJSONFileName}
			echo "  \"file_name\": \"${baseFileName}\"," >> ${workingJSONFileName}
			echo "  \"file_type\": \"detector\"," >> ${workingJSONFileName}
			if [[ "`echo ${fullFileName} | grep 'transfer_test'`" != "" ]]; then
			    echo "  \"DUNE.campaign\": \"DressRehearsalNov2023\"," >> ${workingJSONFileName}
			fi
			echo "  \"first_event\": ${min_event_num}," >> ${workingJSONFileName}
			echo "  \"last_event\": ${max_event_num}," >> ${workingJSONFileName}
			echo "  \"runs\": [[${runNumber},1,\"${offlineRunTypeReallyOpEnv}\"]]" >> ${workingJSONFileName}
			echo "}" >> ${workingJSONFileName}
		    else
			logMessage "ERROR: unable to run ${ourHDF5DumpScript} on \"${fullFileName}\"."
			rm ${workingJSONFileName}
		    fi
		    if [[ $debugLevel -ge 2 ]]; then logMessage "After the TR (event) numbers are processed"; fi
		fi

		if [[ $debugLevel -ge 2 ]]; then logMessage "Before extra field(s) are added"; fi
		if [[ -e "${workingJSONFileName}" ]]; then
		    ${extraFieldCommand} ${fullFileName} ${workingJSONFileName} >/dev/null 2>/dev/null
		    mv ${workingJSONFileName} ${jsonFileName}
		fi
		if [[ $debugLevel -ge 2 ]]; then logMessage "After extra field(s) are added"; fi

		let processed_file_count=$processed_file_count+1
		processed_one_or_more_files="yes"
	    fi

	    if [[ $processed_file_count -ge 16 ]]; then break; fi
	done # loop over the files that have been found

    done # loop over filename prefixes

done # loop until there are no files to be processed

# cleanup
rm -f ${lockFileDir}/${lockFileName}
logMessage "Done with $0 for ${dataDirs}."
