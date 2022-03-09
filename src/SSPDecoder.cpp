/**
 * This is part of the DUNE DAQ Application Framework, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 *
 */

#include "rawdatautils/SSPDecoder.hpp"
#include "hdf5libs/HDF5RawDataFile.hpp"
#include "detdataformats/ssp/SSPTypes.hpp"

namespace dunedaq {
namespace rawdatautils {

enum
{
  TLVL_ENTER_EXIT_METHODS = 5,
  TLVL_CONFIG = 7,
  TLVL_WORK_STEPS = 10,
  TLVL_SEQNO_MAP_CONTENTS = 13,
  TLVL_FRAGMENT_HEADER_DUMP = 17
};

SSPDecoder::SSPDecoder(const std::string& file_name) {

  m_file_name = file_name; 
  
  dunedaq::hdf5libs::HDF5RawDataFile ssp_file = dunedaq::hdf5libs::HDF5RawDataFile(file_name);
  std::vector<std::string> datasets_path = ssp_file.get_all_fragment_dataset_paths();

  // Read all the fragments
  int dropped_fragments = 0;
  int fragment_counter = 0; 
  for (auto& element : datasets_path) {
    fragment_counter += 1;
    std::unique_ptr<dunedaq::daqdataformats::Fragment> frag = ssp_file.get_frag_ptr(element);
    
    if (frag->get_fragment_type() == dunedaq::daqdataformats::FragmentType::kPDSData) {
 
    TLOG_DEBUG(TLVL_ENTER_EXIT_METHODS) << "Fragment size: " << frag->get_size();  
    TLOG_DEBUG(TLVL_ENTER_EXIT_METHODS) << "Fragment header size: " << sizeof(dunedaq::daqdataformats::FragmentHeader);  

    // If the fragment is not empty (i.e. greater than the header size)
    if (frag->get_size() > sizeof(dunedaq::daqdataformats::FragmentHeader) ) {
      // Ptr to the SSP data
      auto ssp_event_header_ptr = reinterpret_cast<dunedaq::detdataformats::ssp::EventHeader*>(frag->get_data()); 
      
      // Start parsing the waveforms included in the fragment

      unsigned int nADC=(ssp_event_header_ptr->length)/2-sizeof(dunedaq::detdataformats::ssp::EventHeader)/sizeof(unsigned short);
      TLOG_DEBUG(TLVL_ENTER_EXIT_METHODS) << "Number of ADC values: " << nADC;

      // Decoding SSP data 
      unsigned short* adcPointer=reinterpret_cast<unsigned short*>(frag->get_data());
      adcPointer += sizeof(dunedaq::detdataformats::ssp::EventHeader)/sizeof(unsigned short);
      unsigned short* adc; 

      std::vector<int> ssp_frames; 
      for (size_t idata=0; idata < nADC; idata++) { 
        adc = adcPointer + idata;
        ssp_frames.push_back(*adc);  
      }    
      adcPointer += nADC;    
    
      // Store SSP data
      m_ssp_frames.push_back( ssp_frames );

    } else { // payload is empty, dropping fragment 
      dropped_fragments += 1;

    }  
  } else {
   std::cout << "Skipping: not PD fragment type" << std::endl;
  }

  }

}

// Property getter functions
// 



std::vector<std::vector<int>> SSPDecoder::get_ssp_frames() {
  return m_ssp_frames;
}


} // rawdatautils
} // dunedaq
