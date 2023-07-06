/**
 * @file DAPHNEUnpacker.cc Fast C++ -> numpy DAPHNE format unpacker
 *
 * This is part of the DUNE DAQ , copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "fddetdataformats/DAPHNEFrame.hpp"
#include "fddetdataformats/DAPHNEStreamFrame.hpp"
#include "daqdataformats/Fragment.hpp"

#include <cstdint>
#include <pybind11/numpy.h>

namespace py = pybind11;
namespace dunedaq::rawdatautils::daphne {

/**                                                                                                                                                                                                                
 * @brief Gets number of DAPHNEFrames in a fragment                                                                                                                                                                
 */
uint32_t get_n_frames(daqdataformats::Fragment const& frag){
  return (frag.get_size() - sizeof(daqdataformats::FragmentHeader)) / sizeof(fddetdataformats::DAPHNEFrame);
}

/**                                                                                                                                                                                                                
 * @brief Gets number of DAPHNEStreamFrames in a fragment                                                                                                                                                          
 */
uint32_t get_n_frames_stream(daqdataformats::Fragment const& frag){
  return (frag.get_size() - sizeof(daqdataformats::FragmentHeader)) / sizeof(fddetdataformats::DAPHNEStreamFrame);
}


/**
 * @brief Unpacks data containing DAPHNEFrames into a numpy array with the ADC
 * values and dimension (number of DAPHNEFrames, channels_per_daphne)
 * Warning: It doesn't check that nframes is a sensible value (can read out of bounds)
 */
py::array_t<uint16_t> np_array_adc_data(void* data, int nframes){
  
  const auto adcs_per_channel     = fddetdataformats::DAPHNEFrame::s_num_adcs;

  py::array_t<uint16_t> ret(nframes * adcs_per_channel);
  auto ptr = static_cast<uint16_t*>(ret.request().ptr);
  for (size_t i=0; i<(size_t)nframes; ++i) {
    auto fr = reinterpret_cast<fddetdataformats::DAPHNEFrame*>(static_cast<char*>(data) + i * sizeof(fddetdataformats::DAPHNEFrame));
    //auto fr = reinterpret_cast<fddetdataformats::DAPHNEFrame*>(static_cast<char*>(data) + i * sizeof(fddetdataformats::DAPHNEFrame));
    for (size_t j=0; j<adcs_per_channel; ++j) {
      ptr[i*adcs_per_channel + j] = fr->get_adc(j);
    }
    //for (size_t j=0; j<channels_per_daphne; ++j)
    
  }
  //ret.resize({nframes, channels_per_daphne});

  return ret;
}

/**                                                                                                                                                                                                                
 * @brief Unpacks data containing DAPHNEStreamFrames into a numpy array with the ADC                                                                                                                               
 * values and dimension (number of DAPHNEStreamFrames * adcs_per_channel (64), channels_per_frame (4))                                                                                                             
 * Warning: It doesn't check that nframes is a sensible value (can read out of bounds)                                                                                                                             
 */
py::array_t<uint16_t> np_array_adc_stream_data(void* data, int nframes){

  const auto channels_per_daphne  = fddetdataformats::DAPHNEStreamFrame::s_channels_per_frame;
  const auto adcs_per_channel     = fddetdataformats::DAPHNEStreamFrame::s_adcs_per_channel;

  py::array_t<uint16_t> ret(channels_per_daphne * nframes * adcs_per_channel);
  auto ptr = static_cast<uint16_t*>(ret.request().ptr);
  for (size_t i=0; i<(size_t)nframes; ++i) {
    auto fr = reinterpret_cast<fddetdataformats::DAPHNEStreamFrame*>(static_cast<char*>(data) + i * sizeof(fddetdataformats::DAPHNEStreamFrame));
    for (size_t j=0; j<adcs_per_channel; ++j)
      for (size_t k=0; k<channels_per_daphne; ++k)
        ptr[channels_per_daphne * (adcs_per_channel * i + j) + k] = fr->get_adc(j,k);
  }
  ret.resize({nframes*adcs_per_channel, channels_per_daphne});

  return ret;
}


/**
 * @brief Unpacks data containing DAPHNEFrames into a numpy array with the
 * timestamps with dimension (number of DAPHNEFrames)
 * Warning: It doesn't check that nframes is a sensible value (can read out of bounds)
 */

py::array_t<uint64_t> np_array_timestamp_data(void* data, int nframes){

  const auto adcs_per_channel     = fddetdataformats::DAPHNEFrame::s_num_adcs;

  py::array_t<uint64_t> ret(nframes);
  auto ptr = static_cast<uint64_t*>(ret.request().ptr);
  for (size_t i=0; i<(size_t)nframes; ++i) {
    auto fr = reinterpret_cast<fddetdataformats::DAPHNEFrame*>(static_cast<char*>(data) + i * sizeof(fddetdataformats::DAPHNEFrame));
    ptr[i] = fr->get_timestamp();
  }

  return ret;
}

/**                                                                                                                                                                                                                
 * @brief Unpacks data containing DAPHNEStreamFrames into a numpy array with the                                                                                                                                   
 * timestamps with dimension (number of DAPHNEStreamFrames)                                                                                                                                                        
 * Warning: It doesn't check that nframes is a sensible value (can read out of bounds)                                                                                                                             
 */
py::array_t<uint64_t> np_array_timestamp_stream_data(void* data, int nframes) {

  const auto adcs_per_channel = fddetdataformats::DAPHNEStreamFrame::s_adcs_per_channel;
  const size_t ticks_per_adc = 1;

  py::array_t<uint64_t> ret(nframes*adcs_per_channel);

  auto ptr = static_cast<uint64_t*>(ret.request().ptr);
  for (size_t i=0; i<(size_t)nframes; ++i) {
    auto fr = reinterpret_cast<fddetdataformats::DAPHNEStreamFrame*>(static_cast<char*>(data) + i * sizeof(fddetdataformats::DAPHNEStreamFrame));
    for (size_t j=0; j<adcs_per_channel; ++j)
      ptr[i*adcs_per_channel+j] = fr->get_timestamp()+j*ticks_per_adc;
  }

  return ret;
}


/**
 * @brief Unpacks a Fragment containing DAPHNEFrames into a numpy array with the
 * ADC values and dimension (number of DAPHNEFrames in the Fragment, 320)
 */
py::array_t<uint16_t> np_array_adc(daqdataformats::Fragment& frag){
  return np_array_adc_data(frag.get_data(), (frag.get_size() - sizeof(daqdataformats::FragmentHeader)) / sizeof(fddetdataformats::DAPHNEFrame));
}

/**                                                                                                                                                                                                                
 * @brief Unpacks a Fragment containing DAPHNEStreamFrames into a numpy array with the                                                                                                                             
 * ADC values and dimension (number of DAPHNEStreamFrames in the Fragment, 4)                                                                                                                                      
 */
py::array_t<uint16_t> np_array_adc_stream(daqdataformats::Fragment& frag){
  return np_array_adc_stream_data(frag.get_data(), (frag.get_size() - sizeof(daqdataformats::FragmentHeader)) / sizeof(fddetdataformats::DAPHNEStreamFrame));
}


/**
 * @brief Unpacks the timestamps in a Fragment containing WIBFrames into a numpy
 * array with dimension (number of DAPHNEFrames in the Fragment)
 */
py::array_t<uint64_t> np_array_timestamp(daqdataformats::Fragment& frag){
  return np_array_timestamp_data(frag.get_data(), (frag.get_size() - sizeof(daqdataformats::FragmentHeader)) / sizeof(fddetdataformats::DAPHNEFrame));
}

/**                                                                                                                                                                                                                
 * @brief Unpacks the timestamps in a Fragment containing DAPHNEStreamFrames into a numpy                                                                                                                          
 * array with dimension (number of DAPHNEStreamFrames in the Fragment)                                                                                                                                             
 */
py::array_t<uint64_t> np_array_timestamp_stream(daqdataformats::Fragment& frag){
  return np_array_timestamp_stream_data(frag.get_data(), (frag.get_size() - sizeof(daqdataformats::FragmentHeader)) / sizeof(fddetdataformats::DAPHNEStreamFrame));
}


} // namespace dunedaq::rawdatautils::daphne // NOLINT
