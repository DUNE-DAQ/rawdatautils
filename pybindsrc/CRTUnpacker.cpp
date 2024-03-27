/**
 * @file CRTUnpacker.cc Fast C++ -> numpy CRT (fixed size) format unpacker
 *
 * This is part of the DUNE DAQ , copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "fddetdataformats/CRTFrame.hpp"
#include "daqdataformats/Fragment.hpp"

#include <cstdint>
#include <pybind11/numpy.h>

namespace py = pybind11;
namespace dunedaq::rawdatautils::crt {

/**                                                                                                                                                                                                                
 * @brief Gets number of CRTFrames in a fragment                                                                                                                                                                
 */
uint32_t get_n_frames(daqdataformats::Fragment const& frag){
  return (frag.get_size() - sizeof(daqdataformats::FragmentHeader)) / sizeof(fddetdataformats::CRTFrame);
}


/**                                                                                                                                                                         \
                                                                                                                                                                             
 * @brief Unpacks module numbers for CRTFrames into a numpy array with dimensions                                                                                        
 * (nframes)                                                                                                                                                                \
                                                                                                                                                                             
 */
py::array_t<uint16_t> np_array_modules_data(void* data, int nframes){

  py::array_t<uint16_t> modules(nframes);
  auto ptr = static_cast<uint16_t*>(modules.request().ptr);

  for (size_t i=0; i<(size_t)nframes; ++i) {
    auto fr = reinterpret_cast<fddetdataformats::CRTFrame*>(static_cast<char*>(data) + i * sizeof(fddetdataformats::CRTFrame));
    ptr[i] = fr->get_module();
  }

  return modules;
}

/**                                                                                                                                                                         \
                                                                                                                                                                             
 * @brief Unpacks module numbers for Fragment that contains CRTFrames into a numpy array with dimensions                                                                \
                                                                                                                                                                             
 */
py::array_t<uint16_t> np_array_modules(daqdataformats::Fragment& frag){
  return np_array_modules_data(frag.get_data(), (frag.get_size() - sizeof(daqdataformats::FragmentHeader)) / sizeof(fddetdataformats::CRTFrame));
}


/**
 * @brief Unpacks data containing CRTFrames into a numpy array with the ADC
 * values and dimension (number of CRTFrames, adcs_per_module (=64))
 * Warning: It doesn't check that nframes is a sensible value (can read out of bounds)
 */
py::array_t<int16_t> np_array_adc_data(void* data, int nframes){
  
  const auto adcs_per_module     = fddetdataformats::CRTFrame::s_num_adcs;

  py::array_t<int16_t> ret(nframes * adcs_per_module);
  auto ptr = static_cast<int16_t*>(ret.request().ptr);
  for (size_t i=0; i<(size_t)nframes; ++i) {
    auto fr = reinterpret_cast<fddetdataformats::CRTFrame*>(static_cast<char*>(data) + i * sizeof(fddetdataformats::CRTFrame));
    for (size_t j=0; j<adcs_per_module; ++j) {
      ptr[i*adcs_per_module + j] = fr->get_adc(j);
    }
  }
  ret.resize({nframes, adcs_per_module});

  return ret;
}

/**
 * @brief Unpacks data containing CRTFrames into a numpy array with the channel
 * values and dimension (number of CRTFrames, adcs_per_module (=64))
 * Warning: It doesn't check that nframes is a sensible value (can read out of bounds)
 */
py::array_t<uint8_t> np_array_channel_data(void* data, int nframes){
  
  const auto adcs_per_module     = fddetdataformats::CRTFrame::s_num_adcs;

  py::array_t<uint8_t> ret(nframes * adcs_per_module);
  auto ptr = static_cast<uint8_t*>(ret.request().ptr);
  for (size_t i=0; i<(size_t)nframes; ++i) {
    auto fr = reinterpret_cast<fddetdataformats::CRTFrame*>(static_cast<char*>(data) + i * sizeof(fddetdataformats::CRTFrame));
    for (size_t j=0; j<adcs_per_module; ++j) {
      ptr[i*adcs_per_module + j] = fr->get_channel(j);
    }
  }
  ret.resize({nframes, adcs_per_module});

  return ret;
}

/**
 * @brief Unpacks data containing CRTFrames into a numpy array with the
 * timestamps with dimension (number of CRTFrames)
 * Warning: It doesn't check that nframes is a sensible value (can read out of bounds)
 */

py::array_t<uint64_t> np_array_timestamp_data(void* data, int nframes){

  //const auto adcs_per_module     = fddetdataformats::CRTFrame::s_num_adcs;

  py::array_t<uint64_t> ret(nframes);
  auto ptr = static_cast<uint64_t*>(ret.request().ptr);
  for (size_t i=0; i<(size_t)nframes; ++i) {
    auto fr = reinterpret_cast<fddetdataformats::CRTFrame*>(static_cast<char*>(data) + i * sizeof(fddetdataformats::CRTFrame));
    ptr[i] = fr->get_timestamp();
  }
  
  return ret;
}


/**
 * @brief Unpacks a Fragment containing CRTFrames into a numpy array with the
 * ADC values and dimension (number of CRTFrames in the Fragment, 64)
 */
py::array_t<int16_t> np_array_adc(daqdataformats::Fragment& frag){
  return np_array_adc_data(frag.get_data(), (frag.get_size() - sizeof(daqdataformats::FragmentHeader)) / sizeof(fddetdataformats::CRTFrame));
}

/**
 * @brief Unpacks a Fragment containing CRTFrames into a numpy array with the
 * channel values and dimension (number of CRTFrames in the Fragment, 64)
 */
py::array_t<uint8_t> np_array_channel(daqdataformats::Fragment& frag){
  return np_array_channel_data(frag.get_data(), (frag.get_size() - sizeof(daqdataformats::FragmentHeader)) / sizeof(fddetdataformats::CRTFrame));
}


/**
 * @brief Unpacks the timestamps in a Fragment containing WIBFrames into a numpy
 * array with dimension (number of CRTFrames in the Fragment)
 */
py::array_t<uint64_t> np_array_timestamp(daqdataformats::Fragment& frag){
  return np_array_timestamp_data(frag.get_data(), (frag.get_size() - sizeof(daqdataformats::FragmentHeader)) / sizeof(fddetdataformats::CRTFrame));
}

} // namespace dunedaq::rawdatautils::crt // NOLINT
