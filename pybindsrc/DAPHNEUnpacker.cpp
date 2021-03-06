/**
 * @file DAPHNEUnpacker.cc Fast C++ -> numpy DAPHNE format unpacker
 *
 * This is part of the DUNE DAQ , copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "detdataformats/daphne/DAPHNEFrame.hpp"
#include "daqdataformats/Fragment.hpp"

#include <cstdint>
#include <pybind11/numpy.h>

namespace py = pybind11;
namespace dunedaq::rawdatautils::daphne {

/**
 * @brief Unpacks data containing DAPHNEFrames into a numpy array with the ADC
 * values and dimension (number of DAPHNEFrames, 320)
 * Warning: It doesn't check that nframes is a sensible value (can read out of bounds)
 */
py::array_t<uint16_t> np_array_adc_data(void* data, int nframes){
  py::array_t<uint16_t> ret(320 * nframes);
  auto ptr = static_cast<uint16_t*>(ret.request().ptr);
  for (size_t i=0; i<(size_t)nframes; ++i) {
    auto fr = reinterpret_cast<detdataformats::daphne::DAPHNEFrame*>(static_cast<char*>(data) + i * sizeof(detdataformats::daphne::DAPHNEFrame));
    for (size_t j=0; j<320; ++j)
      ptr[320 * i + j] = fr->get_adc(j);
  }
  ret.resize({nframes, 320});

  return ret;
}

/**
 * @brief Unpacks data containing DAPHNEFrames into a numpy array with the
 * timestamps with dimension (number of DAPHNEFrames)
 * Warning: It doesn't check that nframes is a sensible value (can read out of bounds)
 */
py::array_t<uint64_t> np_array_timestamp_data(void* data, int nframes){
  py::array_t<uint64_t> ret(nframes);
  auto ptr = static_cast<uint64_t*>(ret.request().ptr);
  for (size_t i=0; i<(size_t)nframes; ++i) {
    auto fr = reinterpret_cast<detdataformats::daphne::DAPHNEFrame*>(static_cast<char*>(data) + i * sizeof(detdataformats::daphne::DAPHNEFrame));
    ptr[i] = fr->get_timestamp();
  }

  return ret;
}

/**
 * @brief Unpacks a Fragment containing DAPHNEFrames into a numpy array with the
 * ADC values and dimension (number of DAPHNEFrames in the Fragment, 320)
 */
py::array_t<uint16_t> np_array_adc(daqdataformats::Fragment& frag){
  return np_array_adc_data(frag.get_data(), (frag.get_size() - sizeof(daqdataformats::FragmentHeader)) / sizeof(detdataformats::daphne::DAPHNEFrame));
}

/**
 * @brief Unpacks the timestamps in a Fragment containing WIBFrames into a numpy
 * array with dimension (number of DAPHNEFrames in the Fragment)
 */
py::array_t<uint64_t> np_array_timestamp(daqdataformats::Fragment& frag){
  return np_array_timestamp_data(frag.get_data(), (frag.get_size() - sizeof(daqdataformats::FragmentHeader)) / sizeof(detdataformats::daphne::DAPHNEFrame));
}

} // namespace dunedaq::rawdatautils::daphne // NOLINT
