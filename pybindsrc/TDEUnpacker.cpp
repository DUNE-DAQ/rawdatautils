/**
 * @file TDE16Unpacker.cc Fast C++ -> numpy TDE16 format unpacker
 *
 * This is part of the DUNE DAQ , copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "detdataformats/tde/TDE16Frame.hpp"
#include "daqdataformats/Fragment.hpp"

#include <cstdint>
#include <pybind11/numpy.h>

namespace py = pybind11;
namespace dunedaq::rawdatautils::tde {

/**
 * @brief Gets number of TDE16Frames in a fragment
 */
uint32_t n_tde_frames(daqdataformats::Fragment const& frag){
  return (frag.get_size() - sizeof(daqdataformats::FragmentHeader)) / sizeof(detdataformats::tde::TDE16Frame);
}

/**
 * @brief Unpacks data containing TDE16Frames into a numpy array with the ADC
 * values and dimension (number of TDE16Frames, 256)
 * Warning: It doesn't check that nframes is a sensible value (can read out of bounds)
 */
py::array_t<uint16_t> np_array_adc_data(void* data, int nframes){
  py::array_t<uint16_t> ret(256 * nframes);
  auto ptr = static_cast<uint16_t*>(ret.request().ptr);
  for (size_t i=0; i<(size_t)nframes; ++i) {
    auto fr = reinterpret_cast<detdataformats::tde::TDE16Frame*>(static_cast<char*>(data) + i * sizeof(detdataformats::tde::TDE16Frame));
    for (size_t j=0; j<256; ++j)
      ptr[256 * i + j] = fr->get_adc_samples(j);
  }
  ret.resize({nframes, 256});

  return ret;
}

/**
 * @brief Unpacks data containing TDE16Frames into a numpy array with the
 * timestamps with dimension (number of TDE16Frames)
 * Warning: It doesn't check that nframes is a sensible value (can read out of bounds)
 */
py::array_t<uint64_t> np_array_timestamp_data(void* data, int nframes){
  py::array_t<uint64_t> ret(nframes);
  auto ptr = static_cast<uint64_t*>(ret.request().ptr);
  for (size_t i=0; i<(size_t)nframes; ++i) {
    auto fr = reinterpret_cast<detdataformats::tde::TDE16Frame*>(static_cast<char*>(data) + i * sizeof(detdataformats::tde::TDE16Frame));
    ptr[i] = fr->get_timestamp();
  }

  return ret;
}

/**
 * @brief Unpacks a Fragment containing TDE16Frames into a numpy array with the
 * ADC values and dimension (number of TDE16Frames in the Fragment, 256)
 */
py::array_t<uint16_t> np_array_adc(daqdataformats::Fragment const& frag){
  return np_array_adc_data(frag.get_data(), n_tde_frames(frag));
}

/**
 * @brief Unpacks the timestamps in a Fragment containing WIBFrames into a numpy
 * array with dimension (number of TDE16Frames in the Fragment)
 */
py::array_t<uint64_t> np_array_timestamp(daqdataformats::Fragment const& frag){
  return np_array_timestamp_data(frag.get_data(), n_tde_frames(frag));
}


} // namespace dunedaq::rawdatautils::tde // NOLINT
