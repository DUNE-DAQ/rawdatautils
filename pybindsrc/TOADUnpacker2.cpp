/**
 *  * @file WIB2Unpacker.cc Fast C++ -> numpy WIB2 format unpacker
 *   *
 *    * This is part of the DUNE DAQ , copyright 2020.
 *     * Licensing/copyright details are in the COPYING file that you should have
 *      * received with this code.
 *       */

#include "detdataformats/toad/TOADFrameOverlay.hpp"
#include "daqdataformats/Fragment.hpp"

#include <cstdint>
#include <pybind11/numpy.h>

namespace py = pybind11;
namespace dunedaq::rawdatautils::toad {

/**
 *  * @brief Unpacks data containing WIB2Frames into a numpy array with the ADC
 *   * values and dimension (number of WIB2Frames, 256)
 *    * Warning: It doesn't check that nframes is a sensible value (can read out of bounds)
 *     */
py::array_t<uint16_t> np_array_adc_data(void* data, int nsamples){
  py::array_t<uint16_t> ret(nsamples);
  auto ptr = static_cast<uint16_t*>(ret.request().ptr);
  auto fr = reinterpret_cast<detdataformats::toad::TOADFrameOverlay*>(static_cast<char*>(data));
    for (size_t j=0; j<nsamples; j++){
      ptr[j] = fr->get_samples(j);
    }
  return ret;
}

/**
 *  * @brief Unpacks a Fragment containing WIB2Frames into a numpy array with the
 *   * ADC values and dimension (number of WIB2Frames in the Fragment, 256)
 *    */
py::array_t<uint16_t> np_array_adc(void* data, int nsamples, int index){
  return np_array_adc_data(data, nsamples);
}

} // namespace dunedaq::rawdatautils::wib2 // NOLINT
