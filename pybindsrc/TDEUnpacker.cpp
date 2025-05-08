/**
 * @file TDEEthUnpacker.cc Fast C++ -> numpy tDEEth format unpacker
 *
 * This is part of the DUNE DAQ , copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "fddetdataformats/TDEEthFrame.hpp"
#include "daqdataformats/Fragment.hpp"

#include <cstdint>
#include <pybind11/numpy.h>

namespace py = pybind11;
namespace dunedaq::rawdatautils::tde {

/**
 * @brief Gets number of TDEEthFrames in a fragment
 */
uint32_t get_n_frames(daqdataformats::Fragment const& frag){
  return (frag.get_size() - sizeof(daqdataformats::FragmentHeader)) / sizeof(fddetdataformats::TDEEthFrame);
}

/**
 * @brief Unpacks data containing TDEEthFrames into a numpy array with the ADC
 * values and dimension (number of TDEEthFrames, 64)
 * Warning: It doesn't check that n_frames is a sensible value (can read out of bounds)
 */
py::array_t<uint16_t> np_array_adc_data(void* data, uint32_t n_frames){

    uint32_t n_ch = fddetdataformats::TDEEthFrame::s_num_channels;
    uint32_t n_smpl = fddetdataformats::TDEEthFrame::s_time_samples_per_frame;

    py::array_t<uint16_t> result(n_ch * n_smpl * n_frames);

    py::buffer_info buf_res = result.request();

    auto ptr_res = static_cast<uint16_t*>(buf_res.ptr);

    for (size_t i=0; i<n_frames; ++i) {

        auto fr = reinterpret_cast<fddetdataformats::TDEEthFrame*>(
            static_cast<char*>(data) + i * sizeof(fddetdataformats::TDEEthFrame)
        );

        for (size_t j=0; j<n_smpl; ++j){
            for (size_t k=0; k<n_ch; ++k){
                ptr_res[(n_smpl*n_ch) * i + n_ch*j + k] = fr->get_adc(k, j);
            }
        }
    }
    result.resize({n_frames*n_smpl, n_ch});

    return result;
}

/**
 * @brief Unpacks data containing TDEEthFrames into a numpy array with the
 * timestamps with dimension (number of TDEEthFrames)
 * Warning: It doesn't check that n_frames is a sensible value (can read out of bounds)
 */
py::array_t<long double> np_array_timestamp_data(void* data, uint32_t n_frames){

    uint32_t n_smpl = fddetdataformats::TDEEthFrame::s_time_samples_per_frame;

    py::array_t<long double> result(n_smpl*n_frames);

    auto ptr = static_cast<long double*>(result.request().ptr);

    for (size_t i=0; i<n_frames; ++i) {
        auto fr = reinterpret_cast<fddetdataformats::TDEEthFrame*>(
            static_cast<char*>(data) + i * sizeof(fddetdataformats::TDEEthFrame)
        );
        long double ts_0 = fr->get_timestamp();
        for(size_t j=0; j<n_smpl; ++j )
            ptr[i*n_smpl+j] = ts_0+31.25*j;
    }

    return result;
}

/**
 * @brief Unpacks a Fragment containing TDEEthFrames into a numpy array with the
 * ADC values and dimension (number of TDEEthFrames in the Fragment, 64)
 */
py::array_t<uint16_t> np_array_adc(daqdataformats::Fragment const& frag){
    return np_array_adc_data(frag.get_data(), get_n_frames(frag));
}

/**
 * @brief Unpacks the timestamps in a Fragment containing TDEEthFrames into a numpy
 * array with dimension (number of TDEEthFrames in the Fragment)
 */
py::array_t<long double> np_array_timestamp(daqdataformats::Fragment const& frag){
    return np_array_timestamp_data(frag.get_data(), get_n_frames(frag));
}

} // namespace dunedaq::rawdatautils::tde // NOLINT
