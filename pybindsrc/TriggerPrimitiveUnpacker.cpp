/**
 * @file TriggerPeimitive.cc Fast C++ -> numpy TriggerPrimitive format unpacker
 *
 * This is part of the DUNE DAQ , copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "trgdataformats/TriggerPrimitive.hpp"
#include "daqdataformats/Fragment.hpp"


#include <cstdint>
#include <pybind11/numpy.h>

namespace py = pybind11;

namespace dunedaq {
namespace rawdatautils {
namespace triggerprimitive {


template <typename T>
py::array _mkarray_via_buffer(size_t n) {
    return py::array(py::buffer_info(
        nullptr, sizeof(T), py::format_descriptor<T>::format(), 1, {n}, {sizeof(T)}));
}


/**
 * @brief Gets number of TriggerPrimitive in a fragment
 */
uint32_t get_n_frames(daqdataformats::Fragment const& frag){
  return frag.get_data_size() / sizeof(trgdataformats::TriggerPrimitive);
}


py::array_t<trgdataformats::TriggerPrimitive> get_tp_array(daqdataformats::Fragment const& frag) {

    auto n = get_n_frames(frag);

    auto tp_data = static_cast<trgdataformats::TriggerPrimitive*>(frag.get_data());

    auto arr = _mkarray_via_buffer<trgdataformats::TriggerPrimitive>(n);
    auto req = arr.request();
    auto *ptr_res = static_cast<trgdataformats::TriggerPrimitive*>(req.ptr);

    for (size_t i=0; i<n; ++i) {
      ptr_res[i] = tp_data[i];
    }

    return arr;
}



} // triggerprimitive
} // rawdatautils
} // dunedaq
