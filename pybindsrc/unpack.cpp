/**
 * @file unpack.cpp Python bindings for python unpackers of data
 *
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "detdataformats/wib/WIBFrame.hpp"
#include "detdataformats/wib2/WIB2Frame.hpp"
#include "detdataformats/daphne/DAPHNEFrame.hpp"
#include "daqdataformats/Fragment.hpp"

#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <fmt/core.h>

namespace py = pybind11;

namespace dunedaq {
namespace rawdatautils {

void print_hex_fragment(daqdataformats::Fragment const& frag) {
  uint64_t* data = static_cast<uint64_t*>(frag.get_data());
  size_t data_size = (frag.get_size() - sizeof(daqdataformats::FragmentHeader))/8;

  for ( size_t i(0); i<data_size; ++i) {
    fmt::print("{:06d} 0x{:016x}\n", i, data[i]);
  }

}


namespace wib {
  extern py::array_t<uint16_t> np_array_adc(daqdataformats::Fragment& frag);
  extern py::array_t<uint16_t> np_array_adc_data(void* data, int nframes);
  extern py::array_t<uint64_t> np_array_timestamp(daqdataformats::Fragment& frag);
  extern py::array_t<uint64_t> np_array_timestamp_data(void* data, int nframes);
}

namespace wib2 {
  extern uint32_t n_wib2_frames(daqdataformats::Fragment const& frag);
  extern py::array_t<uint16_t> np_array_adc(daqdataformats::Fragment const& frag);
  extern py::array_t<uint16_t> np_array_adc_data(void* data, int nframes);
  extern py::array_t<uint64_t> np_array_timestamp(daqdataformats::Fragment const& frag);
  extern py::array_t<uint64_t> np_array_timestamp_data(void* data, int nframes);
}

namespace wibeth {
  extern uint32_t get_n_frames(daqdataformats::Fragment const& frag);
  extern py::array_t<uint16_t> np_array_adc(daqdataformats::Fragment const& frag);
  extern py::array_t<uint16_t> np_array_adc_data(void* data, uint32_t n_frames);
  extern py::array_t<uint64_t> np_array_timestamp(daqdataformats::Fragment const& frag);
  extern py::array_t<uint64_t> np_array_timestamp_data(void* data, uint32_t n_frames);
}


namespace daphne {
  extern py::array_t<uint16_t> np_array_adc(daqdataformats::Fragment& frag);
  extern py::array_t<uint16_t> np_array_adc_data(void* data, int nframes);
  extern py::array_t<uint64_t> np_array_timestamp(daqdataformats::Fragment& frag);
  extern py::array_t<uint64_t> np_array_timestamp_data(void* data, int nframes);
}

namespace unpack {
namespace python {

void
register_unpack(py::module& m) {

  m.def("print_hex_fragment", &print_hex_fragment);

  py::module_ wib_module = m.def_submodule("wib");
  wib_module.def("np_array_adc", &wib::np_array_adc);
  wib_module.def("np_array_timestamp", &wib::np_array_timestamp);
  wib_module.def("np_array_adc_data", &wib::np_array_adc_data);
  wib_module.def("np_array_timestamp_data", &wib::np_array_timestamp_data);

  py::module_ wib2_module = m.def_submodule("wib2");
  wib2_module.def("n_wib2_frames", &wib2::n_wib2_frames);
  wib2_module.def("np_array_adc", &wib2::np_array_adc);
  wib2_module.def("np_array_timestamp", &wib2::np_array_timestamp);
  wib2_module.def("np_array_adc_data", &wib2::np_array_adc_data);
  wib2_module.def("np_array_timestamp_data", &wib2::np_array_timestamp_data);

  py::module_ wibeth_module = m.def_submodule("wibeth");
  wibeth_module.def("get_n_frames", &wibeth::get_n_frames);
  wibeth_module.def("np_array_adc", &wibeth::np_array_adc);
  wibeth_module.def("np_array_timestamp", &wibeth::np_array_timestamp);
  wibeth_module.def("np_array_adc_data", &wibeth::np_array_adc_data);
  wibeth_module.def("np_array_timestamp_data", &wibeth::np_array_timestamp_data);


  py::module_ daphne_module = m.def_submodule("daphne");
  daphne_module.def("np_array_adc", &daphne::np_array_adc);
  daphne_module.def("np_array_timestamp", &daphne::np_array_timestamp);
  daphne_module.def("np_array_adc_data", &daphne::np_array_adc_data);
  daphne_module.def("np_array_timestamp_data", &daphne::np_array_timestamp_data);

  
}

} // namespace python
} // namespace rawdatautils
} // namespace dunedaq
}
