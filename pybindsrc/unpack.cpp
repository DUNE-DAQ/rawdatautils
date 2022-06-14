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

namespace py = pybind11;

namespace dunedaq {
namespace rawdatautils {

namespace wib {
  extern py::array_t<uint16_t> np_array_adc(daqdataformats::Fragment& frag);
  extern py::array_t<uint16_t> np_array_adc_data(void* data, int nframes);
  extern py::array_t<uint64_t> np_array_timestamp(daqdataformats::Fragment& frag);
  extern py::array_t<uint64_t> np_array_timestamp_data(void* data, int nframes);
}

namespace wib2 {
  extern py::array_t<uint16_t> np_array_adc(daqdataformats::Fragment& frag);
  extern py::array_t<uint16_t> np_array_adc_data(void* data, int nframes);
  extern py::array_t<uint64_t> np_array_timestamp(daqdataformats::Fragment& frag);
  extern py::array_t<uint64_t> np_array_timestamp_data(void* data, int nframes);
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
register_unpack(py::module& m)
{
  py::module_ wib_module = m.def_submodule("wib");
  wib_module.def("np_array_adc", &wib::np_array_adc);
  wib_module.def("np_array_timestamp", &wib::np_array_timestamp);
  wib_module.def("np_array_adc_data", &wib::np_array_adc_data);
  wib_module.def("np_array_timestamp_data", &wib::np_array_timestamp_data);

  py::module_ wib2_module = m.def_submodule("wib2");
  wib2_module.def("np_array_adc", &wib2::np_array_adc);
  wib2_module.def("np_array_timestamp", &wib2::np_array_timestamp);
  wib2_module.def("np_array_adc_data", &wib2::np_array_adc_data);
  wib2_module.def("np_array_timestamp_data", &wib2::np_array_timestamp_data);

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
