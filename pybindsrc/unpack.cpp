/**
 * @file unpack.cpp Python bindings for python unpackers of data
 *
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "WIBUnpacker.cc"
#include "WIB2Unpacker.cc"
#include "DAPHNEUnpacker.cc"

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

namespace py = pybind11;

namespace dunedaq {
namespace rawdatautils {
namespace unpack {
namespace python {

void
register_unpack(py::module& m)
{
  m.def("np_array_adc_wib", &wib::np_array_adc);
  m.def("np_array_timestamp_wib", &wib::np_array_timestamp);
  m.def("np_array_adc_wib2", &wib2::np_array_adc);
  m.def("np_array_timestamp_wib2", &wib2::np_array_timestamp);
  m.def("np_array_adc_daphne", &daphne::np_array_adc);
  m.def("np_array_timestamp_daphne", &daphne::np_array_timestamp);
}

} // namespace python
} // namespace rawdatautils
} // namespace dunedaq
}
