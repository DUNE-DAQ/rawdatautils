/**
 * @file file_conversion.cpp Python bindings for file conversions between formats
 *
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "rawdatautils/WIBtoWIB2.hpp"

#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

namespace py = pybind11;

namespace dunedaq {
namespace rawdatautils {

namespace fc {
namespace python {

void
register_file_conversion(py::module& m)
{
  m.def("convert_file", &wibftowib2f);
}

} // namespace python
} // namespace file_conversion
} // namespace rawdatautils
} // namespace dunedaq
