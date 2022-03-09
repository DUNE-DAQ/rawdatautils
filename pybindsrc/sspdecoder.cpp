/**
 * @file sspdecoder.cpp
 *
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "rawdatautils/SSPDecoder.hpp"

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

namespace py = pybind11;

namespace dunedaq {
namespace rawdatautils {
namespace python {

void
register_sspdecoder(py::module& m)
{

    py::class_<SSPDecoder>(m, "SSPDecoder")
        .def(py::init<std::string>())
        .def_property_readonly("ssp_frames", &SSPDecoder::get_ssp_frames)
    ;
    
}

} // namespace python
} // namespace rawdatautils
} // namespace dunedaq
