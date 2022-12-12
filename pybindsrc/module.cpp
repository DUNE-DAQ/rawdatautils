/**
 * @file module.cpp
 *
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

namespace py = pybind11;


namespace dunedaq {
namespace rawdatautils {

namespace unpack{
namespace python {
extern void register_unpack(py::module &);
}
}

namespace python {

PYBIND11_MODULE(_daq_rawdatautils_py, m) {

    m.doc() = "c++ implementation of the dunedaq rawdatautils modules"; // optional module docstring

    py::module_ unpack_module = m.def_submodule("unpack");
    unpack::python::register_unpack(unpack_module);

}

} // namespace python
} // namespace rawdatautils
} // namespace dunedaq
