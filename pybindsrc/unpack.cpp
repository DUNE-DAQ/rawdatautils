/**
 * @file unpack.cpp Python bindings for python unpackers of data
 *
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "fddetdataformats/WIBFrame.hpp"
#include "fddetdataformats/WIB2Frame.hpp"
#include "fddetdataformats/DAPHNEFrame.hpp"
#include "fddetdataformats/DAPHNEEthFrame.hpp"
#include "fddetdataformats/DAPHNEEthStreamFrame.hpp"
#include "fddetdataformats/WIBEthFrame.hpp"
#include "fddetdataformats/TDEEthFrame.hpp"
#include "daqdataformats/Fragment.hpp"

#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <fmt/core.h>

namespace py = pybind11;

namespace dunedaq {
namespace rawdatautils {

void print_hex_fragment(daqdataformats::Fragment const& frag)
{
  uint64_t* data = static_cast<uint64_t*>(frag.get_data());
  size_t data_size = (frag.get_size() - sizeof(daqdataformats::FragmentHeader)) / 8;

  for (size_t i(0); i < data_size; ++i) {
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
  extern uint32_t get_n_frames(daqdataformats::Fragment const& frag);
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
  extern uint32_t get_n_frames(daqdataformats::Fragment const& frag);
  extern py::array_t<uint16_t> np_array_adc(daqdataformats::Fragment& frag);
  extern py::array_t<uint8_t> np_array_channels(daqdataformats::Fragment& frag);
  extern py::array_t<uint16_t> np_array_adc_data(void* data, int nframes);
  extern py::array_t<uint64_t> np_array_timestamp(daqdataformats::Fragment& frag);
  extern py::array_t<uint64_t> np_array_timestamp_data(void* data, int nframes);
  extern py::array_t<uint8_t> np_array_channels_data(void* data, int nframes);

  extern uint32_t get_n_frames_stream(daqdataformats::Fragment const& frag);
  extern py::array_t<uint16_t> np_array_adc_stream(daqdataformats::Fragment& frag);
  extern py::array_t<uint8_t> np_array_channels_stream(daqdataformats::Fragment& frag);
  extern py::array_t<uint16_t> np_array_adc_stream_data(void* data, int nframes);
  extern py::array_t<uint64_t> np_array_timestamp_stream(daqdataformats::Fragment& frag);
  extern py::array_t<uint64_t> np_array_timestamp_stream_data(void* data, int nframes);
  extern py::array_t<uint8_t> np_array_channels_stream_data(void* data, int nframes);
}

namespace daphneeth {
  extern uint32_t get_n_frames(daqdataformats::Fragment const& frag);
  extern py::array_t<uint16_t> np_array_adc(daqdataformats::Fragment& frag);
  extern py::array_t<uint8_t> np_array_channels(daqdataformats::Fragment& frag);
  extern py::array_t<uint16_t> np_array_adc_data(void* data, int nframes);
  extern py::array_t<uint64_t> np_array_timestamp(daqdataformats::Fragment& frag);
  extern py::array_t<uint64_t> np_array_timestamp_data(void* data, int nframes);
  extern py::array_t<uint8_t> np_array_channels_data(void* data, int nframes);
  extern py::array_t<uint8_t> np_array_peak_found(daqdataformats::Fragment& frag);
  extern py::array_t<uint8_t> np_array_peak_found_data(void* data, int nframes);
  extern py::array_t<uint8_t> np_array_peak_num_subpeaks(daqdataformats::Fragment& frag);
  extern py::array_t<uint8_t> np_array_peak_num_subpeaks_data(void* data, int nframes);
  extern py::array_t<uint32_t> np_array_peak_adc_integral(daqdataformats::Fragment& frag);
  extern py::array_t<uint32_t> np_array_peak_adc_integral_data(void* data, int nframes);
  extern py::array_t<uint16_t> np_array_peak_adc_max(daqdataformats::Fragment& frag);
  extern py::array_t<uint16_t> np_array_peak_adc_max_data(void* data, int nframes);
  extern py::array_t<uint16_t> np_array_peak_sample_max(daqdataformats::Fragment& frag);
  extern py::array_t<uint16_t> np_array_peak_sample_max_data(void* data, int nframes);
  extern py::array_t<uint16_t> np_array_peak_samples_over_baseline(daqdataformats::Fragment& frag);
  extern py::array_t<uint16_t> np_array_peak_samples_over_baseline_data(void* data, int nframes);
  extern py::array_t<uint16_t> np_array_peak_sample_start(daqdataformats::Fragment& frag);
  extern py::array_t<uint16_t> np_array_peak_sample_start_data(void* data, int nframes);

  extern uint32_t get_n_frames_stream(daqdataformats::Fragment const& frag);
  extern py::array_t<uint16_t> np_array_adc_stream(daqdataformats::Fragment& frag);
  extern py::array_t<uint8_t> np_array_channels_stream(daqdataformats::Fragment& frag);
  extern py::array_t<uint16_t> np_array_adc_stream_data(void* data, int nframes);
  extern py::array_t<uint64_t> np_array_timestamp_stream(daqdataformats::Fragment& frag);
  extern py::array_t<uint64_t> np_array_timestamp_stream_data(void* data, int nframes);
  extern py::array_t<uint8_t> np_array_channels_stream_data(void* data, int nframes);
}

namespace tde {
  extern uint32_t get_n_frames(daqdataformats::Fragment const& frag);
  extern py::array_t<uint16_t> np_array_adc(daqdataformats::Fragment const& frag);
  extern py::array_t<uint16_t> np_array_adc_data(void* data, uint32_t n_frames);
  extern py::array_t<long double> np_array_timestamp(daqdataformats::Fragment const& frag);
  extern py::array_t<long double> np_array_timestamp_data(void* data, uint32_t n_frames);
}

namespace unpack {
namespace python {

void register_unpack(py::module& m)
{
  m.def("print_hex_fragment", &print_hex_fragment);

  py::module_ wib_module = m.def_submodule("wib");
  wib_module.def("np_array_adc", &wib::np_array_adc);
  wib_module.def("np_array_timestamp", &wib::np_array_timestamp);
  wib_module.def("np_array_adc_data", &wib::np_array_adc_data);
  wib_module.def("np_array_timestamp_data", &wib::np_array_timestamp_data);

  py::module_ wib2_module = m.def_submodule("wib2");
  wib2_module.def("get_n_frames", &wib2::get_n_frames);
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
  daphne_module.def("get_n_frames", &daphne::get_n_frames);
  daphne_module.def("np_array_adc", &daphne::np_array_adc);
  daphne_module.def("np_array_timestamp", &daphne::np_array_timestamp);
  daphne_module.def("np_array_adc_data", &daphne::np_array_adc_data);
  daphne_module.def("np_array_timestamp_data", &daphne::np_array_timestamp_data);
  daphne_module.def("np_array_channels_data", &daphne::np_array_channels_data);
  daphne_module.def("np_array_channels", &daphne::np_array_channels);
  daphne_module.def("get_n_frames_stream", &daphne::get_n_frames_stream);
  daphne_module.def("np_array_adc_stream", &daphne::np_array_adc_stream);
  daphne_module.def("np_array_timestamp_stream", &daphne::np_array_timestamp_stream);
  daphne_module.def("np_array_adc_stream_data", &daphne::np_array_adc_stream_data);
  daphne_module.def("np_array_timestamp_stream_data", &daphne::np_array_timestamp_stream_data);
  daphne_module.def("np_array_channels_stream_data", &daphne::np_array_channels_stream_data);
  daphne_module.def("np_array_channels_stream", &daphne::np_array_channels_stream);

  py::module_ daphneeth_module = m.def_submodule("daphneeth");
  daphneeth_module.def("get_n_frames", &daphneeth::get_n_frames);
  daphneeth_module.def("np_array_adc", &daphneeth::np_array_adc);
  daphneeth_module.def("np_array_timestamp", &daphneeth::np_array_timestamp);
  daphneeth_module.def("np_array_adc_data", &daphneeth::np_array_adc_data);
  daphneeth_module.def("np_array_timestamp_data", &daphneeth::np_array_timestamp_data);
  daphneeth_module.def("np_array_channels_data", &daphneeth::np_array_channels_data);
  daphneeth_module.def("np_array_channels", &daphneeth::np_array_channels);
  daphneeth_module.def("np_array_peak_found", &daphneeth::np_array_peak_found);
  daphneeth_module.def("np_array_peak_found_data", &daphneeth::np_array_peak_found_data);
  daphneeth_module.def("np_array_peak_num_subpeaks", &daphneeth::np_array_peak_num_subpeaks);
  daphneeth_module.def("np_array_peak_num_subpeaks_data", &daphneeth::np_array_peak_num_subpeaks_data);
  daphneeth_module.def("np_array_peak_adc_integral", &daphneeth::np_array_peak_adc_integral);
  daphneeth_module.def("np_array_peak_adc_integral_data", &daphneeth::np_array_peak_adc_integral_data);
  daphneeth_module.def("np_array_peak_adc_max", &daphneeth::np_array_peak_adc_max);
  daphneeth_module.def("np_array_peak_adc_max_data", &daphneeth::np_array_peak_adc_max_data);
  daphneeth_module.def("np_array_peak_sample_max", &daphneeth::np_array_peak_sample_max);
  daphneeth_module.def("np_array_peak_sample_max_data", &daphneeth::np_array_peak_sample_max_data);
  daphneeth_module.def("np_array_peak_samples_over_baseline", &daphneeth::np_array_peak_samples_over_baseline);
  daphneeth_module.def("np_array_peak_samples_over_baseline_data", &daphneeth::np_array_peak_samples_over_baseline_data);
  daphneeth_module.def("np_array_peak_sample_start", &daphneeth::np_array_peak_sample_start);
  daphneeth_module.def("np_array_peak_sample_start_data", &daphneeth::np_array_peak_sample_start_data);
  daphneeth_module.def("get_n_frames_stream", &daphneeth::get_n_frames_stream);
  daphneeth_module.def("np_array_adc_stream", &daphneeth::np_array_adc_stream);
  daphneeth_module.def("np_array_timestamp_stream", &daphneeth::np_array_timestamp_stream);
  daphneeth_module.def("np_array_adc_stream_data", &daphneeth::np_array_adc_stream_data);
  daphneeth_module.def("np_array_timestamp_stream_data", &daphneeth::np_array_timestamp_stream_data);
  daphneeth_module.def("np_array_channels_stream_data", &daphneeth::np_array_channels_stream_data);
  daphneeth_module.def("np_array_channels_stream", &daphneeth::np_array_channels_stream);

  py::module_ tde_module = m.def_submodule("tde");
  tde_module.def("get_n_frames", &tde::get_n_frames);
  tde_module.def("np_array_adc", &tde::np_array_adc);
  tde_module.def("np_array_timestamp", &tde::np_array_timestamp);
  tde_module.def("np_array_adc_data", &tde::np_array_adc_data);
  tde_module.def("np_array_timestamp_data", &tde::np_array_timestamp_data);
}

} // namespace python
} // namespace unpack
} // namespace rawdatautils
} // namespace dunedaq
