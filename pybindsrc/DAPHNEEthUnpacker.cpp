/**
 * @file DAPHNEEthUnpacker.cc Fast C++ -> numpy DAPHNE Ethernet unpacker
 *
 * This is part of the DUNE DAQ , copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "fddetdataformats/DAPHNEEthFrame.hpp"
#include "fddetdataformats/DAPHNEEthStreamFrame.hpp"
#include "daqdataformats/Fragment.hpp"

#include <cstdint>
#include <pybind11/numpy.h>

namespace py = pybind11;
namespace dunedaq::rawdatautils::daphneeth {

uint32_t get_n_frames(daqdataformats::Fragment const& frag)
{
  return (frag.get_size() - sizeof(daqdataformats::FragmentHeader)) / sizeof(fddetdataformats::DAPHNEEthFrame);
}

uint32_t get_n_frames_stream(daqdataformats::Fragment const& frag)
{
  return (frag.get_size() - sizeof(daqdataformats::FragmentHeader)) / sizeof(fddetdataformats::DAPHNEEthStreamFrame);
}

py::array_t<uint8_t> np_array_channels_stream_data(void* data, int nframes)
{
  const auto channels_per_daphne = fddetdataformats::DAPHNEEthStreamFrame::s_num_channels;
  py::array_t<uint8_t> channels(channels_per_daphne * nframes);
  auto ptr = static_cast<uint8_t*>(channels.request().ptr);

  for (size_t i = 0; i < static_cast<size_t>(nframes); ++i) {
    auto fr = reinterpret_cast<fddetdataformats::DAPHNEEthStreamFrame*>(static_cast<char*>(data) + i * sizeof(fddetdataformats::DAPHNEEthStreamFrame));
    ptr[i * channels_per_daphne + 0] = fr->get_channel0();
    ptr[i * channels_per_daphne + 1] = fr->get_channel1();
    ptr[i * channels_per_daphne + 2] = fr->get_channel2();
    ptr[i * channels_per_daphne + 3] = fr->get_channel3();
  }

  channels.resize({nframes, channels_per_daphne});
  return channels;
}

py::array_t<uint8_t> np_array_channels_data(void* data, int nframes)
{
  py::array_t<uint8_t> channels(nframes);
  auto ptr = static_cast<uint8_t*>(channels.request().ptr);

  for (size_t i = 0; i < static_cast<size_t>(nframes); ++i) {
    auto fr = reinterpret_cast<fddetdataformats::DAPHNEEthFrame*>(static_cast<char*>(data) + i * sizeof(fddetdataformats::DAPHNEEthFrame));
    ptr[i] = fr->get_channel();
  }

  return channels;
}

py::array_t<uint8_t> np_array_channels(daqdataformats::Fragment& frag)
{
  return np_array_channels_data(frag.get_data(), (frag.get_size() - sizeof(daqdataformats::FragmentHeader)) / sizeof(fddetdataformats::DAPHNEEthFrame));
}

py::array_t<uint8_t> np_array_channels_stream(daqdataformats::Fragment& frag)
{
  return np_array_channels_stream_data(frag.get_data(), (frag.get_size() - sizeof(daqdataformats::FragmentHeader)) / sizeof(fddetdataformats::DAPHNEEthStreamFrame));
}

py::array_t<uint16_t> np_array_adc_data(void* data, int nframes)
{
  const auto adcs_per_channel = fddetdataformats::DAPHNEEthFrame::s_num_adcs;

  py::array_t<uint16_t> ret(nframes * adcs_per_channel);
  auto ptr = static_cast<uint16_t*>(ret.request().ptr);
  for (size_t i = 0; i < static_cast<size_t>(nframes); ++i) {
    auto fr = reinterpret_cast<fddetdataformats::DAPHNEEthFrame*>(static_cast<char*>(data) + i * sizeof(fddetdataformats::DAPHNEEthFrame));
    for (size_t j = 0; j < adcs_per_channel; ++j) {
      ptr[i * adcs_per_channel + j] = fr->get_adc(j);
    }
  }
  ret.resize({nframes, adcs_per_channel});
  return ret;
}

py::array_t<uint16_t> np_array_adc_stream_data(void* data, int nframes)
{
  const auto channels_per_daphne = fddetdataformats::DAPHNEEthStreamFrame::s_num_channels;
  const auto adcs_per_channel = fddetdataformats::DAPHNEEthStreamFrame::s_adcs_per_channel;

  py::array_t<uint16_t> ret(channels_per_daphne * nframes * adcs_per_channel);
  auto ptr = static_cast<uint16_t*>(ret.request().ptr);
  for (size_t i = 0; i < static_cast<size_t>(nframes); ++i) {
    auto fr = reinterpret_cast<fddetdataformats::DAPHNEEthStreamFrame*>(static_cast<char*>(data) + i * sizeof(fddetdataformats::DAPHNEEthStreamFrame));
    for (size_t j = 0; j < adcs_per_channel; ++j) {
      for (size_t k = 0; k < channels_per_daphne; ++k) {
        ptr[channels_per_daphne * (adcs_per_channel * i + j) + k] = fr->get_adc(j, k);
      }
    }
  }
  ret.resize({nframes * adcs_per_channel, channels_per_daphne});
  return ret;
}

py::array_t<uint64_t> np_array_timestamp_data(void* data, int nframes)
{
  py::array_t<uint64_t> ret(nframes);
  auto ptr = static_cast<uint64_t*>(ret.request().ptr);
  for (size_t i = 0; i < static_cast<size_t>(nframes); ++i) {
    auto fr = reinterpret_cast<fddetdataformats::DAPHNEEthFrame*>(static_cast<char*>(data) + i * sizeof(fddetdataformats::DAPHNEEthFrame));
    ptr[i] = fr->get_timestamp();
  }

  return ret;
}

py::array_t<uint64_t> np_array_timestamp_stream_data(void* data, int nframes)
{
  const auto adcs_per_channel = fddetdataformats::DAPHNEEthStreamFrame::s_adcs_per_channel;
  const size_t ticks_per_adc = 1;

  py::array_t<uint64_t> ret(nframes * adcs_per_channel);
  auto ptr = static_cast<uint64_t*>(ret.request().ptr);
  for (size_t i = 0; i < static_cast<size_t>(nframes); ++i) {
    auto fr = reinterpret_cast<fddetdataformats::DAPHNEEthStreamFrame*>(static_cast<char*>(data) + i * sizeof(fddetdataformats::DAPHNEEthStreamFrame));
    for (size_t j = 0; j < adcs_per_channel; ++j) {
      ptr[i * adcs_per_channel + j] = fr->get_timestamp() + j * ticks_per_adc;
    }
  }

  return ret;
}

template<typename T, typename Getter>
py::array_t<T> np_array_peak_field_data(void* data, int nframes, Getter getter)
{
  constexpr int peaks_per_frame = fddetdataformats::DAPHNEEthFrame::PeakDescriptorData::max_peaks;

  py::array_t<T> ret(nframes * peaks_per_frame);
  auto ptr = static_cast<T*>(ret.request().ptr);
  for (size_t i = 0; i < static_cast<size_t>(nframes); ++i) {
    auto fr = reinterpret_cast<fddetdataformats::DAPHNEEthFrame*>(static_cast<char*>(data) + i * sizeof(fddetdataformats::DAPHNEEthFrame));
    const auto& peaks = fr->get_peaks_data();
    for (int peak = 0; peak < peaks_per_frame; ++peak) {
      ptr[i * peaks_per_frame + peak] = static_cast<T>(getter(peaks, peak));
    }
  }
  ret.resize({nframes, peaks_per_frame});
  return ret;
}

py::array_t<uint8_t> np_array_peak_found_data(void* data, int nframes)
{
  return np_array_peak_field_data<uint8_t>(
    data,
    nframes,
    [](const fddetdataformats::DAPHNEEthFrame::PeakDescriptorData& peaks, int peak) -> uint8_t {
      return static_cast<uint8_t>(peaks.is_found(peak));
    });
}

py::array_t<uint8_t> np_array_peak_num_subpeaks_data(void* data, int nframes)
{
  return np_array_peak_field_data<uint8_t>(
    data,
    nframes,
    [](const fddetdataformats::DAPHNEEthFrame::PeakDescriptorData& peaks, int peak) -> uint8_t {
      return peaks.get_num_subpeaks(peak);
    });
}

py::array_t<uint32_t> np_array_peak_adc_integral_data(void* data, int nframes)
{
  return np_array_peak_field_data<uint32_t>(
    data,
    nframes,
    [](const fddetdataformats::DAPHNEEthFrame::PeakDescriptorData& peaks, int peak) -> uint32_t {
      return peaks.get_adc_integral(peak);
    });
}

py::array_t<uint16_t> np_array_peak_adc_max_data(void* data, int nframes)
{
  return np_array_peak_field_data<uint16_t>(
    data,
    nframes,
    [](const fddetdataformats::DAPHNEEthFrame::PeakDescriptorData& peaks, int peak) -> uint16_t {
      return peaks.get_adc_max(peak);
    });
}

py::array_t<uint16_t> np_array_peak_sample_max_data(void* data, int nframes)
{
  return np_array_peak_field_data<uint16_t>(
    data,
    nframes,
    [](const fddetdataformats::DAPHNEEthFrame::PeakDescriptorData& peaks, int peak) -> uint16_t {
      return peaks.get_sample_max(peak);
    });
}

py::array_t<uint16_t> np_array_peak_samples_over_baseline_data(void* data, int nframes)
{
  return np_array_peak_field_data<uint16_t>(
    data,
    nframes,
    [](const fddetdataformats::DAPHNEEthFrame::PeakDescriptorData& peaks, int peak) -> uint16_t {
      return peaks.get_samples_over_baseline(peak);
    });
}

py::array_t<uint16_t> np_array_peak_sample_start_data(void* data, int nframes)
{
  return np_array_peak_field_data<uint16_t>(
    data,
    nframes,
    [](const fddetdataformats::DAPHNEEthFrame::PeakDescriptorData& peaks, int peak) -> uint16_t {
      return peaks.get_sample_start(peak);
    });
}

py::array_t<uint16_t> np_array_adc(daqdataformats::Fragment& frag)
{
  return np_array_adc_data(frag.get_data(), (frag.get_size() - sizeof(daqdataformats::FragmentHeader)) / sizeof(fddetdataformats::DAPHNEEthFrame));
}

py::array_t<uint16_t> np_array_adc_stream(daqdataformats::Fragment& frag)
{
  return np_array_adc_stream_data(frag.get_data(), (frag.get_size() - sizeof(daqdataformats::FragmentHeader)) / sizeof(fddetdataformats::DAPHNEEthStreamFrame));
}

py::array_t<uint64_t> np_array_timestamp(daqdataformats::Fragment& frag)
{
  return np_array_timestamp_data(frag.get_data(), (frag.get_size() - sizeof(daqdataformats::FragmentHeader)) / sizeof(fddetdataformats::DAPHNEEthFrame));
}

py::array_t<uint64_t> np_array_timestamp_stream(daqdataformats::Fragment& frag)
{
  return np_array_timestamp_stream_data(frag.get_data(), (frag.get_size() - sizeof(daqdataformats::FragmentHeader)) / sizeof(fddetdataformats::DAPHNEEthStreamFrame));
}

py::array_t<uint8_t> np_array_peak_found(daqdataformats::Fragment& frag)
{
  return np_array_peak_found_data(frag.get_data(), (frag.get_size() - sizeof(daqdataformats::FragmentHeader)) / sizeof(fddetdataformats::DAPHNEEthFrame));
}

py::array_t<uint8_t> np_array_peak_num_subpeaks(daqdataformats::Fragment& frag)
{
  return np_array_peak_num_subpeaks_data(frag.get_data(), (frag.get_size() - sizeof(daqdataformats::FragmentHeader)) / sizeof(fddetdataformats::DAPHNEEthFrame));
}

py::array_t<uint32_t> np_array_peak_adc_integral(daqdataformats::Fragment& frag)
{
  return np_array_peak_adc_integral_data(frag.get_data(), (frag.get_size() - sizeof(daqdataformats::FragmentHeader)) / sizeof(fddetdataformats::DAPHNEEthFrame));
}

py::array_t<uint16_t> np_array_peak_adc_max(daqdataformats::Fragment& frag)
{
  return np_array_peak_adc_max_data(frag.get_data(), (frag.get_size() - sizeof(daqdataformats::FragmentHeader)) / sizeof(fddetdataformats::DAPHNEEthFrame));
}

py::array_t<uint16_t> np_array_peak_sample_max(daqdataformats::Fragment& frag)
{
  return np_array_peak_sample_max_data(frag.get_data(), (frag.get_size() - sizeof(daqdataformats::FragmentHeader)) / sizeof(fddetdataformats::DAPHNEEthFrame));
}

py::array_t<uint16_t> np_array_peak_samples_over_baseline(daqdataformats::Fragment& frag)
{
  return np_array_peak_samples_over_baseline_data(frag.get_data(), (frag.get_size() - sizeof(daqdataformats::FragmentHeader)) / sizeof(fddetdataformats::DAPHNEEthFrame));
}

py::array_t<uint16_t> np_array_peak_sample_start(daqdataformats::Fragment& frag)
{
  return np_array_peak_sample_start_data(frag.get_data(), (frag.get_size() - sizeof(daqdataformats::FragmentHeader)) / sizeof(fddetdataformats::DAPHNEEthFrame));
}

} // namespace dunedaq::rawdatautils::daphneeth
