/**
 * @file WIBtoTDE16.hpp Implementation of functions to convert files from the old WIB format to TDE16
 *
 * This is part of the DUNE DAQ , copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#ifndef RAWDATAUTILS_INCLUDE_WIBTOTDE16_HPP_
#define RAWDATAUTILS_INCLUDE_WIBTOTDE16_HPP_

#include <cstdint>
#include <iostream>
#include <fstream>
#include <filesystem>
#include "detdataformats/wib/WIBFrame.hpp"
#include "detdataformats/tde/TDE16Frame.hpp"

namespace dunedaq {
namespace rawdatautils {

detdataformats::tde::TDE16Frame
wibtotde(detdataformats::wib::WIBFrame* fr, uint64_t timestamp=0) {
  detdataformats::tde::TDE16Frame res;
  // leave ADCs empty for now

  auto header = fr->get_wib_header();
  res.get_tde_header()->version = header->version;
  res.get_tde_header()->crate = header->crate_no;
  res.get_tde_header()->slot = header->slot_no;
  res.get_tde_header()->link = header->fiber_no;
  res.set_timestamp(timestamp);
  return res;
}

void
wib_binary_to_tde_binary(std::string& filename, std::string& output) {
  std::ifstream file(filename.c_str(), std::ios::binary);
  std::ofstream out(output.c_str(), std::ios::binary);
  std::cout << "Transforming " << filename << " to " << output << '\n';
  auto size = std::filesystem::file_size(filename);
  std::vector<char> v(size);
  file.read(v.data(), size);
  file.close();
  int num_frames = size / sizeof(detdataformats::wib::WIBFrame);
  if (num_frames > 10 ) num_frames = 10;
  std::cout << "Number of frames found: "<< num_frames << '\n';
  auto ptr = reinterpret_cast<detdataformats::wib::WIBFrame*>(v.data());
  uint64_t timestamp = ptr->get_timestamp();
  while(num_frames--){
    auto tdefr = wibtotde(ptr, timestamp);
    timestamp += (32*4472);
    ptr++;
    out.write(reinterpret_cast<char*>(&tdefr), sizeof(tdefr));
  }
  out.close();
}

void
wib_hdf5_to_tde_binary(std::string& /*filename*/, std::string& /*output*/) {
}


} // namespace dunedaq::rawdatautils
}

#endif // RAWDATAUTILS_INCLUDE_WIBTOTDE16_HPP_
