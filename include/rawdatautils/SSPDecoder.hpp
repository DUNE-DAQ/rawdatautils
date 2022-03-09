/**
 * @file SSPDecoder.hpp
 *
 * Class for reading out HDF5 files as 
 * 
 *
 * This is part of the DUNE DAQ Application Framework, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#ifndef HDF5LIBS_INCLUDE_SSPDECODER_HPP_
#define HDF5LIBS_INCLUDE_SSPDECODER_HPP_

// System
#include <iostream>
#include <string>

#include <vector>

namespace dunedaq {
namespace rawdatautils {

class SSPDecoder
{

public:

  SSPDecoder(const std::string& file_name);

  std::vector<std::vector<int>> get_ssp_frames();

private: 
  SSPDecoder(const SSPDecoder&) = delete;
  SSPDecoder& operator=(const SSPDecoder&) = delete;
  SSPDecoder(SSPDecoder&&) = delete;
  SSPDecoder& operator=(SSPDecoder&&) = delete;

  std::string m_file_name;
  unsigned m_number_events; 

  std::vector<std::vector<int>> m_ssp_frames;

};

} // hdf5libs
} // dunedaq


#endif // HDF5LIBS_INCLUDE_SSPDECODER_HPP_

// Local Variables:
// c-basic-offset: 2
// End:
