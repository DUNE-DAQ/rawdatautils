cmake_minimum_required(VERSION 3.12 FATAL_ERROR)
project(rawdatautils VERSION 1.0.0)

find_package(daq-cmake REQUIRED)
find_package(Boost COMPONENTS unit_test_framework REQUIRED)
find_package(daqdataformats REQUIRED)

daq_setup_environment()

##############################################################################
# Main library
daq_add_library (WIBFragmentDecoder.cpp LINK_LIBRARIES ers::ers HighFive daqdataformats::daqdataformats logging::logging detdataformats::detdataformats detchannelmaps::detchannelmaps hdf5libs::hdf5libs ${CETLIB} ${CETLIB_EXCEPT})

##############################################################################
daq_add_python_bindings(*.cpp LINK_LIBRARIES ${PROJECT_NAME} daqdataformats::daqdataformats)

##############################################################################
# Applications
# daq_add_application(hdf5_demo_tpc_decoder demo_tpc_decoder.cpp LINK_LIBRARIES ${PROJECT_NAME})
# daq_add_application(hdf5_demo_pd_decoder demo_pd_decoder.cpp LINK_LIBRARIES ${PROJECT_NAME})

daq_install()
