cmake_minimum_required(VERSION 3.12 FATAL_ERROR)
project(rawdatautils VERSION 1.2.1)

find_package(daq-cmake REQUIRED)
find_package(Boost COMPONENTS unit_test_framework REQUIRED)
find_package(daqdataformats REQUIRED)
find_package(detdataformats REQUIRED)

daq_setup_environment()

##############################################################################
# Main library
daq_add_library (WIBFragmentDecoder.cpp LINK_LIBRARIES)

##############################################################################
daq_add_python_bindings(*.cpp LINK_LIBRARIES ${PROJECT_NAME} daqdataformats::daqdataformats detdataformats::detdataformats)

daq_add_unit_test(WIBtoWIB2_test LINK_LIBRARIES detdataformats::detdataformats)

##############################################################################
# Applications
# daq_add_application(hdf5_demo_tpc_decoder demo_tpc_decoder.cpp LINK_LIBRARIES ${PROJECT_NAME})
# daq_add_application(hdf5_demo_pd_decoder demo_pd_decoder.cpp LINK_LIBRARIES ${PROJECT_NAME})

daq_install()
