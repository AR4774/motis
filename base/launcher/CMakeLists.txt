cmake_minimum_required(VERSION 3.10)
project(motis)

file(GLOB_RECURSE motis-launcher-files src/*.cc)
add_executable(motis ${motis-launcher-files} ../../modules/csa/include/motis/csa/pretrip_alternative.h ../../modules/csa/include/motis/csa/cpu/csa_alternative_profile_search_default_cpu.h ../../modules/csa/include/motis/csa/cpu/csa_alternative_base_search.h ../../modules/csa/include/motis/csa/csa_alternative_search.h)
target_compile_features(motis PUBLIC cxx_std_17)
target_include_directories(motis PRIVATE include)
target_link_libraries(motis
  ${CMAKE_THREAD_LIBS_INIT}
  boost-system
  boost-filesystem
  conf
  motis-bootstrap
  web-server-tls
  ianatzdb-res
  pbf_sdf_fonts_res-res
  tiles_server_res-res
)
set_target_properties(motis PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
if (NOT MSVC)
  set_target_properties(motis PROPERTIES LINK_FLAGS "-Wl,-rpath,./")
endif()
