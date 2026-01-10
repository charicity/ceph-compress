# lz4

set(hrac_sources
  CompressionPluginHrac.cc
  Hrac.cc
)

add_library(ceph_hrac SHARED ${hrac_sources})
target_link_libraries(ceph_hrac
  PRIVATE compressor $<$<PLATFORM_ID:Windows>:ceph-common>)
set_target_properties(ceph_hrac PROPERTIES
  VERSION 2.0.0
  SOVERSION 2
  INSTALL_RPATH "")
install(TARGETS ceph_hrac DESTINATION ${compressor_plugin_dir})