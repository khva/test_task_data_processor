cmake_minimum_required (VERSION 3.12)

# Enable Hot Reload for MSVC compilers if supported.
if (POLICY CMP0141)
  cmake_policy(SET CMP0141 NEW)
  set(CMAKE_MSVC_DEBUG_INFORMATION_FORMAT "$<IF:$<AND:$<C_COMPILER_ID:MSVC>,$<CXX_COMPILER_ID:MSVC>>,$<$<CONFIG:Debug,RelWithDebInfo>:EditAndContinue>,$<$<CONFIG:Debug,RelWithDebInfo>:ProgramDatabase>>")
endif()

SET(CMAKE_CXX_FLAGS_RELEASE "-O2")

project ("data_processor")

# Add source to this project's executable.
add_executable (data_processor "main.cpp" "data_processor.hpp" "data_processor.cpp" "ring_buffer.hpp" "worker_thread.hpp")

set_property(TARGET data_processor PROPERTY CXX_STANDARD 20)
