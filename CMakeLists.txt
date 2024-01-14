cmake_minimum_required(VERSION 3.25)

# TODO: get version from git
project(logs_oracle
        LANGUAGES   C
        VERSION     1.0
        DESCRIPTION "lib for estimating the number of logs in eth_getLogs request")

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -pipe -pthread -ffast-math")

# Section dependencies
find_package(PkgConfig REQUIRED)

pkg_check_modules(CURL  REQUIRED IMPORTED_TARGET libcurl)
pkg_check_modules(CJSON REQUIRED IMPORTED_TARGET libcjson)
pkg_check_modules(CRITERION IMPORTED_TARGET criterion)

# Section: lib logsoracle
add_library(logsoracle
            err.c common.c file.c vector.c upstream.c liboracle.c)

target_include_directories(logsoracle PRIVATE .)

target_compile_options(logsoracle PRIVATE
                       -Wall -Wextra -Wpedantic # -Werror
                       -Wnull-dereference -Wvla -Wshadow -Wstrict-prototypes
                       -Wfloat-equal -Wconversion -Wdouble-promotion -Wwrite-strings)
target_link_libraries(logsoracle PkgConfig::CURL PkgConfig::CJSON)

set_target_properties(logsoracle PROPERTIES VERSION     ${PROJECT_VERSION})
set_target_properties(logsoracle PROPERTIES DESCRIPTION ${PROJECT_DESCRIPTION})

set_target_properties(logsoracle PROPERTIES C_STANDARD          11)
set_target_properties(logsoracle PROPERTIES C_STANDARD_REQUIRED ON)

set_target_properties(logsoracle PROPERTIES PUBLIC_HEADER liboracle.h)
# set_target_properties(logsoracle PROPERTIES C_VISIBILITY_PRESET hidden)

include(GNUInstallDirs)
install(TARGETS logsoracle
        LIBRARY       DESTINATION ${CMAKE_INSTALL_LIBDIR}
        PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

configure_file(logsoracle.pc.in logsoracle.pc @ONLY)
install(FILES       ${CMAKE_BINARY_DIR}/logsoracle.pc
        DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/pkgconfig)

# Section: unit tests
include(CTest)

add_executable(libtest
               test/liboracle_test.c)
set_target_properties(libtest PROPERTIES C_STANDARD 23)
target_link_libraries(libtest logsoracle PkgConfig::CRITERION)

add_test(NAME libtest COMMAND libtest)
