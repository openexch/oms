cmake_minimum_required(VERSION 3.30)
project(oe_execution_projector VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
find_package(Java 21 REQUIRED COMPONENTS Runtime)
find_package(PostgreSQL REQUIRED)
find_package(Threads REQUIRED)

set(OE_AERON_SOURCE_DIR "" CACHE PATH "Verified Aeron 1.53.1 source directory")
set(OE_SBE_CLASSPATH "" CACHE STRING "Pinned sbe-tool 1.40.1 and Agrona 2.5.0 classpath")
if(NOT EXISTS "${OE_AERON_SOURCE_DIR}/version.txt" OR OE_SBE_CLASSPATH STREQUAL "")
    message(FATAL_ERROR "OE_AERON_SOURCE_DIR and OE_SBE_CLASSPATH are required; see dependencies.lock")
endif()
file(READ "${CMAKE_CURRENT_SOURCE_DIR}/dependencies.lock" dependency_lock)
string(JSON expected_schema_hash GET "${dependency_lock}" journal_schema_sha256)
file(SHA256 "${CMAKE_CURRENT_SOURCE_DIR}/schema/settlement-journal-schema.xml" schema_hash)
if(NOT schema_hash STREQUAL expected_schema_hash)
    message(FATAL_ERROR "Journal schema differs from dependencies.lock")
endif()
file(STRINGS "${OE_AERON_SOURCE_DIR}/version.txt" aeron_version LIMIT_COUNT 1)
if(NOT aeron_version STREQUAL "1.53.1")
    message(FATAL_ERROR "Unsupported Aeron version: ${aeron_version}")
endif()

# Use the supported C++ wrapper over the C client. Disable upstream Gradle/test
# downloads; generate the archive C codecs using the pinned SBE tool ourselves.
set(BUILD_AERON_DRIVER OFF CACHE BOOL "" FORCE)
set(BUILD_AERON_ARCHIVE_API OFF CACHE BOOL "" FORCE)
set(AERON_TESTS OFF CACHE BOOL "" FORCE)
set(AERON_UNIT_TESTS OFF CACHE BOOL "" FORCE)
set(AERON_SYSTEM_TESTS OFF CACHE BOOL "" FORCE)
set(AERON_BUILD_SAMPLES OFF CACHE BOOL "" FORCE)
set(AERON_BUILD_DOCUMENTATION OFF CACHE BOOL "" FORCE)
set(AERON_INSTALL_TARGETS OFF CACHE BOOL "" FORCE)
set(AERON_HIDE_DEPRECATION_MESSAGE ON CACHE BOOL "" FORCE)
add_subdirectory("${OE_AERON_SOURCE_DIR}" aeron EXCLUDE_FROM_ALL)

set(archive_src "${OE_AERON_SOURCE_DIR}/aeron-archive/src/main")
set(generated "${CMAKE_CURRENT_BINARY_DIR}/generated")
add_custom_command(OUTPUT "${generated}/archive-codecs.stamp"
    COMMAND ${CMAKE_COMMAND} -E make_directory "${generated}/c"
    COMMAND ${Java_JAVA_EXECUTABLE} --add-opens java.base/jdk.internal.misc=ALL-UNNAMED
        -Dsbe.target.language=C -Dsbe.target.namespace=aeron.archive.client
        "-Dsbe.output.dir=${generated}/c" -Dsbe.validation.stop.on.error=true
        -cp "${OE_SBE_CLASSPATH}" uk.co.real_logic.sbe.SbeTool
        "${archive_src}/resources/archive/aeron-archive-codecs.xml"
    COMMAND ${CMAKE_COMMAND} -E touch "${generated}/archive-codecs.stamp"
    DEPENDS "${archive_src}/resources/archive/aeron-archive-codecs.xml"
    VERBATIM)
add_custom_target(oe_archive_codecs DEPENDS "${generated}/archive-codecs.stamp")
file(GLOB archive_c_sources CONFIGURE_DEPENDS "${archive_src}/c/client/*.c")
add_library(oe_archive_client STATIC ${archive_c_sources})
add_dependencies(oe_archive_client oe_archive_codecs)
target_include_directories(oe_archive_client PUBLIC "${archive_src}/c" "${archive_src}/cpp_wrapper"
    PRIVATE "${generated}")
target_link_libraries(oe_archive_client PUBLIC aeron_static aeron_client_wrapper Threads::Threads)

target_compile_definitions(oe_archive_client PRIVATE
    AERON_VERSION_TXT="1.53.1" AERON_VERSION_MAJOR=1 AERON_VERSION_MINOR=53
    AERON_VERSION_PATCH=1 AERON_VERSION_GITSHA="release-1.53.1")

add_custom_command(OUTPUT "${generated}/journal-codecs.stamp"
    COMMAND ${CMAKE_COMMAND} -E make_directory "${generated}/journal"
    COMMAND ${Java_JAVA_EXECUTABLE} --add-opens java.base/jdk.internal.misc=ALL-UNNAMED
        -Dsbe.target.language=Cpp -Dsbe.target.namespace=oe.journal
        "-Dsbe.output.dir=${generated}/journal" -Dsbe.validation.stop.on.error=true
        -cp "${OE_SBE_CLASSPATH}" uk.co.real_logic.sbe.SbeTool
        "${CMAKE_CURRENT_SOURCE_DIR}/schema/settlement-journal-schema.xml"
    COMMAND ${CMAKE_COMMAND} -E touch "${generated}/journal-codecs.stamp"
    DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/schema/settlement-journal-schema.xml" VERBATIM)
add_custom_target(oe_journal_codecs DEPENDS "${generated}/journal-codecs.stamp")
add_library(oe_projection STATIC src/journal.cpp src/store.cpp)
add_dependencies(oe_projection oe_journal_codecs)
target_include_directories(oe_projection PUBLIC src PRIVATE "${generated}/journal")
target_link_libraries(oe_projection PUBLIC PostgreSQL::PostgreSQL)
target_compile_options(oe_projection PRIVATE -Wall -Wextra -Wpedantic)
add_executable(execution-projector src/main.cpp src/monitor.cpp)
target_link_libraries(execution-projector PRIVATE oe_projection oe_archive_client)
target_compile_options(execution-projector PRIVATE -Wall -Wextra -Wpedantic)

enable_testing()

add_executable(projector-store-test tests/store_test.cpp)
target_link_libraries(projector-store-test PRIVATE oe_projection)
target_compile_definitions(projector-store-test PRIVATE OE_PROJECTOR_SCHEMA="${CMAKE_CURRENT_SOURCE_DIR}/schema/projector.sql")
add_test(NAME projector-store COMMAND projector-store-test)
set_tests_properties(projector-store PROPERTIES SKIP_RETURN_CODE 77 TIMEOUT 60)

add_executable(projector-monitor-test tests/monitor_test.cpp src/monitor.cpp)
target_include_directories(projector-monitor-test PRIVATE src)
target_link_libraries(projector-monitor-test PRIVATE Threads::Threads)
add_test(NAME projector-monitor COMMAND projector-monitor-test)
set_tests_properties(projector-monitor PROPERTIES TIMEOUT 20)
