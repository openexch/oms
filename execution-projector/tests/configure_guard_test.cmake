# SPDX-License-Identifier: Apache-2.0
# Runs cmake/verify-inputs.cmake against the real inputs (must pass) and against tampered
# copies (each must fail naming the reason). Usage:
#   cmake -DOE_PROJECTOR_DIR=<projector> -DOE_WORK_DIR=<scratch> -P configure_guard_test.cmake
set(verify "${OE_PROJECTOR_DIR}/cmake/verify-inputs.cmake")

function(run_verify dir result output)
    execute_process(COMMAND ${CMAKE_COMMAND} -DOE_PROJECTOR_DIR=${dir} -P ${verify}
        RESULT_VARIABLE code OUTPUT_VARIABLE out ERROR_VARIABLE err)
    set(${result} "${code}" PARENT_SCOPE)
    set(${output} "${out}${err}" PARENT_SCOPE)
endfunction()

function(fresh_copy name out)
    set(dir "${OE_WORK_DIR}/${name}")
    file(REMOVE_RECURSE "${dir}")
    file(MAKE_DIRECTORY "${dir}/schema" "${dir}/tests")
    file(COPY "${OE_PROJECTOR_DIR}/dependencies.lock" DESTINATION "${dir}")
    file(COPY "${OE_PROJECTOR_DIR}/schema/me-journal-schema.xml" DESTINATION "${dir}/schema")
    file(COPY "${OE_PROJECTOR_DIR}/tests/fixtures" DESTINATION "${dir}/tests")
    set(${out} "${dir}" PARENT_SCOPE)
endfunction()

function(expect_refusal name dir pattern)
    run_verify("${dir}" code out)
    if(code EQUAL 0)
        message(FATAL_ERROR "${name}: tampered inputs were accepted")
    endif()
    if(NOT out MATCHES "${pattern}")
        message(FATAL_ERROR "${name}: refused for the wrong reason: ${out}")
    endif()
    message(STATUS "PASS ${name}")
endfunction()

run_verify("${OE_PROJECTOR_DIR}" code out)
if(NOT code EQUAL 0)
    message(FATAL_ERROR "Real inputs refused: ${out}")
endif()
message(STATUS "PASS real inputs verify")

fresh_copy(schema-hash dir)
file(READ "${dir}/dependencies.lock" lock)
string(JSON lock SET "${lock}" me_journal_schema_sha256
    "\"0000000000000000000000000000000000000000000000000000000000000000\"")
file(WRITE "${dir}/dependencies.lock" "${lock}")
expect_refusal(configureRefusesSchemaHashDrift "${dir}" "ME journal schema differs from dependencies.lock")

fresh_copy(schema-three dir)
# The real pre-S1 schema 3 file, kept only as this negative input.
file(COPY_FILE "${OE_PROJECTOR_DIR}/tests/inputs/settlement-journal-schema.xml" "${dir}/schema/me-journal-schema.xml")
file(SHA256 "${dir}/schema/me-journal-schema.xml" hash)
file(READ "${dir}/dependencies.lock" lock)
string(JSON lock SET "${lock}" me_journal_schema_sha256 "\"${hash}\"")
file(WRITE "${dir}/dependencies.lock" "${lock}")
expect_refusal(configureRefusesSchemaThreeEvenWhenPinned "${dir}" "not schemaId 4 version 1")

fresh_copy(fixture dir)
file(APPEND "${dir}/tests/fixtures/java-trade.bin" "x") # one extra byte
expect_refusal(configureRefusesFixtureDrift "${dir}" "Fixture differs from SHA256SUMS: java-trade.bin")

fresh_copy(commit dir)
file(READ "${dir}/dependencies.lock" lock)
string(JSON lock SET "${lock}" fixture_source_commit "\"main\"")
file(WRITE "${dir}/dependencies.lock" "${lock}")
expect_refusal(configureRefusesNonShaFixtureSource "${dir}" "fixture_source_commit is not a match commit sha")

# The PENDING placeholder is tolerated only while the lock itself still carries it. Once the lock
# names the signed match commit, the placeholder branch in verify-inputs.cmake must be deleted;
# this case then requires configure to refuse PENDING.
file(READ "${OE_PROJECTOR_DIR}/dependencies.lock" lock)
string(JSON pinned GET "${lock}" fixture_source_commit)
fresh_copy(pending dir)
file(READ "${dir}/dependencies.lock" lock)
string(JSON lock SET "${lock}" fixture_source_commit "\"PENDING-match-rc2-s1\"")
file(WRITE "${dir}/dependencies.lock" "${lock}")
if(pinned STREQUAL "PENDING-match-rc2-s1")
    run_verify("${dir}" code out)
    if(NOT code EQUAL 0 OR NOT out MATCHES "fixture_source_commit is the placeholder PENDING-match-rc2-s1")
        message(FATAL_ERROR "configureRefusesPendingFixtureSource: placeholder not reported: ${out}")
    endif()
    message(STATUS "PENDING configureRefusesPendingFixtureSource (lock still carries the placeholder)")
else()
    expect_refusal(configureRefusesPendingFixtureSource "${dir}" "fixture_source_commit is not a match commit sha")
endif()

file(REMOVE_RECURSE "${OE_WORK_DIR}")
message(STATUS "PASS configure guard")
