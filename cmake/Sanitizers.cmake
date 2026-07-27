# Global sanitizer wiring, identical in shape to taut's. TAUTQ_SANITIZE is applied at the
# top level BEFORE add_subdirectory(taut), so directory-scope inheritance instruments the
# vendored library with the same flags — mixed instrumentation breaks ASan.
set(TAUTQ_SANITIZE "" CACHE STRING "Comma-separated sanitizers, e.g. address,undefined")

if(TAUTQ_SANITIZE)
    add_compile_options(
        -fsanitize=${TAUTQ_SANITIZE}
        -fno-omit-frame-pointer
        -fno-sanitize-recover=all
    )
    add_link_options(-fsanitize=${TAUTQ_SANITIZE})
    message(STATUS "tautq: sanitizers enabled -> ${TAUTQ_SANITIZE}")
endif()
