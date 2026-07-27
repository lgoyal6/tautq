# Same warning contract as taut (CLAUDE.md): -Wall -Wextra -Werror + strict extras for
# library code; relaxed set for test targets (framework headers trip the strict set).
add_library(tautq_warnings INTERFACE)
target_compile_options(tautq_warnings INTERFACE
    -Wall
    -Wextra
    -Werror
    -Wpedantic
    -Wshadow
    -Wconversion
    -Wsign-conversion
    -Wnon-virtual-dtor
    -Wold-style-cast
    -Wcast-align
    -Wunused
    -Wnull-dereference
    -Wdouble-promotion
)

add_library(tautq_test_warnings INTERFACE)
target_compile_options(tautq_test_warnings INTERFACE
    -Wall
    -Wextra
)
