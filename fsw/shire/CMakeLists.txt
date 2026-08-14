######################################################################
#
# CMAKE build recipe for pc-linux PSP component
#
######################################################################

# This contains the fully platform-specific code to
# run CFE on this target.

# Simulith hardware library sources (sim implementations)
set(SIMULITH_BUILD_DIR "${CMAKE_SOURCE_DIR}/../simulith/build")
set(HWLIB_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../hwlib")
if(EXISTS "${HWLIB_DIR}/src/sim/libcan.c")
    set(HWLIB_SRC_FILES
        ../hwlib/src/sim/libcan.c
        ../hwlib/src/sim/libgpio.c
        ../hwlib/src/sim/libi2c.c
        ../hwlib/src/sim/libmem.c
        ../hwlib/src/sim/libsocket.c
        ../hwlib/src/sim/libspi.c
        ../hwlib/src/sim/libtrq.c
        ../hwlib/src/sim/libuart.c
    )
    set(HWLIB_INC_DIRS ../hwlib/inc)
else()
    message(WARNING "shire PSP: hwlib not found, building without hardware interface library")
    set(HWLIB_SRC_FILES "")
    set(HWLIB_INC_DIRS "")
endif()

# Build the shire PSP implementation as a library
add_library(psp-${CFE_PSP_TARGETNAME}-impl OBJECT
    src/cfe_psp_exception.c
    src/cfe_psp_start.c
    src/cfe_psp_timebase.c
    ../pc-linux/src/cfe_psp_memory.c
    ../pc-linux/src/cfe_psp_ssr.c
    ../pc-linux/src/cfe_psp_support.c
    ../pc-linux/src/cfe_psp_watchdog.c
    ${HWLIB_SRC_FILES}
)

# The _GNU_SOURCE directive is required to call non-posix APIs
# that are specific to the Linux/glibc environment.
# Code outside the pc-linux PSP should _not_ depend on this.
target_compile_definitions(psp-${CFE_SYSTEM_PSPNAME}-impl PRIVATE
    _GNU_SOURCE
    $<TARGET_PROPERTY:psp_module_api,INTERFACE_COMPILE_DEFINITIONS>
)

target_include_directories(psp-${CFE_PSP_TARGETNAME}-impl PRIVATE
    inc
    $<TARGET_PROPERTY:psp_module_api,INTERFACE_INCLUDE_DIRECTORIES>
    ${HWLIB_INC_DIRS}
    ../../../../simulith/include
)

# Link Simulith library for hwlib sim implementations
set(PSP_IMPL_LIBRARIES zmq PARENT_SCOPE)
if(EXISTS "${SIMULITH_BUILD_DIR}/libsimulith.so")
    message(STATUS "shire PSP: Linking Simulith library: ${SIMULITH_BUILD_DIR}/libsimulith.so")
    set(PSP_IMPL_LIBRARIES "${SIMULITH_BUILD_DIR}/libsimulith.so" zmq PARENT_SCOPE)
else()
    message(WARNING "shire PSP: Simulith library not found at ${SIMULITH_BUILD_DIR}/libsimulith.so")
endif()

