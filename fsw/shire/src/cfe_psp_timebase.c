/************************************************************************
 * NASA Docket No. GSC-18,719-1, and identified as “core Flight System: Bootes”
 *
 * Copyright (c) 2020 United States Government as represented by the
 * Administrator of the National Aeronautics and Space Administration.
 * All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ************************************************************************/

/**
 * \file
 *
 * A PSP module to satisfy the PSP time API on systems which
 * do not have a hardware clock register, but do provide a POSIX
 * compliant implementation of clock_gettime() and CLOCK_MONOTONIC
 * that can fulfill this role.
 *
 * The POSIX CLOCK_MONOTONIC is defined as a monotonically increasing
 * clock that has no specific epoch.  It is not affected by local time
 * changes and is not settable.
 *
 * The POSIX interface uses a "struct timespec" which has units in
 * nanoseconds, but this is converted down to units of microseconds for
 * consistency with previous versions of PSP where CFE_PSP_Get_Timebase()
 * returned units of microseconds.
 */

/*
**  System Include Files
*/
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>

#include "cfe_psp.h"
#include "cfe_psp_module.h"

#include "cfe_psp_timebase.h"

/*
 * The specific clock ID to use with clock_gettime
 *
 * Linux provides some special (non-posix) clock IDs that also
 * could be relevant/useful:
 *
 * CLOCK_MONOTONIC_COARSE - emphasis on read speed at the (possible?) expense of precision
 * CLOCK_MONOTONIC_RAW - possibly hardware based, not affected by NTP or other sync software
 * CLOCK_BOOTTIME - includes time the system is suspended.
 *
 * Defaulting to the POSIX-specified "MONOTONIC" but it should be possible to use
 * one of the Linux-specific variants if the target system provides it.
 */
#define CFE_PSP_TIMEBASE_REF_CLOCK CLOCK_MONOTONIC

CFE_PSP_MODULE_DECLARE_SIMPLE(timebase_simulith_clock);

static int simulith_client_initialized = 0;
static uint64_t latest_tick_time_ns = 0;
static uint64_t previous_tick_time_ns = 0;
static pthread_mutex_t tick_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t tick_condition = PTHREAD_COND_INITIALIZER;
static pthread_t tick_distribution_thread;
bool tick_thread_running = false;
volatile uint64_t tick_generation = 0;

void timebase_simulith_clock_Init(uint32 PspModuleId)
{
    /* Inform the user that this module is in use */
    printf("CFE_PSP: Using simulith clock as CFE timebase\n");
}

void CFE_PSP_InitSimulithTime(void)
{
    if (!simulith_client_initialized)
    {
        int status = simulith_client_init(LOCAL_PUB_ADDR, LOCAL_REP_ADDR, "shire-fsw", INTERVAL_NS);
        if (status != 0)
        {
            printf("CFE_PSP: simulith_client_init failed: %d\n", status);
            return;
        }

        status = simulith_client_handshake();
        if (status != 0)
        {
            printf("CFE_PSP: simulith_client_handshake failed: %d\n", status);
            simulith_client_shutdown();
            return;
        }

        simulith_client_initialized = 1;
        pthread_cond_broadcast(&tick_condition); // Signal that the client is initialized
        printf("CFE_PSP: Simulith client initialized and handshake complete\n");

        // Start the tick distribution thread in PSP after handshake
        tick_thread_running = true;
        if (pthread_create(&tick_distribution_thread, NULL, CFE_PSP_SimulithTickDistributionThread, NULL) != 0)
        {
            printf("CFE_PSP: Failed to start tick distribution thread after handshake\n");
        }
    }
}

void CFE_PSP_ShutdownSimulithTime(void)
{
    if (simulith_client_initialized)
    {
        simulith_client_shutdown();
        simulith_client_initialized = 0;
        printf("CFE_PSP: Simulith client shutdown\n");

        // Stop the tick distribution thread and clean up
        tick_thread_running = false;
        pthread_cond_broadcast(&tick_condition); // Wake the thread if waiting
        pthread_join(tick_distribution_thread, NULL);
        pthread_cond_destroy(&tick_condition);
        pthread_mutex_destroy(&tick_mutex);
        tick_generation = 0;
        latest_tick_time_ns = 0;
        previous_tick_time_ns = 0;
    }
}

unsigned int CFE_PSP_WaitForSimulithTick(unsigned int ticks_to_wait)
{  
    pthread_mutex_lock(&tick_mutex);
    uint64_t start_generation = tick_generation;
    while ((tick_generation - start_generation) < ticks_to_wait)
    {
        pthread_cond_wait(&tick_condition, &tick_mutex);
    }
    pthread_mutex_unlock(&tick_mutex);
    return ticks_to_wait;
}

uint64_t CFE_PSP_GetSimulithTimeNs(void)
{
    uint64_t time_ns;
    pthread_mutex_lock(&tick_mutex);
    time_ns = latest_tick_time_ns;
    pthread_mutex_unlock(&tick_mutex);
    return time_ns;
}

/*
 * Master tick receiver thread
 * This thread receives ticks from Simulith and distributes them to all timebases
 */
void* CFE_PSP_SimulithTickDistributionThread(void* arg)
{
    uint64_t tick_time_ns;
    uint32_t tick_count = 0;

    while (tick_thread_running)
    {
        if (simulith_client_wait_for_tick(&tick_time_ns) == 0)
        {
            tick_count++;
            pthread_mutex_lock(&tick_mutex);
            previous_tick_time_ns = latest_tick_time_ns;
            latest_tick_time_ns = tick_time_ns;
            tick_generation++;
            pthread_cond_broadcast(&tick_condition);
            pthread_mutex_unlock(&tick_mutex);
        }
        else
        {
            printf("simulith_client_wait_for_tick() failed\n");
            sched_yield();
        }
    }
    return NULL;
}

void CFE_PSP_GetSimulithTimespec(struct timespec *ts)
{
    uint64_t time_ns = CFE_PSP_GetSimulithTimeNs();
    ts->tv_sec = time_ns / 1000000000UL;
    ts->tv_nsec = time_ns % 1000000000UL;
}

void CFE_PSP_Get_Timebase(uint32 *Tbu, uint32 *Tbl)
{
    struct timespec now;
    
    /* Use simulith time if available, otherwise fall back to system time */
    if (simulith_client_initialized && latest_tick_time_ns > 0)
    {
        CFE_PSP_GetSimulithTimespec(&now);
    }
    else
    {
        /* Fall back to system monotonic clock during early initialization */
        clock_gettime(CFE_PSP_TIMEBASE_REF_CLOCK, &now);
    }
    
    *Tbu = now.tv_sec & 0xFFFFFFFF;
    *Tbl = now.tv_nsec;
}

void CFE_PSP_GetTime(OS_time_t *LocalTime)
{
    struct timespec now;
    
    /* Use simulith time if available, otherwise fall back to system time */
    if (simulith_client_initialized && latest_tick_time_ns > 0)
    {
        CFE_PSP_GetSimulithTimespec(&now);
    }
    else
    {
        /* Fall back to system monotonic clock during early initialization */
        clock_gettime(CFE_PSP_TIMEBASE_REF_CLOCK, &now);
    }
    
    *LocalTime = OS_TimeAssembleFromNanoseconds(now.tv_sec, now.tv_nsec);
}

uint32 CFE_PSP_GetTimerTicksPerSecond(void)
{
    /* Use INTERVAL_NS to calculate ticks per second */
    return (uint32)(1000000000UL / INTERVAL_NS);
}

uint32 CFE_PSP_GetTimerLow32Rollover(void)
{
    /* Use INTERVAL_NS to calculate the rollover value */
    return (uint32)(1000000000UL / INTERVAL_NS);
}
