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

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

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
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <sched.h>
#include <errno.h>
#include <inttypes.h>
#include <math.h>
#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>

#include "cfe_psp.h"
#include "cfe_psp_module.h"

#include "cfe_psp_timebase.h"
#include "simulith.h"

/* Production links the Simulith transport metrics provider. Keep the terminal
 * diagnostics optional so the PSP remains independently linkable in coverage
 * and on targets that do not provide simulated device transports. */
void simulith_transport_write_metrics_json(FILE *stream) __attribute__((weak));

/* cFE TIME's internal 1Hz tone-signal ISR (declared in the TIME module's
 * private cfe_time_utils.h, not a public PSP-facing header -- declared
 * directly here rather than including that header, since nothing else in
 * this file needs it). See CFE_TIME_TaskInit()'s own comment in
 * cfe_time_task.c: when the OSAL has no "cFS-Master" timebase for it to
 * hook its own 1Hz callback to, "the PSP must use the old way and call the
 * 1hz function directly" -- exactly the situation here, since this PSP's
 * time is simulith-tick-driven rather than backed by a generic OSAL timer.
 * Weak, like simulith_transport_write_metrics_json above, so this file
 * still links standalone (e.g. coverage-io_lib-shire_psp_runtime-testrunner,
 * which builds this PSP source without the TIME module). */
extern void CFE_TIME_Tone1HzISR(void) __attribute__((weak));

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
static int simulith_client_created = 0;
static bool simulith_time_valid = false;
static uint64_t latest_tick_time_ns = 0;
static uint64_t pending_tick_sequence = 0;
static bool pending_tick_completion = false;
static bool pending_tick_dispatched = false;
static bool pending_tick_auto_completion = false;
static bool deferred_tick_completion = false;
static pthread_mutex_t tick_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t tick_condition = PTHREAD_COND_INITIALIZER;
/* Dedicated generation mailbox for the receiver -> SCH handoff. Other cFE
 * tick and participant waiters retain tick_condition and its accounting. */
static uint32_t sch_mailbox_generation = 0;
static uint64_t sch_mailbox_published_ns = 0;
static uint64_t sch_handoff_count = 0;
static uint64_t sch_handoff_total_ns = 0;
static uint64_t sch_handoff_max_ns = 0;

static void CFE_PSP_WakeSchMailbox(void)
{
    __atomic_add_fetch(&sch_mailbox_generation, 1U, __ATOMIC_RELEASE);
    (void)syscall(SYS_futex, &sch_mailbox_generation, FUTEX_WAKE_PRIVATE, 1,
                  NULL, NULL, 0);
}
static pthread_t tick_distribution_thread;
static pthread_t bootstrap_tick_thread;
static bool bootstrap_tick_running = false;
bool tick_thread_running = false;
volatile uint64_t tick_generation = 0;

#define CFE_PSP_MAX_SIMULITH_PARTICIPANTS 64
#define CFE_PSP_MAX_SIMULITH_PIPES 256

typedef struct
{
    uint32_t schedule_entry;
    uint32_t message_id;
    uint32_t pipe_id;
    const void *message_ref;
    uint64_t sequence;
    bool claimed;
    bool completed;
    bool delivery_bound;
    uint64_t registered_ns;
    uint64_t claimed_ns;
    uint64_t claimed_cpu_ns;
} simulith_participant_t;

#define CFE_PSP_MAX_PARTICIPANT_METRICS 128
#define CFE_PSP_MAX_COMMAND_DELIVERY_METRICS 128
#define CFE_PSP_LATENCY_BUCKET_WIDTH_NS UINT64_C(5000)
#define CFE_PSP_LATENCY_BUCKETS 2048

typedef struct
{
    uint32_t schedule_entry;
    uint32_t message_id;
    uint64_t count;
    uint64_t dispatch_total_ns;
    uint64_t execution_total_ns;
    uint64_t execution_cpu_total_ns;
    uint64_t dispatch_max_ns;
    uint64_t execution_max_ns;
    uint64_t execution_cpu_max_ns;
    uint64_t dispatch_histogram_overflows;
    uint64_t execution_histogram_overflows;
    uint64_t execution_cpu_histogram_overflows;
    uint64_t dispatch_histogram[CFE_PSP_LATENCY_BUCKETS];
    uint64_t execution_histogram[CFE_PSP_LATENCY_BUCKETS];
    uint64_t execution_cpu_histogram[CFE_PSP_LATENCY_BUCKETS];
} simulith_participant_metric_t;

typedef struct
{
    int token;
    uint32_t pipe_id;
    int publication_token;
} simulith_task_participant_t;

typedef struct
{
    uint32_t pipe_id;
    bool observed_poll;
    bool observed_blocking_receive;
} simulith_pipe_behavior_t;

static simulith_participant_t simulith_participants[CFE_PSP_MAX_SIMULITH_PARTICIPANTS];
static size_t simulith_participant_count = 0;
static simulith_pipe_behavior_t simulith_pipe_behaviors[CFE_PSP_MAX_SIMULITH_PIPES];
static size_t simulith_pipe_behavior_count = 0;
static bool simulith_participant_accounting_error = false;
static pthread_key_t simulith_participant_key;
static pthread_once_t simulith_participant_once = PTHREAD_ONCE_INIT;
static uint64_t simulith_participants_registered = 0;
static uint64_t simulith_participants_completed = 0;
static uint64_t simulith_participants_canceled = 0;
static uint64_t simulith_sch_ticks_completed = 0;
static uint64_t simulith_ground_output_due = 0;
static uint64_t simulith_ground_output_sent = 0;
static uint64_t simulith_ground_output_throttled = 0;
static uint64_t simulith_last_ground_output_ns = 0;
static uint32_t simulith_ground_output_message_id = 0;
static simulith_participant_metric_t
    simulith_participant_metrics[CFE_PSP_MAX_PARTICIPANT_METRICS];
static size_t simulith_participant_metric_count = 0;
typedef struct
{
    uint32_t message_id;
    uint64_t count;
} simulith_command_delivery_metric_t;
static simulith_command_delivery_metric_t
    simulith_command_delivery_metrics[CFE_PSP_MAX_COMMAND_DELIVERY_METRICS];
static uint64_t simulith_command_delivery_metric_overflows = 0;

static void CFE_PSP_RecordCommandDelivery(uint32_t message_id)
{
    size_t index = ((uint64_t)message_id * UINT64_C(2654435761)) %
        CFE_PSP_MAX_COMMAND_DELIVERY_METRICS;
    for (size_t probe = 0; probe < CFE_PSP_MAX_COMMAND_DELIVERY_METRICS; ++probe)
    {
        simulith_command_delivery_metric_t *metric =
            &simulith_command_delivery_metrics[index];
        if (metric->count == 0 || metric->message_id == message_id)
        {
            metric->message_id = message_id;
            metric->count++;
            return;
        }
        index = (index + 1U) % CFE_PSP_MAX_COMMAND_DELIVERY_METRICS;
    }
    simulith_command_delivery_metric_overflows++;
}

static uint64_t CFE_PSP_MonotonicNs(void)
{
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (uint64_t)now.tv_sec * 1000000000ULL + (uint64_t)now.tv_nsec;
}

static uint64_t CFE_PSP_ThreadCpuNs(void)
{
    struct timespec now;
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &now);
    return (uint64_t)now.tv_sec * 1000000000ULL + (uint64_t)now.tv_nsec;
}

static unsigned int CFE_PSP_LatencyBucket(uint64_t latency_ns)
{
    uint64_t bucket = latency_ns / CFE_PSP_LATENCY_BUCKET_WIDTH_NS;
    return bucket < CFE_PSP_LATENCY_BUCKETS ? (unsigned int)bucket :
        CFE_PSP_LATENCY_BUCKETS - 1U;
}

static uint64_t CFE_PSP_LatencyPercentile(
    const uint64_t histogram[CFE_PSP_LATENCY_BUCKETS], uint64_t count,
    uint64_t numerator, uint64_t denominator)
{
    if (count == 0)
        return 0;
    uint64_t target = (count * numerator + denominator - 1) / denominator;
    uint64_t accumulated = 0;
    for (unsigned int bucket = 0; bucket < CFE_PSP_LATENCY_BUCKETS; ++bucket)
    {
        accumulated += histogram[bucket];
        if (accumulated >= target)
            return ((uint64_t)bucket + 1U) *
                CFE_PSP_LATENCY_BUCKET_WIDTH_NS;
    }
    return 0;
}

static void CFE_PSP_RecordParticipantLatency(const simulith_participant_t *participant,
                                             uint64_t completed_ns,
                                             uint64_t completed_cpu_ns)
{
    if (participant->claimed_ns < participant->registered_ns ||
        completed_ns < participant->claimed_ns ||
        completed_cpu_ns < participant->claimed_cpu_ns)
        return;
    size_t index = 0;
    while (index < simulith_participant_metric_count &&
           (simulith_participant_metrics[index].schedule_entry != participant->schedule_entry ||
            simulith_participant_metrics[index].message_id != participant->message_id))
        index++;
    if (index == simulith_participant_metric_count)
    {
        if (index >= CFE_PSP_MAX_PARTICIPANT_METRICS)
            return;
        simulith_participant_metrics[index].schedule_entry = participant->schedule_entry;
        simulith_participant_metrics[index].message_id = participant->message_id;
        simulith_participant_metric_count++;
    }

    simulith_participant_metric_t *metric = &simulith_participant_metrics[index];
    uint64_t dispatch_ns = participant->claimed_ns - participant->registered_ns;
    uint64_t execution_ns = completed_ns - participant->claimed_ns;
    uint64_t execution_cpu_ns = completed_cpu_ns - participant->claimed_cpu_ns;
    metric->count++;
    metric->dispatch_total_ns += dispatch_ns;
    metric->execution_total_ns += execution_ns;
    metric->execution_cpu_total_ns += execution_cpu_ns;
    if (dispatch_ns > metric->dispatch_max_ns) metric->dispatch_max_ns = dispatch_ns;
    if (execution_ns > metric->execution_max_ns) metric->execution_max_ns = execution_ns;
    if (execution_cpu_ns > metric->execution_cpu_max_ns)
        metric->execution_cpu_max_ns = execution_cpu_ns;
    if (dispatch_ns >= CFE_PSP_LATENCY_BUCKET_WIDTH_NS * CFE_PSP_LATENCY_BUCKETS)
        metric->dispatch_histogram_overflows++;
    if (execution_ns >= CFE_PSP_LATENCY_BUCKET_WIDTH_NS * CFE_PSP_LATENCY_BUCKETS)
        metric->execution_histogram_overflows++;
    if (execution_cpu_ns >= CFE_PSP_LATENCY_BUCKET_WIDTH_NS * CFE_PSP_LATENCY_BUCKETS)
        metric->execution_cpu_histogram_overflows++;
    metric->dispatch_histogram[CFE_PSP_LatencyBucket(dispatch_ns)]++;
    metric->execution_histogram[CFE_PSP_LatencyBucket(execution_ns)]++;
    metric->execution_cpu_histogram[CFE_PSP_LatencyBucket(execution_cpu_ns)]++;
}

static void CFE_PSP_CreateParticipantKey(void)
{
    (void)pthread_key_create(&simulith_participant_key, free);
}

static simulith_task_participant_t *CFE_PSP_FindTaskParticipant(void)
{
    pthread_once(&simulith_participant_once, CFE_PSP_CreateParticipantKey);
    return pthread_getspecific(simulith_participant_key);
}

static simulith_task_participant_t *CFE_PSP_GetTaskParticipant(void)
{
    simulith_task_participant_t *task = CFE_PSP_FindTaskParticipant();
    if (task == NULL)
    {
        task = calloc(1, sizeof(*task));
        if (task != NULL)
        {
            (void)pthread_setspecific(simulith_participant_key, task);
        }
    }

    return task;
}

typedef struct
{
    uint64_t generation;
    bool initialized;
} tick_cursor_t;

static pthread_key_t tick_cursor_key;
static pthread_once_t tick_cursor_once = PTHREAD_ONCE_INIT;

static void CFE_PSP_CreateTickCursorKey(void)
{
    (void)pthread_key_create(&tick_cursor_key, free);
}

static void *CFE_PSP_BootstrapTickThread(void *arg)
{
    (void)arg;
    /* Startup uses a private, real-time 10 ms clock. Running this faster than
     * real time compresses cFE startup timeouts before all applications can
     * finish their post-operational initialization. Mission sequence zero is
     * not published until SCH stops this thread and handshakes. */
    const struct timespec delay = {
        .tv_sec = INTERVAL_NS / 1000000000ULL,
        .tv_nsec = INTERVAL_NS % 1000000000ULL
    };
    for (;;)
    {
        nanosleep(&delay, NULL);
        pthread_mutex_lock(&tick_mutex);
        if (!bootstrap_tick_running)
        {
            pthread_mutex_unlock(&tick_mutex);
            break;
        }
        tick_generation++;
        pthread_cond_broadcast(&tick_condition);
        pthread_mutex_unlock(&tick_mutex);
    }
    return NULL;
}

void timebase_simulith_clock_Init(uint32 PspModuleId)
{
    /* Inform the user that this module is in use */
    (void)PspModuleId;
    printf("CFE_PSP: Using simulith clock as CFE timebase\n");
    CFE_PSP_InitSimulithTime();
}

void CFE_PSP_InitSimulithTime(void)
{
    if (!simulith_client_created)
    {
        int status = simulith_client_init(LOCAL_PUB_ADDR, LOCAL_REP_ADDR, "shire-fsw", INTERVAL_NS);
        if (status != 0)
        {
            printf("CFE_PSP: simulith_client_init failed: %d\n", status);
            return;
        }

        status = simulith_client_configure_phases(SIMULITH_PHASE_MASK_EXECUTE);
        if (status != 0)
        {
            printf("CFE_PSP: failed to configure execute-phase participation\n");
            simulith_client_shutdown();
            return;
        }

        pthread_mutex_lock(&tick_mutex);
        simulith_client_created = 1;
        simulith_client_initialized = 0;
        simulith_time_valid = false;
        deferred_tick_completion = false;
        pending_tick_completion = false;
        pending_tick_dispatched = false;
        pending_tick_auto_completion = false;
        pending_tick_sequence = 0;
        latest_tick_time_ns = 0;
        tick_generation = 0;
        sch_handoff_count = sch_handoff_total_ns = sch_handoff_max_ns = 0;
        simulith_participant_count = 0;
        simulith_pipe_behavior_count = 0;
        simulith_participant_accounting_error = false;
        simulith_participants_registered = 0;
        simulith_participants_completed = 0;
        simulith_participants_canceled = 0;
        simulith_sch_ticks_completed = 0;
        simulith_ground_output_due = 0;
        simulith_ground_output_sent = 0;
        simulith_ground_output_throttled = 0;
        simulith_last_ground_output_ns = 0;
        simulith_ground_output_message_id = 0;
        memset(simulith_participant_metrics, 0, sizeof(simulith_participant_metrics));
        simulith_participant_metric_count = 0;
        memset(simulith_command_delivery_metrics, 0,
               sizeof(simulith_command_delivery_metrics));
        simulith_command_delivery_metric_overflows = 0;
        bootstrap_tick_running = true;
        tick_thread_running = true;
        pthread_mutex_unlock(&tick_mutex);
        if (pthread_create(&bootstrap_tick_thread, NULL,
                           CFE_PSP_BootstrapTickThread, NULL) != 0)
        {
            pthread_mutex_lock(&tick_mutex);
            bootstrap_tick_running = false;
            tick_thread_running = false;
            pthread_mutex_unlock(&tick_mutex);
            simulith_client_shutdown();
            simulith_client_created = 0;
            printf("CFE_PSP: failed to start startup bootstrap clock\n");
            return;
        }
        printf("CFE_PSP: Simulith client configured; SCH will start synchronized ticks\n");
    }
}

int CFE_PSP_StartSynchronizedTicks(void)
{
    if (!simulith_client_created || simulith_client_initialized)
        return simulith_client_initialized ? 0 : -1;
    pthread_mutex_lock(&tick_mutex);
    bootstrap_tick_running = false;
    pthread_mutex_unlock(&tick_mutex);
    pthread_join(bootstrap_tick_thread, NULL);
    int status = simulith_client_handshake();
    if (status != 0)
    {
        printf("CFE_PSP: simulith_client_handshake failed: %d\n", status);
        pthread_mutex_lock(&tick_mutex);
        tick_thread_running = false;
        simulith_client_initialized = 0;
        simulith_client_created = 0;
        pthread_cond_broadcast(&tick_condition);
        pthread_mutex_unlock(&tick_mutex);
        CFE_PSP_WakeSchMailbox();
        simulith_client_shutdown();
        return -1;
    }
    pthread_mutex_lock(&tick_mutex);
    simulith_client_initialized = 1;
    tick_thread_running = true;
    pthread_mutex_unlock(&tick_mutex);
    if (pthread_create(&tick_distribution_thread, NULL,
                       CFE_PSP_SimulithTickDistributionThread, NULL) != 0)
    {
        pthread_mutex_lock(&tick_mutex);
        tick_thread_running = false;
        simulith_client_initialized = 0;
        pthread_cond_broadcast(&tick_condition);
        pthread_mutex_unlock(&tick_mutex);
        CFE_PSP_WakeSchMailbox();
        printf("CFE_PSP: Failed to start tick distribution thread after handshake\n");
        return -1;
    }
    pthread_mutex_lock(&tick_mutex);
    pthread_cond_broadcast(&tick_condition);
    pthread_mutex_unlock(&tick_mutex);
    printf("CFE_PSP: Simulith handshake complete; synchronized ticks active\n");
    return 0;
}

void CFE_PSP_StopSynchronizedTicks(void)
{
    pthread_mutex_lock(&tick_mutex);
    tick_thread_running = false;
    pthread_cond_broadcast(&tick_condition);
    pthread_mutex_unlock(&tick_mutex);
    CFE_PSP_WakeSchMailbox();
    simulith_client_request_stop();
}

void CFE_PSP_ShutdownSimulithTime(void)
{
    if (simulith_client_created)
    {
        pthread_mutex_lock(&tick_mutex);
        bool stop_bootstrap = bootstrap_tick_running;
        bootstrap_tick_running = false;
        pthread_mutex_unlock(&tick_mutex);
        if (stop_bootstrap)
        {
            pthread_join(bootstrap_tick_thread, NULL);
        }

        if (simulith_client_initialized)
        {
            pthread_mutex_lock(&tick_mutex);
            tick_thread_running = false;
            pthread_cond_broadcast(&tick_condition);
            pthread_mutex_unlock(&tick_mutex);
            CFE_PSP_WakeSchMailbox();
            simulith_client_request_stop();
            pthread_join(tick_distribution_thread, NULL);
        }
        simulith_client_shutdown();
        printf("CFE_PSP: Simulith client shutdown\n");
        pthread_mutex_lock(&tick_mutex);
        simulith_client_initialized = 0;
        simulith_client_created = 0;
        simulith_time_valid = false;
        deferred_tick_completion = false;
        pending_tick_completion = false;
        pending_tick_dispatched = false;
        pending_tick_auto_completion = false;
        pending_tick_sequence = 0;
        simulith_participant_count = 0;
        simulith_pipe_behavior_count = 0;
        tick_generation = 0;
        latest_tick_time_ns = 0;
        pthread_cond_broadcast(&tick_condition);
        pthread_mutex_unlock(&tick_mutex);
    }
}

unsigned int CFE_PSP_WaitForSimulithTick(unsigned int ticks_to_wait)
{  
    pthread_once(&tick_cursor_once, CFE_PSP_CreateTickCursorKey);
    tick_cursor_t *cursor = pthread_getspecific(tick_cursor_key);
    if (cursor == NULL)
    {
        cursor = calloc(1, sizeof(*cursor));
        if (cursor == NULL)
            return 0;
        (void)pthread_setspecific(tick_cursor_key, cursor);
    }
    pthread_mutex_lock(&tick_mutex);
    if (!cursor->initialized)
    {
        cursor->generation = tick_generation;
        cursor->initialized = true;
    }
    uint64_t target_generation = cursor->generation + ticks_to_wait;
    while (tick_thread_running && tick_generation < target_generation)
    {
        pthread_cond_wait(&tick_condition, &tick_mutex);
    }
    if (tick_generation >= target_generation)
        cursor->generation = target_generation;
    pthread_mutex_unlock(&tick_mutex);
    return ticks_to_wait;
}

int CFE_PSP_WaitForPendingSimulithTick(void)
{
    for (;;) {
        /* Observe the generation before testing the predicate under the
         * mutex. A publish between the predicate and FUTEX_WAIT then returns
         * EAGAIN rather than losing a wakeup. */
        uint32_t observed = __atomic_load_n(&sch_mailbox_generation,
                                             __ATOMIC_ACQUIRE);
        pthread_mutex_lock(&tick_mutex);
        if (!tick_thread_running) {
            pthread_mutex_unlock(&tick_mutex);
            return -1;
        }
        if (pending_tick_completion && !pending_tick_dispatched &&
            !pending_tick_auto_completion) {
            uint64_t latency_ns = CFE_PSP_MonotonicNs() - sch_mailbox_published_ns;
            sch_handoff_count++;
            sch_handoff_total_ns += latency_ns;
            if (latency_ns > sch_handoff_max_ns)
                sch_handoff_max_ns = latency_ns;
            pending_tick_dispatched = true;
            pthread_mutex_unlock(&tick_mutex);
            return 0;
        }
        pthread_mutex_unlock(&tick_mutex);
        (void)syscall(SYS_futex, &sch_mailbox_generation,
                      FUTEX_WAIT_PRIVATE, observed, NULL, NULL, 0);
    }
}

void CFE_PSP_EnableDeferredTickCompletion(void)
{
    pthread_mutex_lock(&tick_mutex);
    deferred_tick_completion = true;
    pthread_mutex_unlock(&tick_mutex);
    printf("CFE_PSP: FSW tick completion is now SCH-driven\n");
}

int CFE_PSP_RegisterSimulithParticipant(uint32_t schedule_entry, uint32_t message_id)
{
    simulith_task_participant_t *task = CFE_PSP_GetTaskParticipant();
    if (task == NULL)
        return -1;

    pthread_mutex_lock(&tick_mutex);
    if (!pending_tick_completion ||
        simulith_participant_count >= CFE_PSP_MAX_SIMULITH_PARTICIPANTS ||
        task->publication_token > 0)
    {
        simulith_participant_accounting_error = true;
        printf("CFE_PSP: unable to register participant sequence=%" PRIu64
               " schedule_entry=%u mid=0x%04x\n",
               pending_tick_sequence, schedule_entry, message_id);
        pthread_mutex_unlock(&tick_mutex);
        return -1;
    }
    size_t index = simulith_participant_count++;
    simulith_participants[index] = (simulith_participant_t){
        .schedule_entry = schedule_entry,
        .message_id = message_id,
        .sequence = pending_tick_sequence,
        .registered_ns = CFE_PSP_MonotonicNs()
    };
    simulith_participants_registered++;
    task->publication_token = (int)index + 1;
    pthread_mutex_unlock(&tick_mutex);
    return (int)index + 1;
}

int CFE_PSP_ReserveSimulithMessageDelivery(uint32_t message_id,
                                           uint32_t pipe_id,
                                           const void *message_ref)
{
    simulith_task_participant_t *task = CFE_PSP_FindTaskParticipant();

    /* Only work emitted while a scheduled participant is executing belongs to
     * that participant's synchronized transaction chain. Calls from ordinary
     * asynchronous tasks retain the normal software-bus behavior. */
    if (task == NULL || (task->token <= 0 && task->publication_token <= 0))
        return 0;

    pthread_mutex_lock(&tick_mutex);
    if (!pending_tick_completion)
    {
        pthread_mutex_unlock(&tick_mutex);
        return 0;
    }

    simulith_participant_t *parent;
    bool publication = task->publication_token > 0;
    int parent_token = publication ? task->publication_token : task->token;
    if ((size_t)parent_token > simulith_participant_count)
    {
        pthread_mutex_unlock(&tick_mutex);
        return 0;
    }

    parent = &simulith_participants[parent_token - 1];
    if (parent->completed || parent->sequence != pending_tick_sequence ||
        (publication ? parent->message_id != message_id : !parent->claimed))
    {
        pthread_mutex_unlock(&tick_mutex);
        return 0;
    }

    /* A poll-only pipe is a durable input queue, not a task activation. Its
     * owner consumes the queued data under a later scheduled participant.
     * Requiring same-tick consumption would deadlock flight-like applications
     * such as CF, which intentionally drains channel input only on CF wakeup. */
    if (!publication)
    {
        for (size_t index = 0; index < simulith_pipe_behavior_count; ++index)
        {
            const simulith_pipe_behavior_t *behavior =
                &simulith_pipe_behaviors[index];
            if (behavior->pipe_id == pipe_id && behavior->observed_poll &&
                !behavior->observed_blocking_receive)
            {
                pthread_mutex_unlock(&tick_mutex);
                return 0;
            }
        }
    }

    simulith_participant_t *participant;
    int token;
    if (publication && !parent->delivery_bound)
    {
        participant = parent;
        token = parent_token;
    }
    else
    {
        if (simulith_participant_count >= CFE_PSP_MAX_SIMULITH_PARTICIPANTS)
        {
            simulith_participant_accounting_error = true;
            printf("CFE_PSP: unable to reserve message delivery sequence=%" PRIu64
                   " parent_schedule_entry=%u mid=0x%04x pipe=%u\n",
                   pending_tick_sequence, parent->schedule_entry, message_id, pipe_id);
            pthread_mutex_unlock(&tick_mutex);
            return -1;
        }

        size_t index = simulith_participant_count++;
        participant = &simulith_participants[index];
        *participant = (simulith_participant_t){
            .schedule_entry = publication ? parent->schedule_entry :
                                            (parent->schedule_entry | 0x80000000U),
            .message_id = message_id,
            .sequence = pending_tick_sequence,
            .registered_ns = CFE_PSP_MonotonicNs()
        };
        simulith_participants_registered++;
        token = (int)index + 1;
    }

    participant->pipe_id = pipe_id;
    participant->message_ref = message_ref;
    participant->delivery_bound = true;
    pthread_mutex_unlock(&tick_mutex);
    return token;
}

void CFE_PSP_EndSimulithMessageDelivery(int token, bool delivered)
{
    if (token > 0 && !delivered)
        CFE_PSP_CancelSimulithParticipant(token);
}

void CFE_PSP_EndSimulithMessagePublication(void)
{
    simulith_task_participant_t *task = CFE_PSP_FindTaskParticipant();
    if (task == NULL || task->publication_token <= 0)
        return;

    pthread_mutex_lock(&tick_mutex);
    int token = task->publication_token;
    task->publication_token = 0;
    if ((size_t)token <= simulith_participant_count)
    {
        simulith_participant_t *participant = &simulith_participants[token - 1];
        if (!participant->delivery_bound && !participant->completed)
        {
            participant->claimed = true;
            participant->completed = true;
            simulith_participants_canceled++;
            pthread_cond_broadcast(&tick_condition);
        }
    }
    pthread_mutex_unlock(&tick_mutex);
}

void CFE_PSP_CancelSimulithParticipant(int token)
{
    pthread_mutex_lock(&tick_mutex);
    if (token > 0 && (size_t)token <= simulith_participant_count)
    {
        simulith_participant_t *participant = &simulith_participants[token - 1];
        if (!participant->completed)
        {
            participant->claimed = true;
            participant->completed = true;
            simulith_participants_canceled++;
            pthread_cond_broadcast(&tick_condition);
        }
        else
        {
            simulith_participant_accounting_error = true;
            printf("CFE_PSP: duplicate participant cancellation token=%d\n", token);
        }
    }
    else
    {
        simulith_participant_accounting_error = true;
        printf("CFE_PSP: invalid participant cancellation token=%d\n", token);
    }
    pthread_mutex_unlock(&tick_mutex);
}

void CFE_PSP_SimulithTaskBeginReceive(uint32_t pipe_id, bool polling)
{
    simulith_task_participant_t *task = CFE_PSP_FindTaskParticipant();
    pthread_mutex_lock(&tick_mutex);

    size_t behavior_index = 0;
    while (behavior_index < simulith_pipe_behavior_count &&
           simulith_pipe_behaviors[behavior_index].pipe_id != pipe_id)
        behavior_index++;
    if (behavior_index == simulith_pipe_behavior_count &&
        behavior_index < CFE_PSP_MAX_SIMULITH_PIPES)
    {
        simulith_pipe_behaviors[behavior_index] =
            (simulith_pipe_behavior_t){.pipe_id = pipe_id};
        simulith_pipe_behavior_count++;
    }
    if (behavior_index < simulith_pipe_behavior_count)
    {
        simulith_pipe_behaviors[behavior_index].observed_poll |= polling;
        simulith_pipe_behaviors[behavior_index].observed_blocking_receive |=
            !polling;
    }

    if (!task || task->token <= 0 || task->pipe_id != pipe_id)
    {
        pthread_mutex_unlock(&tick_mutex);
        return;
    }

    if ((size_t)task->token <= simulith_participant_count)
    {
        simulith_participant_t *participant = &simulith_participants[task->token - 1];
        if (participant->sequence == pending_tick_sequence && participant->claimed)
        {
            uint64_t completed_ns = CFE_PSP_MonotonicNs();
            uint64_t completed_cpu_ns = CFE_PSP_ThreadCpuNs();
            participant->completed = true;
            simulith_participants_completed++;
            CFE_PSP_RecordParticipantLatency(participant, completed_ns,
                                             completed_cpu_ns);
            pthread_cond_broadcast(&tick_condition);
        }
    }
    pthread_mutex_unlock(&tick_mutex);
    task->token = 0;
}

void CFE_PSP_SimulithMessageReceived(uint32_t message_id,
                                     uint32_t pipe_id,
                                     const void *message_ref)
{
    simulith_task_participant_t *task = CFE_PSP_GetTaskParticipant();
    pthread_mutex_lock(&tick_mutex);
    CFE_PSP_RecordCommandDelivery(message_id);

    if (task == NULL || task->token > 0)
    {
        pthread_mutex_unlock(&tick_mutex);
        return;
    }
    for (size_t index = 0; index < simulith_participant_count; ++index)
    {
        simulith_participant_t *participant = &simulith_participants[index];
        if (!participant->claimed && participant->delivery_bound &&
            participant->sequence == pending_tick_sequence &&
            participant->message_id == message_id &&
            participant->pipe_id == pipe_id &&
            participant->message_ref == message_ref)
        {
            participant->claimed = true;
            participant->claimed_ns = CFE_PSP_MonotonicNs();
            participant->claimed_cpu_ns = CFE_PSP_ThreadCpuNs();
            task->token = (int)index + 1;
            task->pipe_id = pipe_id;
            break;
        }
    }
    pthread_mutex_unlock(&tick_mutex);
}

bool CFE_PSP_AllowPeriodicGroundOutput(uint32_t message_id)
{
    static uint64_t interval_ns = 0;
    pthread_mutex_lock(&tick_mutex);
    if (interval_ns == 0)
    {
        double hz = 20.0;
        const char *value = getenv("SHIRE_GROUND_OUTPUT_HZ");
        if (value != NULL)
        {
            char *end = NULL;
            errno = 0;
            double parsed = strtod(value, &end);
            if (errno == 0 && end != value && *end == '\0' &&
                isfinite(parsed) && parsed > 0.0)
                hz = parsed;
        }
        double requested_interval = 1000000000.0 / hz;
        interval_ns = requested_interval < 1.0 ? 1 :
            (requested_interval > (double)UINT64_MAX ? UINT64_MAX :
             (uint64_t)requested_interval);
    }

    uint64_t now_ns = CFE_PSP_MonotonicNs();
    if (simulith_ground_output_message_id == 0)
        simulith_ground_output_message_id = message_id;
    else if (simulith_ground_output_message_id != message_id)
    {
        fprintf(stderr,
                "CFE_PSP: periodic ground output MID changed from 0x%04" PRIx32
                " to 0x%04" PRIx32 "\n",
                simulith_ground_output_message_id, message_id);
        simulith_participant_accounting_error = true;
    }
    simulith_ground_output_due++;
    if (simulith_last_ground_output_ns == 0 ||
        now_ns - simulith_last_ground_output_ns >= interval_ns)
    {
        simulith_last_ground_output_ns = now_ns;
        simulith_ground_output_sent++;
        pthread_mutex_unlock(&tick_mutex);
        return true;
    }
    simulith_ground_output_throttled++;
    pthread_mutex_unlock(&tick_mutex);
    return false;
}

int CFE_PSP_WaitForSimulithParticipants(void)
{
    pthread_mutex_lock(&tick_mutex);
    if (simulith_participant_accounting_error)
    {
        pthread_mutex_unlock(&tick_mutex);
        return -1;
    }
    for (;;)
    {
        if (simulith_participant_accounting_error)
        {
            pthread_mutex_unlock(&tick_mutex);
            return -1;
        }
        size_t completed = 0;
        for (size_t index = 0; index < simulith_participant_count; ++index)
            completed += simulith_participants[index].completed ? 1U : 0U;
        if (completed == simulith_participant_count)
            break;

        struct timespec deadline;
        clock_gettime(CLOCK_REALTIME, &deadline);
        deadline.tv_sec++;
        if (pthread_cond_timedwait(&tick_condition, &tick_mutex, &deadline) == ETIMEDOUT)
        {
            printf("CFE_PSP: Simulith sequence %lu stalled; waiting for",
                   (unsigned long)pending_tick_sequence);
            for (size_t index = 0; index < simulith_participant_count; ++index)
            {
                if (!simulith_participants[index].completed)
                    printf(" schedule_entry[%u]/mid[0x%04x]/pipe[%u]%s",
                           simulith_participants[index].schedule_entry,
                           simulith_participants[index].message_id,
                           simulith_participants[index].pipe_id,
                           simulith_participants[index].claimed ? "/running" : "/unclaimed");
            }
            printf("\n");
            fflush(stdout);
        }
        if (!tick_thread_running || !pending_tick_completion)
        {
            pthread_mutex_unlock(&tick_mutex);
            return -1;
        }
    }
    pthread_mutex_unlock(&tick_mutex);
    return 0;
}

int CFE_PSP_CompleteSimulithTick(void)
{
    uint64_t sequence;
    pthread_mutex_lock(&tick_mutex);
    if (!pending_tick_completion)
    {
        pthread_mutex_unlock(&tick_mutex);
        printf("CFE_PSP: duplicate or missing SCH tick completion\n");
        return -1;
    }
    sequence = pending_tick_sequence;
    /* Retire this sequence before notifying the server. The completion send
     * releases the server to publish the next tick immediately, so clearing
     * the state after send could erase an already-received next sequence. */
    pending_tick_completion = false;
    pending_tick_dispatched = false;
    pending_tick_auto_completion = false;
    pthread_cond_broadcast(&tick_condition);
    pthread_mutex_unlock(&tick_mutex);

    if (simulith_client_complete_tick(sequence, SIMULITH_PHASE_EXECUTE) != 0)
    {
        printf("CFE_PSP: failed to complete Simulith sequence %lu\n",
               (unsigned long)sequence);
        return -1;
    }

    pthread_mutex_lock(&tick_mutex);
    simulith_sch_ticks_completed++;
    pthread_mutex_unlock(&tick_mutex);

    return 0;
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
    uint64_t tick_sequence;
    simulith_phase_t tick_phase;
    uint64_t next_tone_ns = 0;
    for (;;)
    {
        pthread_mutex_lock(&tick_mutex);
        bool keep_running = tick_thread_running;
        pthread_mutex_unlock(&tick_mutex);
        if (!keep_running)
            break;
        int receive_status = simulith_client_receive_phase(&tick_time_ns, &tick_sequence, &tick_phase);
        if (receive_status == 1 && tick_phase == SIMULITH_PHASE_STOP)
        {
            pthread_mutex_lock(&tick_mutex);
            tick_thread_running = false;
            pthread_cond_broadcast(&tick_condition);
            pthread_mutex_unlock(&tick_mutex);
            CFE_PSP_WakeSchMailbox();
            pthread_mutex_lock(&tick_mutex);
            printf("SIMULITH_FSW_TERMINAL {\"sch_ticks\":%" PRIu64
                   ",\"participants_registered\":%" PRIu64
                   ",\"participants_completed\":%" PRIu64
                   ",\"participants_canceled\":%" PRIu64
                   ",\"ground_output_due\":%" PRIu64
                   ",\"ground_output_sent\":%" PRIu64
                   ",\"ground_output_throttled\":%" PRIu64
                   ",\"ground_output_mid\":%u"
                   ",\"sch_handoff_us\":{\"count\":%" PRIu64
                   ",\"mean\":%.3f,\"max\":%.3f}"
                   ",\"participant_latency_resolution_us\":5"
                   ",\"participant_histogram_max_us\":10240"
                   ",\"participant_latency\":[",
                   simulith_sch_ticks_completed, simulith_participants_registered,
                   simulith_participants_completed, simulith_participants_canceled,
                   simulith_ground_output_due, simulith_ground_output_sent,
                   simulith_ground_output_throttled,
                   simulith_ground_output_message_id,
                   sch_handoff_count,
                   sch_handoff_count ?
                       (double)sch_handoff_total_ns /
                       (double)sch_handoff_count / 1000.0 : 0.0,
                   (double)sch_handoff_max_ns / 1000.0);
            for (size_t index = 0; index < simulith_participant_metric_count; ++index)
            {
                const simulith_participant_metric_t *metric =
                    &simulith_participant_metrics[index];
                uint64_t dispatch_p50 = CFE_PSP_LatencyPercentile(
                    metric->dispatch_histogram, metric->count, 50, 100) / 1000;
                uint64_t dispatch_p95 = CFE_PSP_LatencyPercentile(
                    metric->dispatch_histogram, metric->count, 95, 100) / 1000;
                uint64_t execution_p50 = CFE_PSP_LatencyPercentile(
                    metric->execution_histogram, metric->count, 50, 100) / 1000;
                uint64_t execution_p95 = CFE_PSP_LatencyPercentile(
                    metric->execution_histogram, metric->count, 95, 100) / 1000;
                uint64_t execution_cpu_p50 = CFE_PSP_LatencyPercentile(
                    metric->execution_cpu_histogram, metric->count, 50, 100) / 1000;
                uint64_t execution_cpu_p95 = CFE_PSP_LatencyPercentile(
                    metric->execution_cpu_histogram, metric->count, 95, 100) / 1000;
                printf("%s{\"schedule_entry\":%u,\"mid\":%u,\"count\":%" PRIu64
                       ",\"dispatch_us\":{\"mean\":%.3f,\"p50\":%" PRIu64
                       ",\"p95\":%" PRIu64 ",\"max\":%.3f,"
                       "\"histogram_overflows\":%" PRIu64 "},"
                       "\"execution_us\":{\"mean\":%.3f,\"p50\":%" PRIu64
                       ",\"p95\":%" PRIu64 ",\"max\":%.3f,"
                       "\"histogram_overflows\":%" PRIu64 "},"
                       "\"execution_cpu_us\":{\"mean\":%.3f,\"p50\":%" PRIu64
                       ",\"p95\":%" PRIu64 ",\"max\":%.3f,"
                       "\"histogram_overflows\":%" PRIu64 "}}",
                       index == 0 ? "" : ",", metric->schedule_entry,
                       metric->message_id, metric->count,
                       (double)metric->dispatch_total_ns / (double)metric->count / 1000.0,
                       dispatch_p50, dispatch_p95,
                       (double)metric->dispatch_max_ns / 1000.0,
                       metric->dispatch_histogram_overflows,
                       (double)metric->execution_total_ns / (double)metric->count / 1000.0,
                       execution_p50, execution_p95,
                       (double)metric->execution_max_ns / 1000.0,
                       metric->execution_histogram_overflows,
                       (double)metric->execution_cpu_total_ns / (double)metric->count / 1000.0,
                       execution_cpu_p50, execution_cpu_p95,
                       (double)metric->execution_cpu_max_ns / 1000.0,
                       metric->execution_cpu_histogram_overflows);
            }
            printf("],\"command_delivery_overflows\":%" PRIu64
                   ",\"command_deliveries\":[",
                   simulith_command_delivery_metric_overflows);
            int first_delivery = 1;
            for (size_t index = 0;
                 index < CFE_PSP_MAX_COMMAND_DELIVERY_METRICS; ++index)
            {
                if (simulith_command_delivery_metrics[index].count == 0)
                    continue;
                printf("%s{\"mid\":%u,\"count\":%" PRIu64 "}",
                       first_delivery ? "" : ",",
                       simulith_command_delivery_metrics[index].message_id,
                       simulith_command_delivery_metrics[index].count);
                first_delivery = 0;
            }
            printf("],");
            if (simulith_transport_write_metrics_json)
                simulith_transport_write_metrics_json(stdout);
            else
                printf("\"device_transactions\":[]");
            printf("}\n");
            pthread_mutex_unlock(&tick_mutex);
            fflush(stdout);
            break;
        }
        if (receive_status == 0)
        {
            if (tick_phase != SIMULITH_PHASE_EXECUTE)
                continue;
            pthread_mutex_lock(&tick_mutex);
            latest_tick_time_ns = tick_time_ns;
            simulith_time_valid = true;
            pending_tick_sequence = tick_sequence;
            pending_tick_completion = true;
            pending_tick_dispatched = false;
            pending_tick_auto_completion = !deferred_tick_completion;
            simulith_participant_count = 0;
            simulith_participant_accounting_error = false;
            tick_generation++;
            sch_mailbox_published_ns = CFE_PSP_MonotonicNs();
            pthread_cond_broadcast(&tick_condition);
            bool complete_immediately = pending_tick_auto_completion;
            pthread_mutex_unlock(&tick_mutex);
            CFE_PSP_WakeSchMailbox();

            /* CFE_TIME_TaskInit() only creates its own 1Hz tone driver when
             * the OSAL exposes a "cFS-Master" timebase (see its comment:
             * absent that, "the PSP must use the old way and call the 1hz
             * function directly"). This PSP has no such timebase -- time
             * here is simulith-tick-driven, not backed by a generic OSAL
             * timer -- so drive the tone directly, keyed to simulated time
             * (not wall-clock) so it stays correctly paced under
             * simulith_speed scaling and matches what CFE_TIME_LatchClock()
             * (via CFE_PSP_GetTime()) reads. Must run outside tick_mutex:
             * CFE_TIME_Tone1HzISR() -> CFE_TIME_LatchClock() ->
             * CFE_PSP_GetTime() re-acquires it. */
            if (CFE_TIME_Tone1HzISR && tick_time_ns >= next_tone_ns)
            {
                uint64_t periods = ((tick_time_ns - next_tone_ns) / 1000000000ULL) + 1ULL;
                for (uint64_t period = 0; period < periods; ++period)
                    CFE_TIME_Tone1HzISR();
                next_tone_ns += periods * 1000000000ULL;
            }

            /* During cFE startup no scheduler exists to own completion. Once
             * SCH enables deferred mode, only SCH may release the next tick. */
            if (complete_immediately)
                CFE_PSP_CompleteSimulithTick();
        }
        else
        {
            pthread_mutex_lock(&tick_mutex);
            bool still_running = tick_thread_running;
            pthread_mutex_unlock(&tick_mutex);
            if (still_running)
                printf("simulith_client_receive_tick() failed\n");
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
    uint64_t sim_time_ns;
    bool use_simulith_time;

    pthread_mutex_lock(&tick_mutex);
    sim_time_ns = latest_tick_time_ns;
    use_simulith_time = simulith_client_initialized && simulith_time_valid;
    pthread_mutex_unlock(&tick_mutex);
    if (use_simulith_time)
    {
        now.tv_sec  = sim_time_ns / 1000000000UL;
        now.tv_nsec = sim_time_ns % 1000000000UL;
    }
    else if (clock_gettime(CFE_PSP_TIMEBASE_REF_CLOCK, &now) != 0)
    {
        now.tv_sec  = 0;
        now.tv_nsec = 0;
    }
    
    *Tbu = now.tv_sec & 0xFFFFFFFF;
    *Tbl = now.tv_nsec;
}

void CFE_PSP_GetTime(OS_time_t *LocalTime)
{
    struct timespec now;

    /* Use simulith time if available, otherwise fall back to system time */
    pthread_mutex_lock(&tick_mutex);
    bool use_simulith_time = simulith_client_initialized && simulith_time_valid;
    if (use_simulith_time)
    {
        now.tv_sec = latest_tick_time_ns / 1000000000UL;
        now.tv_nsec = latest_tick_time_ns % 1000000000UL;
    }
    pthread_mutex_unlock(&tick_mutex);
    if (!use_simulith_time)
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
