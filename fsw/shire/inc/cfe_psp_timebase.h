#ifndef CFE_PSP_TIMEBASE_H
#define CFE_PSP_TIMEBASE_H

#include <stdint.h>
#include <stdbool.h>
#include <time.h>

/* Simulith PSP time API */
void CFE_PSP_InitSimulithTime(void);
int CFE_PSP_StartSynchronizedTicks(void);
void CFE_PSP_StopSynchronizedTicks(void);
void CFE_PSP_ShutdownSimulithTime(void);
unsigned int CFE_PSP_WaitForSimulithTick(unsigned int ticks_to_wait);
int CFE_PSP_WaitForPendingSimulithTick(void);
void CFE_PSP_EnableDeferredTickCompletion(void);
int CFE_PSP_RegisterSimulithParticipant(uint32_t schedule_entry, uint32_t message_id);
int CFE_PSP_ReserveSimulithMessageDelivery(uint32_t message_id,
                                           uint32_t pipe_id,
                                           const void *message_ref);
void CFE_PSP_EndSimulithMessageDelivery(int token, bool delivered);
void CFE_PSP_EndSimulithMessagePublication(void);
void CFE_PSP_CancelSimulithParticipant(int token);
void CFE_PSP_SimulithTaskBeginReceive(uint32_t pipe_id, bool polling);
void CFE_PSP_SimulithMessageReceived(uint32_t message_id,
                                     uint32_t pipe_id,
                                     const void *message_ref);
int CFE_PSP_WaitForSimulithParticipants(void);
bool CFE_PSP_AllowPeriodicGroundOutput(uint32_t message_id);
int CFE_PSP_CompleteSimulithTick(void);
uint64_t CFE_PSP_GetSimulithTimeNs(void);
void* CFE_PSP_SimulithTickDistributionThread(void* arg);
void CFE_PSP_GetSimulithTimespec(struct timespec *ts);

#endif /* CFE_PSP_TIMEBASE_H */
