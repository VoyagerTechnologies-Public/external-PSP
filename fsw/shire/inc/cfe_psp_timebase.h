#ifndef CFE_PSP_TIMEBASE_H
#define CFE_PSP_TIMEBASE_H

#include <stdint.h>
#include <time.h>

#include "simulith.h"  /* Simulith client API */

/* Simulith PSP time API */
void CFE_PSP_InitSimulithTime(void);
void CFE_PSP_ShutdownSimulithTime(void);
unsigned int CFE_PSP_WaitForSimulithTick(unsigned int ticks_to_wait);
uint64_t CFE_PSP_GetSimulithTimeNs(void);
void* CFE_PSP_SimulithTickDistributionThread(void* arg);
void CFE_PSP_GetSimulithTimespec(struct timespec *ts);

#endif /* CFE_PSP_TIMEBASE_H */
