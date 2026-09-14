/************************************************************************
 * SHIRE Software Bus delivery observer adapter.
 *
 * This file is selected only by the amd64-shire toolchain. Generic cFE sees
 * only the neutral observer interface and physical flight targets compile the
 * observer calls into no-ops.
 ************************************************************************/

#include "cfe.h"
#include "cfe_psp_timebase.h"
#include "cfe_sb_observer.h"

CFE_SB_ObserverToken_t CFE_SB_Observer_BeginDelivery(CFE_SB_MsgId_t         RoutingMsgId,
                                                      CFE_SB_PipeId_t        PipeId,
                                                      const CFE_SB_Buffer_t *Buffer)
{
    CFE_MSG_Type_t type;

    if (CFE_MSG_GetTypeFromMsgId(RoutingMsgId, &type) != CFE_SUCCESS ||
        type != CFE_MSG_Type_Cmd)
    {
        return CFE_SB_OBSERVER_INVALID_TOKEN;
    }

    int token = CFE_PSP_ReserveSimulithMessageDelivery(
        CFE_SB_MsgIdToValue(RoutingMsgId),
        (uint32_t)CFE_RESOURCEID_TO_ULONG(PipeId),
        Buffer);
    return token > 0 ? (CFE_SB_ObserverToken_t)token : CFE_SB_OBSERVER_INVALID_TOKEN;
}

void CFE_SB_Observer_EndDelivery(CFE_SB_ObserverToken_t Token, bool Delivered)
{
    CFE_PSP_EndSimulithMessageDelivery((int)Token, Delivered);
}

void CFE_SB_Observer_EndTransmit(void)
{
    CFE_PSP_EndSimulithMessagePublication();
}

void CFE_SB_Observer_BeginReceive(CFE_SB_PipeId_t PipeId, bool Polling)
{
    CFE_PSP_SimulithTaskBeginReceive(
        (uint32_t)CFE_RESOURCEID_TO_ULONG(PipeId), Polling);
}

void CFE_SB_Observer_MessageReceived(CFE_SB_MsgId_t         RoutingMsgId,
                                     CFE_SB_PipeId_t        PipeId,
                                     const CFE_SB_Buffer_t *Buffer)
{
    CFE_MSG_Type_t type;
    if (CFE_MSG_GetTypeFromMsgId(RoutingMsgId, &type) == CFE_SUCCESS &&
        type == CFE_MSG_Type_Cmd)
    {
        CFE_PSP_SimulithMessageReceived(CFE_SB_MsgIdToValue(RoutingMsgId),
                                        (uint32_t)CFE_RESOURCEID_TO_ULONG(PipeId),
                                        Buffer);
    }
}
