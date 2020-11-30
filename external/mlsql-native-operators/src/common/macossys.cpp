#include "macossys.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <mach/mach.h>



// Returns 1.0f for "CPU fully pinned", 0.0f for "CPU idle", or somewhere in between
// You'll need to call this at regular intervals, since it measures the load between
// the previous call and the current one.
float GetCPULoad()
{
    unsigned long ulSystemPrevious;
    unsigned long ulUserPrevious;
    unsigned long ulNicePrevious;
    unsigned long ulIdlePrevious;
    auto nWaitSec = 1;
    int nErr = CalculateCPULoad(&ulSystemPrevious, &ulUserPrevious, &ulNicePrevious, &ulIdlePrevious);
    if(nErr != KERN_SUCCESS)
    {
        printf("Kernel error: %s\n", mach_error_string(nErr));
        return -1;
    }
    sleep(nWaitSec);
    unsigned long ulSystemNext;
    unsigned long ulUserNext;
    unsigned long ulNiceNext;
    unsigned long ulIdleNext;
    nErr = CalculateCPULoad(&ulSystemNext, &ulUserNext, &ulNiceNext, &ulIdleNext);
    if(nErr != KERN_SUCCESS)
    {
        printf("Kernel error: %s\n", mach_error_string(nErr));
        return -1;
    }
    float fUsageTime = (float)(ulSystemNext - ulSystemPrevious) + (ulUserNext - ulUserPrevious) + (ulNiceNext - ulNicePrevious);
    float fTotalTime = fUsageTime + (float)(ulIdleNext - ulIdlePrevious);
    return fUsageTime/fTotalTime*100;
}

int CalculateCPULoad(unsigned long * pulSystem, unsigned long * pulUser, unsigned long * pulNice, unsigned long * pulIdle)
{
    mach_msg_type_number_t  unCpuMsgCount = 0;
    processor_flavor_t nCpuFlavor = PROCESSOR_CPU_LOAD_INFO;;
    kern_return_t   nErr = 0;
    natural_t unCPUNum = 0;
    processor_cpu_load_info_t structCpuData;
    host_t host = mach_host_self();
    *pulSystem = 0;
    *pulUser = 0;
    *pulNice = 0;
    *pulIdle = 0;
    nErr = host_processor_info( host,nCpuFlavor,&unCPUNum,
                                (processor_info_array_t *)&structCpuData,&unCpuMsgCount );
    for(int i = 0; i<(int)unCPUNum; i++)
    {
        *pulSystem += structCpuData[i].cpu_ticks[CPU_STATE_SYSTEM];
        *pulUser += structCpuData[i].cpu_ticks[CPU_STATE_USER];
        *pulNice += structCpuData[i].cpu_ticks[CPU_STATE_NICE];
        *pulIdle += structCpuData[i].cpu_ticks[CPU_STATE_IDLE];
    }
    return nErr;
}

