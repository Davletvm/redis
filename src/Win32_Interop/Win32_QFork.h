/*
 * Copyright (c), Microsoft Open Technologies, Inc.
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *  - Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  - Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#pragma once

#include <Windows.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum operationType {
    otINVALID = 0,
    otRDB = 1,
    otAOF = 2,
    otRDBINMEMORY = 3,
    otDBCHECK = 4,
} OperationType;

typedef enum operationStatus {
    osUNSTARTED = 0,
    osINPROGRESS = 1, // transitioned to by BeginForkOperation
    osCOMPLETE = 2,  // Complete signaled, or failed, transitioned to by GetForkOperationStatus
    osWAITINGFOREXIT = 3, // Waiting for exit (terminate signaled), transitioned to by StartEndForkProcess
    osEXITED = 4,   // Exited - transitioned to by GetForkOperationStatus
    osCLEANING = 5, // transitioned to by manual call to clean
    osCLEANEDUP = 6, // transitinoed to by manual call to clean
    osMASK      = 0x0FFF,
    osFAILED    = 0x1000,
    osINMEMORY  = 0x2000,
    
} OperationStatus;

typedef enum startupStatus {
    ssFAILED = 0,                 // Something went wrong, exit program with error.
    ssCONTINUE_AS_MASTER = 1,     // Master qfork initialization complete, continue as master instance. Call QForkShutdown when exiting.
    ssSLAVE_EXIT = 2              // Slave completed operation. Call QForkShutdown and exit.
} StartupStatus;

typedef struct {
    void * heapStart;
    void * heapEnd;
} HeapExtent;

#define MAX_GLOBAL_DATA 10000
typedef struct QForkBeginInfo {
    char filename[MAX_PATH];
    BYTE globalData[MAX_GLOBAL_DATA];
    size_t globalDataSize;
    unsigned __int32 dictHashSeed;
} QForkStartupInfo;
    
StartupStatus QForkStartup(int argc, char** argv);
BOOL QForkShutdown();


// For master process use only
BOOL BeginForkOperation(OperationType type, char* fileName, int sendBufferSize, LPVOID globalData, int sizeOfGlobalData, DWORD* childPID, unsigned __int32 dictHashSeed);
OperationStatus GetForkOperationStatus(BOOL forceEnd);
void EndForkOperation(int * pExitCode);
void AbortForkOperation(BOOL blockUntilCleanedUp);
void AdvanceCleanupForkOperation(BOOL forceEnd, int *exitCode);
void ClearInMemoryBuffersMasterParent();
void GetHeapExtent(HeapExtent * pextent);
void GetCOWStats(int * cowPages, int * copiedPages, int * scannedPages, int * totalPages);

// For DLMalloc use only
LPVOID AllocHeapBlockMap(size_t size, BOOL allocateHigh);
int FreeHeapBlockMap(LPVOID block, size_t size);
LPVOID AllocHeapBlockMoreCore(size_t size);

// Need to change default Redis maxmemory setting which is normally infinite
extern BOOL g_isForkedProcess;

#ifndef NO_QFORKIMPL
#ifdef QFORK_MAIN_IMPL
int redis_main(int argc, char** argv);
#else
#define main redis_main
#endif
#endif

#ifdef __cplusplus
}
#endif
