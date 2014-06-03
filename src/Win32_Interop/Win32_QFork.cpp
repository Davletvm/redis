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

#include <Windows.h>
#include <errno.h>
#include <stdio.h>
#include <wchar.h>
#include <Psapi.h>

#define QFORK_MAIN_IMPL
#include "Win32_QFork.h"

#include "Win32_QFork_impl.h"
#include "Win32_dlmalloc.h"
#include "Win32_SmartHandle.h"
#include <vector>
#include <iostream>
#include <fstream>
#include <sstream>
#include "..\redisLog.h"
using namespace std;

//#define DEBUG_WITH_PROCMON
#ifdef DEBUG_WITH_PROCMON
#define FILE_DEVICE_PROCMON_LOG 0x00009535
#define IOCTL_EXTERNAL_LOG_DEBUGOUT (ULONG) CTL_CODE( FILE_DEVICE_PROCMON_LOG, 0x81, METHOD_BUFFERED, FILE_WRITE_ACCESS )

HANDLE hProcMonDevice = INVALID_HANDLE_VALUE;
BOOL WriteToProcmon (wstring message)
{
    if (hProcMonDevice != INVALID_HANDLE_VALUE) {
        DWORD nb = 0;
        return DeviceIoControl(
            hProcMonDevice, 
            IOCTL_EXTERNAL_LOG_DEBUGOUT,
            (LPVOID)(message.c_str()),
            (DWORD)(message.length() * sizeof(wchar_t)),
            NULL,
            0,
            &nb,
            NULL);
    } else {
        return FALSE;
    }
}
#endif

/*
Redis is an in memory DB. We need to share the redis database with a quasi-forked process so that we can do the RDB and AOF saves 
without halting the main redis process, or crashing due to code that was never designed to be thread safe. Essentially we need to
replicate the COW behavior of fork() on Windows, but we don't actually need a complete fork() implementation. A complete fork() 
implementation would require subsystem level support to make happen. The following is required to make this quasi-fork scheme work:

DLMalloc (http://g.oswego.edu/dl/html/malloc.html):
    - replaces malloc/realloc/free, either by manual patching of the zmalloc code in Redis or by patching the CRT routines at link time
    - partitions space into segments that it allocates from (currently configured as 64MB chunks)
    - we map/unmap these chunks as requested into a memory map (unmapping allows the system to decide how to reduce the physical memory 
      pressure on system)

DLMallocMemoryMap:
   - An uncomitted memory map whose size is the total physical memory on the system less some memory for the rest of the system so that 
     we avoid excessive swapping.
   - This is reserved high in VM space so that it can be mapped at a specific address in the child qforked process (ASLR must be 
     disabled for these processes)
   - This must be mapped in exactly the same virtual memory space in both forker and forkee.

QForkConrolMemoryMap:
   - contains a map of the allocated segments in the DLMallocMemoryMap
   - contains handles for inter-process synchronization
   - contains pointers to some of the global data in the parent process if mapped into DLMallocMemoryMap, and a copy of any other 
     required global data

QFork process:
    - a copy of the parent process with a command line specifying QFork behavior
    - when a COW operation is requested via an event signal
        - opens the DLMAllocMemoryMap with PAGE_WRITECOPY
        - reserve space for DLMAllocMemoryMap at the memory location specified in ControlMemoryMap
        - locks the DLMalloc segments as specified in QForkConrolMemoryMap 
        - maps global data from the QForkConrolMEmoryMap into this process
        - executes the requested operation
        - unmaps all the mm views (discarding any writes)
        - signals the parent when the operation is complete

How the parent invokes the QFork process:
    - protects mapped memory segments with VirtualProtect using PAGE_WRITECOPY (both the allocated portions of DLMAllocMemoryMap and 
      the QForkConrolMemoryMap)
    - QForked process is signaled to process command
    - Parent waits (asynchronously) until QForked process signals that operation is complete, then as an atomic operation:
        - signals and waits for the forked process to terminate
        - resotres protection status on mapped blocks
        - determines which pages have been modified and copies these to a buffer
        - unmaps the view of the heap (discarding COW changes form the view)
        - remaps the view
        - copies the changes back into the view
*/

#ifndef LODWORD
    #define LODWORD(_qw)    ((DWORD)(_qw))
#endif
#ifndef HIDWORD
    #define HIDWORD(_qw)    ((DWORD)(((_qw) >> (sizeof(DWORD)*8)) & DWORD(~0)))
#endif

const SIZE_T cAllocationGranularity = 1 << 26;                   // 64MB per dlmalloc heap block 
const int cMaxBlocks = 1 << 16;                                  // 64MB*64K sections = 4TB. 4TB is the largest memory config Windows supports at present.
const wchar_t* cMapFileBaseName = L"RedisQFork";
const char* qforkFlag = "--QFork";
const int cDeadForkWait = 30000;
const size_t pageSize = 4096;

const char* maxvirtualmemoryflag = "maxvirtualmemory";

typedef enum BlockState {
    bsINVALID = 0,
    bsUNMAPPED = 1,   
    bsMAPPED = 2
}BlockState;



struct QForkControl {
    HANDLE heapMemoryMapFile;
    HANDLE heapMemoryMap;
    int availableBlocksInHeap;                 // number of blocks in blockMap (dynamically determined at run time)
    SIZE_T heapBlockSize;           
    BlockState heapBlockMap[cMaxBlocks];
    LPVOID heapStart;

    OperationType typeOfOperation;
    HANDLE forkedProcessReady;
    HANDLE startOperation;
    HANDLE operationComplete;
    HANDLE operationFailed;
    HANDLE terminateForkedProcess;

    HANDLE inMemoryBuffersControlHandle;
    InMemoryBuffersControl * inMemoryBuffersControl;
    int inMemoryBuffersControlOffset;
    HANDLE doSendBuffer[2];
    HANDLE doneSentBuffer[2];

    // global data pointers to be passed to the forked process
    QForkBeginInfo globalData;
};

QForkControl* g_pQForkControl = NULL;
HANDLE g_hQForkControlFileMap = NULL;

HANDLE g_hForkedProcess = NULL;
HANDLE g_hEndForkThread = NULL;
SIZE_T g_win64maxmemory = 0;
SIZE_T g_win64maxvirtualmemory = 0;
BOOL g_isForkedProcess = FALSE;
int g_SlaveExitCode = 0; // For slave process

extern "C"
{
    // forward def from util.h. 
    long long memtoll(const char *p, int *err);
}





BOOL QForkSlaveInit(HANDLE QForkConrolMemoryMapHandle, DWORD ParentProcessID) {
    SmartHandle shParent;
    SmartHandle shMMFile;
    SmartFileView<QForkControl> sfvMasterQForkControl;
    SmartHandle dupHeapFileHandle;
    SmartHandle dupForkedProcessReady; 
    SmartHandle dupStartOperation;
    SmartHandle dupOperationComplete;
    SmartHandle dupOperationFailed;
    SmartHandle dupTerminateProcess;
    SmartFileMapHandle sfmhMapFile;
    SmartFileView<byte> sfvHeap;
    SmartHandle sfMMFileInMemoryControlHandle;
    SmartFileView<InMemoryBuffersControl> sfvInMemory;
    SmartHandle dupSendBuffer0;
    SmartHandle dupSendBuffer1;
    SmartHandle dupSentBuffer0;
    SmartHandle dupSentBuffer1;

    try {
        g_isForkedProcess = TRUE;
        shParent.Assign( 
            OpenProcess(SYNCHRONIZE | PROCESS_DUP_HANDLE, TRUE, ParentProcessID),
            string("Could not open parent process"));

        shMMFile.Assign(shParent, QForkConrolMemoryMapHandle);
        sfvMasterQForkControl.Assign( 
            shMMFile, 
            FILE_MAP_COPY, 
            string("Could not map view of QForkControl in slave. Is system swap file large enough?"));
        g_pQForkControl = sfvMasterQForkControl;

        // duplicate handles and stuff into control structure (master protected by PAGE_WRITECOPY)
        dupHeapFileHandle.Assign(shParent, sfvMasterQForkControl->heapMemoryMapFile);
        g_pQForkControl->heapMemoryMapFile = dupHeapFileHandle;
        dupForkedProcessReady.Assign(shParent,sfvMasterQForkControl->forkedProcessReady);
        g_pQForkControl->forkedProcessReady = dupForkedProcessReady;
        dupStartOperation.Assign(shParent, sfvMasterQForkControl->startOperation);
        g_pQForkControl->startOperation = dupStartOperation;
        dupOperationComplete.Assign(shParent, sfvMasterQForkControl->operationComplete);
        g_pQForkControl->operationComplete = dupOperationComplete;
        dupOperationFailed.Assign(shParent, sfvMasterQForkControl->operationFailed);
        g_pQForkControl->operationFailed = dupOperationFailed;
        dupTerminateProcess.Assign(shParent, sfvMasterQForkControl->terminateForkedProcess);
        g_pQForkControl->terminateForkedProcess = dupTerminateProcess;

        // signal parent that we are ready.  We can do the rest later.
        SetEvent(g_pQForkControl->forkedProcessReady);


        // create section handle on MM file
        SIZE_T mmSize = g_pQForkControl->availableBlocksInHeap * cAllocationGranularity;
        sfmhMapFile.Assign(
           g_pQForkControl->heapMemoryMapFile, 
           PAGE_READONLY, 
           HIDWORD(mmSize), LODWORD(mmSize),
           string("QForkSlaveInit: Could not open file mapping object in slave"));
        g_pQForkControl->heapMemoryMap = sfmhMapFile;

        sfvHeap.Assign(
            g_pQForkControl->heapMemoryMap,
            FILE_MAP_READ,
            0, 0, 0,
            g_pQForkControl->heapStart,
            string("QForkSlaveInit: Could not map heap in forked process. Is system swap file large enough?"));

        if (g_pQForkControl->inMemoryBuffersControlHandle) {
            sfMMFileInMemoryControlHandle.Assign(shParent, g_pQForkControl->inMemoryBuffersControlHandle);
            g_pQForkControl->inMemoryBuffersControlHandle = sfMMFileInMemoryControlHandle;

            sfvInMemory.Assign(g_pQForkControl->inMemoryBuffersControlHandle, FILE_MAP_ALL_ACCESS, 0, 0, 0, string("QForkSlaveInit: Could not map inmemory buffers in forked process. Is system swap file large enough?"));
            g_pQForkControl->inMemoryBuffersControl = sfvInMemory;

            g_pQForkControl->inMemoryBuffersControl->buffer[0] = (SPBuffer*)(((char*)g_pQForkControl->inMemoryBuffersControl) + sizeof(InMemoryBuffersControl));
            g_pQForkControl->inMemoryBuffersControl->buffer[1] = (SPBuffer*)(((char*)g_pQForkControl->inMemoryBuffersControl) + g_pQForkControl->inMemoryBuffersControlOffset);
        }
        dupSendBuffer0.Assign(shParent, sfvMasterQForkControl->doSendBuffer[0]);
        g_pQForkControl->doSendBuffer[0] = dupSendBuffer0;
        dupSendBuffer1.Assign(shParent, sfvMasterQForkControl->doSendBuffer[1]);
        g_pQForkControl->doSendBuffer[1] = dupSendBuffer1;
        dupSentBuffer0.Assign(shParent, sfvMasterQForkControl->doneSentBuffer[0]);
        g_pQForkControl->doneSentBuffer[0] = dupSentBuffer0;
        dupSentBuffer1.Assign(shParent, sfvMasterQForkControl->doneSentBuffer[1]);
        g_pQForkControl->doneSentBuffer[1] = dupSentBuffer1;



        if (dlmallopt(M_MMAP_THRESHOLD, cAllocationGranularity) == 0) {
            throw std::system_error(
                GetLastError(),
                system_category(),
                "QForkMasterInit: DLMalloc failed initializing direct memory map threshold.");
        }
        if (dlmallopt(M_GRANULARITY, cAllocationGranularity) == 0) {
            throw std::system_error(
                GetLastError(),
                system_category(),
                "QForkmasterinit: DLMalloc failed initializing allocation granularity.");
        }


        
        // wait for parent to signal operation start
        WaitForSingleObject(g_pQForkControl->startOperation, INFINITE);

        // copy redis globals into fork process
        SetupGlobals(g_pQForkControl->globalData.globalData, g_pQForkControl->globalData.globalDataSize, g_pQForkControl->globalData.dictHashSeed);
        
        // execute requiested operation
        int exitCode;
        if (g_pQForkControl->typeOfOperation == OperationType::otRDB) {
            exitCode = do_rdbSave(g_pQForkControl->globalData.filename);
        } else if (g_pQForkControl->typeOfOperation == OperationType::otAOF) {
            exitCode = do_aofSave(g_pQForkControl->globalData.filename);
        } else if (g_pQForkControl->typeOfOperation == OperationType::otRDBINMEMORY) {
            exitCode = do_rdbSaveInMemory(g_pQForkControl->inMemoryBuffersControl, g_pQForkControl->doSendBuffer, g_pQForkControl->doneSentBuffer);
        } else {
            throw runtime_error("unexpected operation type");
        }
        g_SlaveExitCode = exitCode;

        // let parent know weare done
        SetEvent(g_pQForkControl->operationComplete);

        // parent will notify us when to quit
        WaitForSingleObject(g_pQForkControl->terminateForkedProcess, INFINITE);

        redisLog(REDIS_NOTICE, "Successfully completed background operation.  Exiting child.");

        g_pQForkControl = NULL;
        return TRUE;
    }
    catch(std::system_error syserr) {
        if(g_pQForkControl != NULL) {
            if(g_pQForkControl->operationFailed != NULL) {
                SetEvent(g_pQForkControl->operationFailed);
            }
            g_pQForkControl = NULL;
        }
        return FALSE;
    }
    catch(std::runtime_error runerr) {
        SetEvent(g_pQForkControl->operationFailed);
        g_pQForkControl = NULL;
        return FALSE;
    }
    return FALSE;
}


void CreateEventHandle(HANDLE * out) {
    HANDLE h = CreateEvent(NULL, TRUE, FALSE, NULL);
    if (h == NULL) {
        throw std::system_error(
            GetLastError(),
            system_category(),
            "QForkMasterInit: CreateEvent failed.");
    }
    *out = h;
}


BOOL QForkMasterInit( __int64 maxMemoryVirtualBytes) {

    // This will be reset to the correct value when config is processed
    setLogVerbosityLevel(REDIS_WARNING);

    try {
        // allocate file map for qfork control so it can be passed to the forked process
        g_hQForkControlFileMap = CreateFileMappingW(
            INVALID_HANDLE_VALUE,
            NULL,
            PAGE_READWRITE,
            0, sizeof(QForkControl),
            NULL);
        if (g_hQForkControlFileMap == NULL) {
            throw std::system_error(
                GetLastError(),
                system_category(),
                "QForkMasterInit: CreateFileMapping failed");
        }

        g_pQForkControl = (QForkControl*)MapViewOfFile(
            g_hQForkControlFileMap, 
            FILE_MAP_ALL_ACCESS,
            0, 0,
            0);
        if (g_pQForkControl == NULL) {
            throw std::system_error(
                GetLastError(),
                system_category(),
                "QForkMasterInit: MapViewOfFile failed");
        }

        g_pQForkControl->inMemoryBuffersControl = NULL;
        g_pQForkControl->inMemoryBuffersControlHandle = NULL;


        // This must be called only once per process! Calling it more times than that will not recreate existing 
        // section, and dlmalloc will ultimately fail with an access violation. Once is good.
        if (dlmallopt(M_MMAP_THRESHOLD, cAllocationGranularity) == 0) {
            throw std::system_error(
                GetLastError(),
                system_category(),
                "QForkMasterInit: DLMalloc failed initializing direct memory map threshold.");
        }
        if (dlmallopt(M_GRANULARITY, cAllocationGranularity) == 0) {
            throw std::system_error(
                GetLastError(),
                system_category(),
                "QForkMasterInit: DLMalloc failed initializing allocation granularity.");
        }
        g_pQForkControl->heapBlockSize = cAllocationGranularity;

        // determine the number of blocks we can allocate (heap must be completely mappable in physical memory for qfork to succeed)
        PERFORMANCE_INFORMATION perfinfo;
        perfinfo.cb = sizeof(PERFORMANCE_INFORMATION);
        if (FALSE == GetPerformanceInfo(&perfinfo, sizeof(PERFORMANCE_INFORMATION))) {
            throw system_error(GetLastError(), system_category(), "GetPerformanceInfo failed");
        }
        SIZE_T maxSystemReservePages = 3i64 * 1024i64 * 1024i64 * 1024i64 / pageSize;
        SIZE_T maxPhysicalPagesToUse = perfinfo.PhysicalTotal;
        SIZE_T maxPhysicalMapping = (maxPhysicalPagesToUse * pageSize * 14i64) / 10i64;
        g_win64maxmemory = maxPhysicalPagesToUse * pageSize;
        if (maxMemoryVirtualBytes != -1) {
            SIZE_T allocationBlocks = maxMemoryVirtualBytes / cAllocationGranularity;
            allocationBlocks += ((maxMemoryVirtualBytes % cAllocationGranularity) != 0);
            allocationBlocks = max(2, allocationBlocks);
            maxPhysicalMapping = allocationBlocks * cAllocationGranularity;
            g_win64maxmemory = maxPhysicalMapping * 7i64 / 10i64;
        }
        g_pQForkControl->availableBlocksInHeap = (int)(maxPhysicalMapping / cAllocationGranularity);
        g_win64maxvirtualmemory = g_pQForkControl->availableBlocksInHeap * cAllocationGranularity;
        if (g_pQForkControl->availableBlocksInHeap <= 0) {
            throw std::runtime_error(
                "QForkMasterInit: Not enough physical memory to initialize Redis.");
        }

        wchar_t heapMemoryMapPath[MAX_PATH];
        swprintf_s( 
            heapMemoryMapPath, 
            MAX_PATH, 
            L"%s_%d.dat", 
            cMapFileBaseName, 
            GetCurrentProcessId());


        g_pQForkControl->heapMemoryMapFile = 
            CreateFileW( 
                heapMemoryMapPath,
                GENERIC_READ | GENERIC_WRITE,
                0,
                NULL,
                CREATE_ALWAYS,
                FILE_ATTRIBUTE_NORMAL| FILE_FLAG_DELETE_ON_CLOSE,
                NULL );
        if (g_pQForkControl->heapMemoryMapFile == INVALID_HANDLE_VALUE) {
            throw std::system_error(
                GetLastError(),
                system_category(),
                "QForkMasterInit: CreateFileW failed.");
        }

        SIZE_T mmSize = g_pQForkControl->availableBlocksInHeap * cAllocationGranularity;
        g_pQForkControl->heapMemoryMap = 
            CreateFileMappingW( 
                g_pQForkControl->heapMemoryMapFile,
                NULL,
                PAGE_READWRITE,
                HIDWORD(mmSize),
                LODWORD(mmSize),
                NULL);
        if (g_pQForkControl->heapMemoryMap == NULL) {
            throw std::system_error(
                GetLastError(),
                system_category(),
                "QForkMasterInit: CreateFileMapping failed.");
        }
            
        // Find a place in the virtual memory space where we can reserve space for our allocations that is likely
        // to be available in the forked process.  (If this ever fails in the forked process, we will have to launch
        // the forked process and negotiate for a shared memory address here.)
        LPVOID pHigh = VirtualAllocEx( 
            GetCurrentProcess(),
            NULL,
            mmSize,
            MEM_RESERVE | MEM_TOP_DOWN, 
            PAGE_READWRITE);
        if (pHigh == NULL) {
            throw std::system_error(
                GetLastError(),
                system_category(),
                "QForkMasterInit: VirtualAllocEx failed.");
        }
        if (VirtualFree(pHigh, 0, MEM_RELEASE) == FALSE) {
            throw std::system_error(
                GetLastError(),
                system_category(),
                "QForkMasterInit: VirtualFree failed.");
        }

        g_pQForkControl->heapStart = 
            MapViewOfFileEx(
                g_pQForkControl->heapMemoryMap,
                FILE_MAP_ALL_ACCESS,
                0,0,                            
                0,  
                pHigh);
        if (g_pQForkControl->heapStart == NULL) {
            throw std::system_error(
                GetLastError(),
                system_category(),
                "QForkMasterInit: MapViewOfFileEx failed.");
        }

        for (int n = 0; n < cMaxBlocks; n++) {
            g_pQForkControl->heapBlockMap[n] = 
                ((n < g_pQForkControl->availableBlocksInHeap) ?
                BlockState::bsUNMAPPED : BlockState::bsINVALID);
        }

        g_pQForkControl->typeOfOperation = OperationType::otINVALID;

        CreateEventHandle(&g_pQForkControl->forkedProcessReady);
        CreateEventHandle(&g_pQForkControl->startOperation);
        CreateEventHandle(&g_pQForkControl->operationComplete);
        CreateEventHandle(&g_pQForkControl->operationFailed);
        CreateEventHandle(&g_pQForkControl->terminateForkedProcess);
        CreateEventHandle(&g_pQForkControl->doSendBuffer[0]);
        CreateEventHandle(&g_pQForkControl->doSendBuffer[1]);
        CreateEventHandle(&g_pQForkControl->doneSentBuffer[0]);
        CreateEventHandle(&g_pQForkControl->doneSentBuffer[1]);
        
        return TRUE;
    }
    catch(std::system_error syserr) {
        redisLog(REDIS_WARNING, "QForkMasterInit: system error caught. error code=0x%08x, message=%s", syserr.code().value(), syserr.what());
    }
    catch(std::runtime_error runerr) {
        redisLog(REDIS_WARNING, "QForkMasterInit: runtime error caught. message=%s", runerr.what());
    }
    catch(...) {
        redisLog(REDIS_WARNING, "QForkMasterInit: other exception caught.");
    }
    return FALSE;
}



// QFork API
StartupStatus QForkStartup(int argc, char** argv) {
    bool foundSlaveFlag = false;
    HANDLE QForkConrolMemoryMapHandle = NULL;
    DWORD PPID = 0;
    __int64 maxvirtualmemory = -1;
    int memtollerr;
    if ((argc == 3) && (strcmp(argv[0], qforkFlag) == 0)) {
        // slave command line looks like: --QFork [QForkConrolMemoryMap handle] [parent process id]
        foundSlaveFlag = true;
        char* endPtr;
        QForkConrolMemoryMapHandle = (HANDLE)strtoul(argv[1],&endPtr,10);
        char* end = NULL;
        PPID = strtoul(argv[2], &end, 10);
    } else {
        bool maxMemoryFlagFound = false;
        for (int n = 1; n < argc; n++) {
            // check for maxmemory + reserve bypass flags in .conf file
            if( n == 1  && strncmp(argv[n],"--",2) != 0 ) {
                ifstream config;
                config.open(argv[n]);
                if (config.fail())
                    continue;
                while (!config.eof()) {
                    string line;
                    getline(config,line);
                    istringstream iss(line);
                    string token;
                    if (getline(iss, token, ' ')) {
                        if (_stricmp(token.c_str(), maxvirtualmemoryflag) == 0) {
                            string maxmemoryString;
                            if (getline(iss, maxmemoryString, ' ')) {
                                maxvirtualmemory = memtoll(maxmemoryString.c_str(), &memtollerr);
                                if (memtollerr != 0) {
                                    redisLog(REDIS_WARNING, 
                                        "%s specified. Unable to convert %s to the maximum number of bytes to use for the heap.", 
                                        maxvirtualmemoryflag,
                                        maxmemoryString.c_str());
                                    redisLog(REDIS_WARNING, "Failing startup.");
                                    return StartupStatus::ssFAILED;
                                }
                                maxMemoryFlagFound = true;
                            }
                        }
                    }
                }
                continue;
            }
            if( strncmp(argv[n],"--", 2) == 0) {
                if (_stricmp(argv[n]+2,maxvirtualmemoryflag) == 0) {
                    if (n + 1 >= argc) {
                        redisLog(REDIS_WARNING,
                            "%s specified without a size.", 
                            argv[n] );
                        redisLog(REDIS_WARNING, "Failing startup.");
                        return StartupStatus::ssFAILED;
                    }
                    maxvirtualmemory = memtoll(argv[n+1], &memtollerr);
                    if (memtollerr != 0) {
                        redisLog(REDIS_WARNING,
                            "%s specified. Unable to convert %s to the maximum number of bytes to use for the heap.", 
                            argv[n],
                            argv[n+1] );
                        redisLog(REDIS_WARNING, "Failing startup.");
                        return StartupStatus::ssFAILED;
                    } 
                    maxMemoryFlagFound = true;
                }
            }
        }
    }

    if (foundSlaveFlag) {
        return QForkSlaveInit( QForkConrolMemoryMapHandle, PPID ) ? StartupStatus::ssSLAVE_EXIT : StartupStatus::ssFAILED;
    } else {
        return QForkMasterInit(maxvirtualmemory) ? StartupStatus::ssCONTINUE_AS_MASTER : StartupStatus::ssFAILED;
    }
}

void CloseEventHandle(HANDLE * phandle)
{
    if (*phandle != NULL) {
        CloseHandle(*phandle);
        *phandle = NULL;
    }
}

BOOL QForkShutdown() {
    if(g_hForkedProcess != NULL) {
        TerminateProcess(g_hForkedProcess, -1);
        g_hForkedProcess = NULL;
    }

    if( g_pQForkControl != NULL )
    {
        CloseEventHandle(&g_pQForkControl->forkedProcessReady);
        CloseEventHandle(&g_pQForkControl->startOperation);
        CloseEventHandle(&g_pQForkControl->operationComplete);
        CloseEventHandle(&g_pQForkControl->operationFailed);
        CloseEventHandle(&g_pQForkControl->terminateForkedProcess);
        CloseEventHandle(&g_pQForkControl->heapMemoryMap);
        CloseEventHandle(&g_pQForkControl->doneSentBuffer[0]);
        CloseEventHandle(&g_pQForkControl->doneSentBuffer[1]);
        CloseEventHandle(&g_pQForkControl->doSendBuffer[0]);
        CloseEventHandle(&g_pQForkControl->doSendBuffer[1]);

        if (g_pQForkControl->heapMemoryMapFile != INVALID_HANDLE_VALUE) {
            CloseHandle(g_pQForkControl->heapMemoryMapFile);
            g_pQForkControl->heapMemoryMapFile = INVALID_HANDLE_VALUE;
        }
        if (g_pQForkControl->heapStart != NULL) {
            UnmapViewOfFile(g_pQForkControl->heapStart);
            g_pQForkControl->heapStart = NULL;
        }
        if(g_pQForkControl != NULL) {
            UnmapViewOfFile(g_pQForkControl);
            g_pQForkControl = NULL;
        }
        CloseEventHandle(&g_hQForkControlFileMap);
    }
    return TRUE;
}

void ResetEventHandle(HANDLE event) {
    if (ResetEvent(event) == FALSE) {
        throw std::system_error(
            GetLastError(),
            system_category(),
            "BeginForkOperation: ResetEvent() failed.");
    }
}


void ForceEndForkProcess()
{
    if (!g_hEndForkThread)
        return;
    if (WaitForSingleObject(g_hEndForkThread, 0) == WAIT_TIMEOUT) {
        redisLog(REDIS_DEBUG, "Waiting for EndForkAsync thread");
        SetThreadPriority(g_hEndForkThread, THREAD_PRIORITY_HIGHEST);
        WaitForSingleObject(g_hEndForkThread, INFINITE);
        redisLog(REDIS_DEBUG, "EndForkAsync thread completed");
    }
    CloseHandle(g_hEndForkThread);
    g_hEndForkThread = NULL;

    LPVOID controlCopy = malloc(sizeof(QForkControl));
    if (controlCopy == NULL) {
        throw std::system_error(
            GetLastError(),
            system_category(),
            "EndForkOperation: allocation failed.");
    }
    memcpy(controlCopy, g_pQForkControl, sizeof(QForkControl));
    if (UnmapViewOfFile(g_pQForkControl) == FALSE) {
        throw std::system_error(
            GetLastError(),
            system_category(),
            "EndForkOperation: UnmapViewOfFile failed.");
    }
    g_pQForkControl = (QForkControl*)
        MapViewOfFileEx(
        g_hQForkControlFileMap,
        FILE_MAP_ALL_ACCESS,
        0, 0,
        0,
        g_pQForkControl);
    if (g_pQForkControl == NULL) {
        throw std::system_error(
            GetLastError(),
            system_category(),
            "EndForkOperation: Remapping ForkControl failed.");
    }
    memcpy(g_pQForkControl, controlCopy, sizeof(QForkControl));
    delete controlCopy;
    controlCopy = NULL;

    if (g_hForkedProcess) {
        CloseHandle(g_hForkedProcess);
        g_hForkedProcess = NULL;
    }
}



BOOL BeginForkOperation(OperationType type, char* fileName, int sendBufferSize, LPVOID globalData, int sizeOfGlobalData, DWORD* childPID, uint32_t dictHashSeed) {
    try {

        redisLog(REDIS_NOTICE, "Starting to fork parent process.");
        ForceEndForkProcess();
        // copy operation data
        if (fileName) {
            strcpy_s(g_pQForkControl->globalData.filename, fileName);
        } else {
            type = otRDBINMEMORY;
            g_pQForkControl->globalData.filename[0] = 0;
        }
        g_pQForkControl->typeOfOperation = type;
        if (sizeOfGlobalData > MAX_GLOBAL_DATA) {
            throw std::runtime_error("Global state too large.");
        }
        memcpy(&(g_pQForkControl->globalData.globalData), globalData, sizeOfGlobalData);
        g_pQForkControl->globalData.globalDataSize = sizeOfGlobalData;
        g_pQForkControl->globalData.dictHashSeed = dictHashSeed;

        // ensure events are in the correst state
        ResetEventHandle(g_pQForkControl->operationComplete);
        ResetEventHandle(g_pQForkControl->operationFailed);
        ResetEventHandle(g_pQForkControl->startOperation);
        ResetEventHandle(g_pQForkControl->forkedProcessReady);
        ResetEventHandle(g_pQForkControl->terminateForkedProcess);
        ResetEventHandle(g_pQForkControl->doSendBuffer[0]);
        ResetEventHandle(g_pQForkControl->doSendBuffer[1]);
        ResetEventHandle(g_pQForkControl->doneSentBuffer[0]);
        ResetEventHandle(g_pQForkControl->doneSentBuffer[1]);

        if (type == otRDBINMEMORY) {

            size_t size = sizeof(InMemoryBuffersControl);
            if (sendBufferSize < 1024) sendBufferSize = 1024;
            size_t SPBufferSize = offsetof(SPBuffer, b) + sendBufferSize;
            SPBufferSize = (SPBufferSize + 0x0f) & ~0x0f;
            size += SPBufferSize * 2;
            g_pQForkControl->inMemoryBuffersControlOffset = sizeof(InMemoryBuffersControl)+SPBufferSize;

            if (g_pQForkControl->inMemoryBuffersControl) {
                if (!UnmapViewOfFile(g_pQForkControl->inMemoryBuffersControl)) {
                    throw std::system_error(
                        GetLastError(),
                        system_category(),
                        "BeginForkOperation: UnmapViewOfFile failed");
                }
                g_pQForkControl->inMemoryBuffersControl = NULL;
            }

            if (g_pQForkControl->inMemoryBuffersControlHandle) {
                CloseHandle(g_pQForkControl->inMemoryBuffersControlHandle);
                g_pQForkControl->inMemoryBuffersControlHandle = NULL;
            }

            g_pQForkControl->inMemoryBuffersControlHandle = CreateFileMappingW(
                INVALID_HANDLE_VALUE,
                NULL,
                PAGE_READWRITE,
                HIDWORD(size), LODWORD(size),
                NULL);
            if (g_pQForkControl->inMemoryBuffersControlHandle == NULL) {
                throw std::system_error(
                    GetLastError(),
                    system_category(),
                    "BeginForkOperation: CreateFileMapping failed");
            }

            g_pQForkControl->inMemoryBuffersControl = (InMemoryBuffersControl*)MapViewOfFile(
                g_pQForkControl->inMemoryBuffersControlHandle,
                FILE_MAP_ALL_ACCESS,
                0, 0,
                0);
            if (g_pQForkControl->inMemoryBuffersControl == NULL) {
                throw std::system_error(
                    GetLastError(),
                    system_category(),
                    "BeginForkOperation: MapViewOfFile failed");
            }
            g_pQForkControl->inMemoryBuffersControl->buffer[0] = (SPBuffer*)(((char*)g_pQForkControl->inMemoryBuffersControl) + sizeof(InMemoryBuffersControl));
            g_pQForkControl->inMemoryBuffersControl->buffer[1] = (SPBuffer*)(((char*)g_pQForkControl->inMemoryBuffersControl) + g_pQForkControl->inMemoryBuffersControlOffset);
            g_pQForkControl->inMemoryBuffersControl->bufferSize = sendBufferSize;

            SetupInMemoryBuffersMasterParent(g_pQForkControl->inMemoryBuffersControl, g_pQForkControl->doSendBuffer, g_pQForkControl->doneSentBuffer);
        } else {
            g_pQForkControl->inMemoryBuffersControlHandle = NULL;
            g_pQForkControl->inMemoryBuffersControl = NULL;
        }

        // protect both the heap and the fork control map from propagating local changes 
        DWORD oldProtect = 0;
        if (VirtualProtect(g_pQForkControl, sizeof(QForkControl), PAGE_WRITECOPY, &oldProtect) == FALSE) {
            throw std::system_error(
                GetLastError(),
                system_category(),
                "BeginForkOperation: VirtualProtect 1 failed");
        }
        redisLog(REDIS_DEBUG, "Protecting heap");
        if (VirtualProtect(
            g_pQForkControl->heapStart,
            g_pQForkControl->availableBlocksInHeap * g_pQForkControl->heapBlockSize,
            PAGE_WRITECOPY,
            &oldProtect) == FALSE) {
            throw std::system_error(
                GetLastError(),
                system_category(),
                "BeginForkOperation: VirtualProtect 2 failed - Most likely your swap file is too small or system has too much memory pressure.");
        }
        redisLog(REDIS_DEBUG, "Protected heap");


        // Launch the "forked" process
        char fileName[MAX_PATH];
        if (0 == GetModuleFileNameA(NULL, fileName, MAX_PATH)) {
            throw system_error(
                GetLastError(),
                system_category(),
                "Failed to get module name.");
        }

        STARTUPINFOA si;
        memset(&si,0, sizeof(STARTUPINFOA));
        si.cb = sizeof(STARTUPINFOA);
        char arguments[_MAX_PATH];
        memset(arguments,0,_MAX_PATH);
        PROCESS_INFORMATION pi;
        si.dwFlags = STARTF_USESTDHANDLES;
        si.hStdOutput = GetStdHandle(STD_OUTPUT_HANDLE);
        si.hStdInput = GetStdHandle(STD_INPUT_HANDLE);
        si.hStdError = GetStdHandle(STD_ERROR_HANDLE);
        sprintf_s(arguments, _MAX_PATH, "%s %ld %ld", qforkFlag, g_hQForkControlFileMap, GetCurrentProcessId());
        redisLog(REDIS_DEBUG, "launching child");
        if (FALSE == CreateProcessA(fileName, arguments, NULL, NULL, TRUE, 0, NULL, NULL, &si, &pi)) {
            throw system_error( 
                GetLastError(),
                system_category(),
                "Problem creating slave process" );
        }
        g_hForkedProcess = pi.hProcess; // must CloseHandle on this
        CloseHandle(pi.hThread);

        redisLog(REDIS_DEBUG, "Waiting on forked process");


        HANDLE handles[2];
        handles[0] = g_pQForkControl->forkedProcessReady;
        handles[1] = g_pQForkControl->operationFailed;

        // wait for "forked" process to map memory
        if (WaitForMultipleObjects(2, handles, FALSE, 1000000) != WAIT_OBJECT_0) {
                throw system_error(
                GetLastError(),
                system_category(),
                "Forked Process did not respond successfully in a timely manner.");
        }

        // signal the 2nd process that we want to do some work
        SetEvent(g_pQForkControl->startOperation);

        (*childPID) = pi.dwProcessId;
        redisLog(REDIS_NOTICE, "Forked successfully");
        return TRUE;
    }
    catch(std::system_error syserr) {
        redisLog(REDIS_WARNING, "BeginForkOperation: system error caught. error code=0x%08x, message=%s", syserr.code().value(), syserr.what());
    }
    catch(std::runtime_error runerr) {
        redisLog(REDIS_WARNING, "BeginForkOperation: runtime error caught. message=%s", runerr.what());
    }
    catch(...) {
        redisLog(REDIS_WARNING, "BeginForkOperation: other exception caught.");
    }
    try {
        ClearInMemoryBuffersMasterParent();
        if (g_hForkedProcess != 0) {
            if (TerminateProcess(g_hForkedProcess, 1) == FALSE) {
                throw std::system_error(
                    GetLastError(),
                    system_category(),
                    "AbortOperation: Killing forked process failed.");
            }
            CloseHandle(g_hForkedProcess);
            g_hForkedProcess = NULL;
        }
        DWORD oldProtect;
        if (VirtualProtect(
            g_pQForkControl->heapStart,
            g_pQForkControl->availableBlocksInHeap * g_pQForkControl->heapBlockSize,
            PAGE_READWRITE,
            &oldProtect) == FALSE) {
            throw std::system_error(
                GetLastError(),
                system_category(),
                "BeginForkOperation: Cannot reset back to read-write");
        }
        if (VirtualProtect(g_pQForkControl, sizeof(QForkControl), PAGE_READWRITE, &oldProtect) == FALSE) {
            throw std::system_error(
                GetLastError(),
                system_category(),
                "BeginForkOperation: Cannot reset control back to read-write");
        }
        if (g_pQForkControl->inMemoryBuffersControl) {
            if (!UnmapViewOfFile(g_pQForkControl->inMemoryBuffersControlHandle)) {
                throw std::system_error(
                    GetLastError(),
                    system_category(),
                    "BeginForkOperation: UnMapViewOfFile failed");
            }
        }
        if (g_pQForkControl->inMemoryBuffersControlHandle)
            CloseHandle(g_pQForkControl->inMemoryBuffersControlHandle);
    }
    catch (std::system_error syserr) {
        redisLog(REDIS_WARNING, "BeginForkOperation Revert: system error caught. error code=0x%08x, message=%s", syserr.code().value(), syserr.what());
        exit(1);
    }
    catch (std::runtime_error runerr) {
        redisLog(REDIS_WARNING, "BeginForkOperation Revert: runtime error caught. message=%s", runerr.what());
        exit(1);
    }
    catch (...) {
        redisLog(REDIS_WARNING, "BeginForkOperation Revert: other exception caught.");
        exit(1);
    }



    return FALSE;
}






DWORD WINAPI EndForkThreadProc(void * param)
{
    try {

        SetEvent(g_pQForkControl->terminateForkedProcess);

        if( g_hForkedProcess != 0 )
        {
            if (WaitForSingleObject(g_hForkedProcess, cDeadForkWait) == WAIT_TIMEOUT) {
                redisLog(REDIS_WARNING, "Force killing child");
                if (TerminateProcess(g_hForkedProcess, 1) == FALSE) {
                    throw std::system_error(
                        GetLastError(),
                        system_category(),
                        "EndForkOperation: Killing forked process failed.");
                }
            }
        }
        redisLog(REDIS_DEBUG, "Child exited");

        if (g_pQForkControl->inMemoryBuffersControl) {
            if (!UnmapViewOfFile(g_pQForkControl->inMemoryBuffersControl)) {
                throw std::system_error(
                    GetLastError(),
                    system_category(),
                    "BeginForkOperation: UnmapViewOfFile failed");
            }
            g_pQForkControl->inMemoryBuffersControl = NULL;
        }

        if (g_pQForkControl->inMemoryBuffersControlHandle) {
            CloseHandle(g_pQForkControl->inMemoryBuffersControlHandle);
            g_pQForkControl->inMemoryBuffersControlHandle = NULL;
        }

        // restore protection constants on shared memory blocks 
        DWORD oldProtect = 0;
        if (VirtualProtect(g_pQForkControl, sizeof(QForkControl), PAGE_READWRITE, &oldProtect) == FALSE) {
            throw std::system_error(
                GetLastError(), 
                system_category(),
                "EndForkOperation: VirtualProtect 3 failed.");
        }


        redisLog(REDIS_NOTICE, "Fork operation async completed");

        return TRUE;
    }
    catch(std::system_error syserr) {
        redisLog(REDIS_WARNING, "0x%08x - %s", syserr.code().value(), syserr.what());

        // If we can not properly restore fork state, then another fork operation is not possible. 
        exit(1);
    }
    catch( ... ) {
        redisLog(REDIS_WARNING, "Some other exception caught in EndForkOperation().");
        exit(1);
    }
    return FALSE;
}



void StartEndForkProcess()
{
    try {
        if (g_hEndForkThread) {
            return;
        }
        redisLog(REDIS_DEBUG, "Starting EndForkThread");
        ClearInMemoryBuffersMasterParent();

        if ((g_hEndForkThread = CreateThread(NULL, 0, EndForkThreadProc, NULL, CREATE_SUSPENDED, NULL)) == NULL) {
            throw std::system_error(
                GetLastError(),
                system_category(),
                "Cannot create end fork thread.");
        }
        if (SetThreadPriority(g_hEndForkThread, THREAD_PRIORITY_IDLE) == FALSE) {
            throw std::system_error(
                GetLastError(),
                system_category(),
                "Cannot set thread priority to idle.");
        }
        if (ResumeThread(g_hEndForkThread) == -1) {
            throw std::system_error(
                GetLastError(),
                system_category(),
                "Cannot resume thread.");
        }
    }
    catch (std::system_error syserr) {
        redisLog(REDIS_WARNING, "0x%08x - %s", syserr.code().value(), syserr.what());

        // If we can not properly restore fork state, then another fork operation is not possible. 
        exit(1);
    }
    catch (...) {
        redisLog(REDIS_WARNING, "Some other exception caught in EndForkOperation().");
        exit(1);
    }

}


OperationStatus GetForkOperationStatus(BOOL cleanupIfDone) {
    OperationStatus failed = (WaitForSingleObject(g_pQForkControl->operationFailed, 0) == WAIT_OBJECT_0) ? OperationStatus::osFAILED : (OperationStatus)0;

    if (cleanupIfDone) {
        StartEndForkProcess();
    }

    if (g_hEndForkThread && WaitForSingleObject(g_hEndForkThread, 0) == WAIT_OBJECT_0) {
        return (OperationStatus) (OperationStatus::osCLEANEDUP | failed);
    }
    
    if (g_hForkedProcess && WaitForSingleObject(g_hForkedProcess, 0) == WAIT_OBJECT_0) {
        return (OperationStatus)(OperationStatus::osEXITED | failed);
    }

    if (WaitForSingleObject(g_pQForkControl->operationComplete, 0) == WAIT_OBJECT_0 || failed) {
        return (OperationStatus)(OperationStatus::osCOMPLETE | failed);
    }

    if (WaitForSingleObject(g_pQForkControl->forkedProcessReady, 0) == WAIT_OBJECT_0) {
        return (OperationStatus)(OperationStatus::osINPROGRESS | failed);
    }

    return OperationStatus::osUNSTARTED;
}


BOOL EndForkOperation(int * pExitCode)
{
    try {
        redisLog(REDIS_NOTICE, "Ending fork operation");
        StartEndForkProcess();
        if (g_hForkedProcess) {
            WaitForSingleObject(g_hForkedProcess, INFINITE);
            if (pExitCode != NULL) {
                GetExitCodeProcess(g_hForkedProcess, (DWORD*)pExitCode);
            }
            CloseHandle(g_hForkedProcess);
            g_hForkedProcess = NULL;
        }
        // ensure events are in the correct state
        ResetEventHandle(g_pQForkControl->operationComplete);
        ResetEventHandle(g_pQForkControl->operationFailed);
        ResetEventHandle(g_pQForkControl->startOperation);
        ResetEventHandle(g_pQForkControl->forkedProcessReady);
        ResetEventHandle(g_pQForkControl->terminateForkedProcess);
        ResetEventHandle(g_pQForkControl->doSendBuffer[0]);
        ResetEventHandle(g_pQForkControl->doSendBuffer[1]);
        ResetEventHandle(g_pQForkControl->doneSentBuffer[0]);
        ResetEventHandle(g_pQForkControl->doneSentBuffer[1]);


        redisLog(REDIS_DEBUG, "Unprotecting heap");

        void * heapAltRegion = MapViewOfFileEx(g_pQForkControl->heapMemoryMap, FILE_MAP_ALL_ACCESS, 0, 0, 0, 0);
        if (heapAltRegion == NULL) {
            throw std::system_error(
                GetLastError(),
                system_category(),
                "MapViewOfFileEx failure");
        }

        HANDLE hProcess = GetCurrentProcess();
        const size_t ONEMB = 1024 * pageSize;
        size_t MBS = g_pQForkControl->availableBlocksInHeap * g_pQForkControl->heapBlockSize / ONEMB;
        int pages = (int)(ONEMB / pageSize);
        int copiedCount = 0;
        PSAPI_WORKING_SET_EX_INFORMATION pwsi[1024]; //= new PSAPI_WORKING_SET_EX_INFORMATION[pages];
        if (pwsi == NULL) {
            throw new system_error(
                GetLastError(),
                system_category(),
                "pwsi == NULL");
        }

        for (int count = 0; count < MBS; count++) {
            DWORD oldProtect;
            if (VirtualProtect(
                (BYTE*)g_pQForkControl->heapStart + ONEMB * count,
                ONEMB,
                PAGE_READWRITE,
                &oldProtect) == FALSE) {
                throw std::system_error(
                    GetLastError(),
                    system_category(),
                    "EndForkOperation: VirtualProtect 4 failed.");
            }

            memset(pwsi, 0, sizeof(PSAPI_WORKING_SET_EX_INFORMATION) * 1024);
            for (int page = 0; page < pages; page++) {
                pwsi[page].VirtualAddress = (BYTE*)g_pQForkControl->heapStart + page * pageSize + ONEMB * count;
            }
            if (QueryWorkingSetEx(
                hProcess,
                pwsi,
                sizeof(PSAPI_WORKING_SET_EX_INFORMATION) * pages) == FALSE) {
                throw system_error(
                    GetLastError(),
                    system_category(),
                    "QueryWorkingSet failure");
            }

            for (int page = 0; page < pages; page++) {
                if (pwsi[page].VirtualAttributes.Valid == 1) {
                    // A 0 share count indicates a COW page
                    if (pwsi[page].VirtualAttributes.ShareCount == 0) {
                        copiedCount++;
                        size_t offset = ONEMB * count + page * pageSize;
                        memcpy(
                            (BYTE*)heapAltRegion + offset,
                            (BYTE*)g_pQForkControl->heapStart + offset,
                            pageSize);
                        DWORD old;
                        if (VirtualProtect((BYTE*)g_pQForkControl->heapStart + offset, pageSize, PAGE_READWRITE | PAGE_REVERT_TO_FILE_MAP, &old) == FALSE) {
                            throw std::system_error(
                                GetLastError(),
                                system_category(),
                                "EndForkOperation: Revert to file map failed.");
                        }
                    }
                }
            }            
        }
        redisLog(REDIS_DEBUG, "Copied changed pages: %d", copiedCount);

        if (UnmapViewOfFile(heapAltRegion) == FALSE) {
            throw std::system_error(
                GetLastError(),
                system_category(),
                "EndForkOperation: UnmapViewOfFile failed.");
        }
        redisLog(REDIS_DEBUG, "Unmapped original heap");
//        delete pwsi;



        LPVOID controlCopy = malloc(sizeof(QForkControl));
        if (controlCopy == NULL) {
            throw std::system_error(
                GetLastError(),
                system_category(),
                "EndForkOperation: allocation failed.");
        }
        memcpy(controlCopy, g_pQForkControl, sizeof(QForkControl));
        if (UnmapViewOfFile(g_pQForkControl) == FALSE) {
            throw std::system_error(
                GetLastError(),
                system_category(),
                "EndForkOperation: UnmapViewOfFile failed.");
        }
        g_pQForkControl = (QForkControl*)
            MapViewOfFileEx(
            g_hQForkControlFileMap,
            FILE_MAP_ALL_ACCESS,
            0, 0,
            0,
            g_pQForkControl);
        if (g_pQForkControl == NULL) {
            throw std::system_error(
                GetLastError(),
                system_category(),
                "EndForkOperation: Remapping ForkControl failed.");
        }
        memcpy(g_pQForkControl, controlCopy, sizeof(QForkControl));
        delete controlCopy;
        controlCopy = NULL;

        redisLog(REDIS_NOTICE, "Ended fork operation");

        return TRUE;
    }
    catch (std::system_error syserr) {
        redisLog(REDIS_WARNING, "0x%08x - %s", syserr.code().value(), syserr.what());

        // If we can not properly restore fork state, then another fork operation is not possible. 
        exit(1);
    }
    catch (...) {
        redisLog(REDIS_WARNING, "Some other exception caught in EndForkOperation().");
        exit(1);
    }
    return FALSE;
}


BOOL AbortForkOperation(BOOL blockUntilCleanedup)
{
    try {
        redisLog(REDIS_NOTICE, "Aborting child process");
        ClearInMemoryBuffersMasterParent();
        if (g_hForkedProcess && !g_hEndForkThread)
        {
            SetEvent(g_pQForkControl->operationFailed);
            if (WaitForSingleObject(g_hForkedProcess, 0) == WAIT_OBJECT_0) {
                // background process still running
                if (TerminateProcess(g_hForkedProcess, 1) == FALSE) {
                    throw std::system_error(
                        GetLastError(),
                        system_category(),
                        "AbortOperation: Killing forked process failed.");
                }
            }
            if (blockUntilCleanedup) {
                EndForkThreadProc(NULL);
                // now do the same with qfork control
                LPVOID controlCopy = malloc(sizeof(QForkControl));
                if (controlCopy == NULL) {
                    throw std::system_error(
                        GetLastError(),
                        system_category(),
                        "EndForkOperation: allocation failed.");
                }
                memcpy(controlCopy, g_pQForkControl, sizeof(QForkControl));
                if (UnmapViewOfFile(g_pQForkControl) == FALSE) {
                    throw std::system_error(
                        GetLastError(),
                        system_category(),
                        "EndForkOperation: UnmapViewOfFile failed.");
                }
                g_pQForkControl = (QForkControl*)
                    MapViewOfFileEx(
                    g_hQForkControlFileMap,
                    FILE_MAP_ALL_ACCESS,
                    0, 0,
                    0,
                    g_pQForkControl);
                if (g_pQForkControl == NULL) {
                    throw std::system_error(
                        GetLastError(),
                        system_category(),
                        "EndForkOperation: Remapping ForkControl failed.");
                }
                memcpy(g_pQForkControl, controlCopy, sizeof(QForkControl));
                delete controlCopy;
                controlCopy = NULL;
                if (g_hForkedProcess) {
                    CloseHandle(g_hForkedProcess);
                    g_hForkedProcess = 0;
                }
            } else {
                StartEndForkProcess();
            }
        } else if (g_hEndForkThread) {
            // background process in cleanup
            if (blockUntilCleanedup) {
                ForceEndForkProcess();
                if (g_hForkedProcess) {
                    CloseHandle(g_hForkedProcess);
                    g_hForkedProcess = 0;
                }
            }
        }

        return TRUE;
    }
    catch (std::system_error syserr) {
        redisLog(REDIS_WARNING, "0x%08x - %s", syserr.code().value(), syserr.what());

        // If we can not properly restore fork state, then another fork operation is not possible. 
        exit(1);
    }
    catch (...) {
        redisLog(REDIS_WARNING, "Some other exception caught in EndForkOperation().");
        exit(1);
    }
    return FALSE;
}




int blocksMapped = 0;
int totalAllocCalls = 0;
int totalFreeCalls = 0;

LPVOID AllocHeapBlock(size_t size, BOOL allocateHigh) {
    if (g_isForkedProcess) {
        LPVOID rv =  VirtualAlloc(0, size, MEM_RESERVE|MEM_COMMIT| (allocateHigh ? MEM_TOP_DOWN: 0), PAGE_READWRITE);
        return rv;
    }
    totalAllocCalls++;
    LPVOID retPtr = (LPVOID)NULL;
    if (size % g_pQForkControl->heapBlockSize != 0 ) {
        errno = EINVAL;
        return retPtr;
    }
    int contiguousBlocksToAllocate = (int)(size / g_pQForkControl->heapBlockSize);

    size_t mapped = 0;
    int startIndex = allocateHigh ? g_pQForkControl->availableBlocksInHeap - 1 : contiguousBlocksToAllocate - 1;
    int endIndex = allocateHigh ? -1 : g_pQForkControl->availableBlocksInHeap - contiguousBlocksToAllocate + 1;
    int direction = allocateHigh ? -1 : 1;
    int blockIndex = 0;
    int contiguousBlocksFound = 0;
    for(blockIndex = startIndex; 
        blockIndex != endIndex; 
        blockIndex += direction) {
        for (int n = 0; n < contiguousBlocksToAllocate; n++) {
            if (g_pQForkControl->heapBlockMap[blockIndex + n * direction] == BlockState::bsUNMAPPED) {
                contiguousBlocksFound++;
            }
            else {
                contiguousBlocksFound = 0;
                break;
            }
        }
        if (contiguousBlocksFound == contiguousBlocksToAllocate) {
            break;
        }
    }

    if (contiguousBlocksFound == contiguousBlocksToAllocate) {
        int allocationStart = blockIndex + (allocateHigh ? 1 - contiguousBlocksToAllocate : 0);
        LPVOID blockStart = 
            reinterpret_cast<byte*>(g_pQForkControl->heapStart) + 
            (g_pQForkControl->heapBlockSize * allocationStart);
        for(int n = 0; n < contiguousBlocksToAllocate; n++ ) {
            g_pQForkControl->heapBlockMap[allocationStart+n] = BlockState::bsMAPPED;
            blocksMapped++;
            mapped += g_pQForkControl->heapBlockSize; 
        }
        retPtr = blockStart;
    }
    else {
        errno = ENOMEM;
    }

    return retPtr;
}

BOOL FreeHeapBlock(LPVOID block, size_t size)
{
    if (g_isForkedProcess) {
        char* cptr = (char*)block;

        MEMORY_BASIC_INFORMATION minfo;
        while (size) {
            if (VirtualQuery(cptr, &minfo, sizeof(minfo)) == 0)
                return -1;
            if (minfo.BaseAddress != cptr || minfo.AllocationBase != cptr ||
                minfo.State != MEM_COMMIT || minfo.RegionSize > size)
                return -1;
    
            if (VirtualFree(cptr, 0, MEM_RELEASE) == 0) {
                return -1;
            }
            cptr += minfo.RegionSize;
            size -= minfo.RegionSize;
        }
        return 0;
    }

    totalFreeCalls++;
    if (size == 0) {
        return FALSE;
    }

    INT_PTR ptrDiff = reinterpret_cast<byte*>(block) - reinterpret_cast<byte*>(g_pQForkControl->heapStart);
    if (ptrDiff < 0 || (ptrDiff % g_pQForkControl->heapBlockSize) != 0) {
        return FALSE;
    }

    int blockIndex = (int)(ptrDiff / g_pQForkControl->heapBlockSize);
    if (blockIndex >= g_pQForkControl->availableBlocksInHeap) {
        return FALSE;
    }

    int contiguousBlocksToFree = (int)(size / g_pQForkControl->heapBlockSize);

    if (VirtualUnlock(block, size) == FALSE) {
        DWORD err = GetLastError();
        if (err != ERROR_NOT_LOCKED) {
            return FALSE;
        }
    };
    for (int n = 0; n < contiguousBlocksToFree; n++ ) {
        blocksMapped--;
        g_pQForkControl->heapBlockMap[blockIndex + n] = BlockState::bsUNMAPPED;
    }
    return TRUE;
}


extern "C"
{
    // The external main() is redefined as redis_main() by Win32_QFork.h.
    // The CRT will call this replacement main() before the previous main()
    // is invoked so that the QFork allocator can be setup prior to anything 
    // Redis will allocate.
    int main(int argc, char* argv[]) {
#ifdef DEBUG_WITH_PROCMON
        hProcMonDevice = 
            CreateFile( 
                L"\\\\.\\Global\\ProcmonDebugLogger", 
                GENERIC_READ|GENERIC_WRITE, 
                FILE_SHARE_READ|FILE_SHARE_WRITE|FILE_SHARE_DELETE, 
                NULL, 
                OPEN_EXISTING, 
                FILE_ATTRIBUTE_NORMAL, 
                NULL );
#endif

        StartupStatus status = QForkStartup(argc, argv);
        if (status == ssCONTINUE_AS_MASTER) {
            int retval = redis_main(argc, argv);
            QForkShutdown();
            return retval;
        } else if (status == ssSLAVE_EXIT) {
            // slave is done - clean up and exit
            QForkShutdown();
            return g_SlaveExitCode;
        } else if (status == ssFAILED) {
            // master or slave failed initialization
            return 1;
        } else {
            // unexpected status return
            return 2;
        }
    }
}
