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
#include "Win32_Service.h"
#include "Win32_CommandLine.h"
#include "..\redisLog.h"
#include <time.h>
#include <vector>
#include <iostream>
#include <sstream>
#include <stdint.h>
#include <exception>
#include <DbgHelp.h>
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

#define IFFAILTHROW(a,m) if(!(a)) { throw std::system_error(GetLastError(), system_category(), m); }




#ifndef LODWORD
    #define LODWORD(_qw)    ((DWORD)(_qw))
#endif
#ifndef HIDWORD
    #define HIDWORD(_qw)    ((DWORD)(((_qw) >> (sizeof(DWORD)*8)) & DWORD(~0)))
#endif

const SIZE_T cAllocationGranularity = 1 << 26;                   // 64MB per dlmalloc heap block 
const int cMaxBlocks = (1 << 16)/4;                                  // 64MB*16K sections = 1TB.
const char* cMapFileBaseName = "RedisQFork";
const int cDeadForkWait = 30000;
const size_t pageSize = 4096;

typedef enum BlockState {
    bsINVALID = 0,
    bsUNMAPPED = 1,
    bsMAPPED = 2
}BlockState;

struct BlockData {
    BlockState state;
    HANDLE heapMemoryMapFile;
    HANDLE heapMemoryMap;
};

struct QForkControl {
    int availableBlocksInHeap;                 // number of blocks in blockMap (dynamically determined at run time)
    int maxAvailableBlockInHeap;
    SIZE_T heapBlockSize;           
    BlockData heapBlockMap[cMaxBlocks];
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
    HANDLE doSendBuffer[MAXSENDBUFFER];
    HANDLE doneSentBuffer[MAXSENDBUFFER];

    // global data pointers to be passed to the forked process
    QForkBeginInfo globalData;
};


struct CleanupState {
    operationStatus currentState;
    bool failed;
    bool inMemory;
    size_t offsetCopied;
    size_t copyBatchSize;
    int copiedPages;
    int cowPages;
    int scannedPages;
    int exitCode;
    int heapBlocksToCleanup;
    time_t forkExitTimeout;
    LPVOID heapEnd;
    uint64_t * pageBitMap;
};

struct DatFilePaths {
    char PrimaryPath[MAX_PATH];
    unsigned int countInPrimaryPath;
};

QForkControl* g_pQForkControl;
HANDLE g_hQForkControlFileMap;

HANDLE g_hForkedProcess;
HANDLE g_hEndForkThread;
BOOL g_isForkedProcess;
CleanupState g_CleanupState;
DatFilePaths g_DataFilePaths;
int g_SlaveExitCode; // For slave process



const long long cSentinelHeapSize = 30 * 1024 * 1024;
extern "C" int checkForSentinelMode(int argc, char **argv);

typedef void(*ForceCOWBUfferProto)(LPVOID buf, size_t size);
extern "C" void FDAPI_SetForceCOWBuffer(ForceCOWBUfferProto proto);

extern "C"
{
    // forward def from util.h. 
    long long memtoll(const char *p, int *err);
    void TransitionToFreeWindow(int final);
    int dbCheck();
}



__forceinline int64_t AddrToBitSlot(LPVOID addr, int * bit) {
    uint64_t c = (uint64_t) addr;
    c -= (uint64_t) g_pQForkControl->heapStart;
    c = c >> 12;
    *bit = c & ((1ULL << 6) - 1);
    return c >> 6;
}

__forceinline bool IsAddrInHeap(LPVOID addr) {
    return (g_CleanupState.currentState != osUNSTARTED && addr >= g_pQForkControl->heapStart && addr < g_CleanupState.heapEnd);
}

__forceinline bool IsAddrCOW(LPVOID addr) {
    int bit;
    uint64_t slot = AddrToBitSlot(addr, &bit);
    return (g_CleanupState.pageBitMap[slot] & (1ULL << bit)) != 0;
}

void ForceCOWBuffer(LPVOID addr, size_t size) {
    if (!IsAddrInHeap(addr) || size == 0) return;
    int bitstart, bitstop;
    uint64_t slotstart = AddrToBitSlot(addr, &bitstart);
    uint64_t slotend = AddrToBitSlot((BYTE*)addr + size - 1, &bitstop);
    for (uint64_t idx = slotstart; idx <= slotend; idx++) {
        if (g_CleanupState.pageBitMap[idx] == ~0ULL) continue;
        for (int bitidx = ((idx == slotstart) ? bitstart : 0); bitidx <= ((idx == slotend) ? bitstop : 63); bitidx++) {
            if ((g_CleanupState.pageBitMap[idx] & (1ULL << bitidx)) == 0) {
                BYTE * baddr = (BYTE*) g_pQForkControl->heapStart + (((idx << 6) + bitidx) << 12);
                DWORD oldProtect;
                if (g_CleanupState.currentState >= osEXITED) {
                    IFFAILTHROW(VirtualProtect(baddr, 1, PAGE_READWRITE, &oldProtect), "Unable to mark as ReadWrite in ForceCOW.");
                } else {
                    IFFAILTHROW(VirtualProtect(baddr, 1, PAGE_WRITECOPY, &oldProtect), "Unable to mark as WriteCopy in ForceCOW.");
                    g_CleanupState.cowPages++;
                    g_CleanupState.pageBitMap[idx] |= (1ULL << bitidx);
                }
            }
        }
    }
}


typedef BOOL(_stdcall* MiniDumpWriteDumpFunc)(HANDLE hProcess,
    DWORD ProcessId,
    HANDLE hFile,
    MINIDUMP_TYPE DumpType,
    PMINIDUMP_EXCEPTION_INFORMATION ExceptionParam,
    PMINIDUMP_USER_STREAM_INFORMATION UserStreamParam,
    PMINIDUMP_CALLBACK_INFORMATION CallbackParam);


DWORD WINAPI DumpThreadProc(void* param) {
    redisLog(REDIS_WARNING, "Crashed.  Attempting to generate dump");
    MINIDUMP_EXCEPTION_INFORMATION *exinfo = (MINIDUMP_EXCEPTION_INFORMATION*)param;
    HMODULE lib = LoadLibraryA("dbghelp.dll");
    if (lib) {
        MiniDumpWriteDumpFunc func = (MiniDumpWriteDumpFunc)GetProcAddress(lib, "MiniDumpWriteDump");
        if (func) {

            HANDLE file = CreateFileA("dumpfile.dmp", GENERIC_READ | GENERIC_WRITE, 0, NULL, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);

            func(GetCurrentProcess(), GetCurrentProcessId(), file, (MINIDUMP_TYPE)(MiniDumpWithIndirectlyReferencedMemory | MiniDumpWithDataSegs), exinfo, NULL, NULL);
        }
    }
    return 0;
}

void CreateMiniDump(EXCEPTION_POINTERS * excinfo)
{
    MINIDUMP_EXCEPTION_INFORMATION exinfo;
    exinfo.ExceptionPointers = excinfo;
    exinfo.ClientPointers = FALSE;
    exinfo.ThreadId = GetCurrentThreadId();

    HANDLE h = CreateThread(NULL, 0, DumpThreadProc, excinfo ? &exinfo : NULL, 0, NULL);

    WaitForSingleObject(h, INFINITE);
}

LONG CALLBACK VectoredHandler(PEXCEPTION_POINTERS exinfo) {
    
    if (exinfo->ExceptionRecord->ExceptionCode == EXCEPTION_ACCESS_VIOLATION) {
        if (exinfo->ExceptionRecord->ExceptionInformation[0] == 1) {
            LPVOID addr = (LPVOID) exinfo->ExceptionRecord->ExceptionInformation[1];
            if (IsAddrInHeap(addr) && !IsAddrCOW(addr)) {
                DWORD oldProtect;
                if (g_CleanupState.currentState >= osEXITED) {
                    if (VirtualProtect(addr, 1, PAGE_READWRITE, &oldProtect))
                        return EXCEPTION_CONTINUE_EXECUTION;
                } else {
                    if (VirtualProtect(addr, 1, PAGE_WRITECOPY, &oldProtect)) {
                        int bit;
                        uint64_t slot = AddrToBitSlot(addr, &bit);
                        g_CleanupState.pageBitMap[slot] |= (1ULL << bit);
                        g_CleanupState.cowPages++;
                        return EXCEPTION_CONTINUE_EXECUTION;
                    }
                }
            }
        }
    }
    if (exinfo->ExceptionRecord->ExceptionCode == EXCEPTION_BREAKPOINT) return EXCEPTION_CONTINUE_SEARCH;
    if (exinfo->ExceptionRecord->ExceptionCode == DBG_CONTROL_C) return EXCEPTION_CONTINUE_SEARCH;
    CreateMiniDump(exinfo);
    return EXCEPTION_CONTINUE_SEARCH;
}

HANDLE RegisterMiniDumpHandler() {
    HANDLE h = AddVectoredExceptionHandler(0, VectoredHandler);
    return h;
}


BOOL QForkSlaveInit(HANDLE QForkConrolMemoryMapHandle, DWORD ParentProcessID) {
    SmartHandle shParent;
    SmartHandle shMMFile;
    SmartFileView<QForkControl> sfvMasterQForkControl;
    SmartHandle dupForkedProcessReady; 
    SmartHandle dupStartOperation;
    SmartHandle dupOperationComplete;
    SmartHandle dupOperationFailed;
    SmartHandle dupTerminateProcess;
    SmartHandle sfMMFileInMemoryControlHandle;
    SmartFileView<InMemoryBuffersControl> sfvInMemory;
    SmartHandle dupSendBuffer[MAXSENDBUFFER];
    SmartHandle dupSentBuffer[MAXSENDBUFFER];

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
        SIZE_T mmSize = cAllocationGranularity;

        vector<SmartHandle> dupHeapFileHandle(g_pQForkControl->availableBlocksInHeap);

        vector<SmartFileMapHandle>  sfmhMapFile(g_pQForkControl->availableBlocksInHeap);

        vector<SmartFileView<byte>> sfvHeap(g_pQForkControl->availableBlocksInHeap);


        // duplicate handles and stuff into control structure (master protected by PAGE_WRITECOPY)
        for (int x = 0; x < g_pQForkControl->availableBlocksInHeap; x++) {
            dupHeapFileHandle[x].Assign(shParent, sfvMasterQForkControl->heapBlockMap[x].heapMemoryMapFile);
            g_pQForkControl->heapBlockMap[x].heapMemoryMapFile = dupHeapFileHandle[x];

            sfmhMapFile[x].Assign(
                g_pQForkControl->heapBlockMap[x].heapMemoryMapFile,
                PAGE_READONLY,
                HIDWORD(mmSize),
                LODWORD(mmSize),
                string("QForkSlaveInit: Could not open file mapping object in slave"));
            g_pQForkControl->heapBlockMap[x].heapMemoryMap = sfmhMapFile[x];

            sfvHeap[x].Assign(
                g_pQForkControl->heapBlockMap[x].heapMemoryMap,
                FILE_MAP_READ,
                0, 0, 0,
                x * g_pQForkControl->heapBlockSize + (byte*)g_pQForkControl->heapStart,
                string("QForkSlaveInit: Could not map heap in forked process. Is system swap file large enough?"));
        }

        // copy redis globals into fork process
        SetupGlobals(g_pQForkControl->globalData.globalData, g_pQForkControl->globalData.globalDataSize, g_pQForkControl->globalData.dictHashSeed);

        if (g_pQForkControl->inMemoryBuffersControlHandle) {
            sfMMFileInMemoryControlHandle.Assign(shParent, g_pQForkControl->inMemoryBuffersControlHandle);
            g_pQForkControl->inMemoryBuffersControlHandle = sfMMFileInMemoryControlHandle;

            sfvInMemory.Assign(g_pQForkControl->inMemoryBuffersControlHandle, FILE_MAP_ALL_ACCESS, 0, 0, 0, string("QForkSlaveInit: Could not map inmemory buffers in forked process. Is system swap file large enough?"));
            g_pQForkControl->inMemoryBuffersControl = sfvInMemory;

            for (int x = 0; x < MAXSENDBUFFER; x++) {
                g_pQForkControl->inMemoryBuffersControl->buffer[x] = (SPBuffer*)(((char*)g_pQForkControl->inMemoryBuffersControl) + sizeof(InMemoryBuffersControl) + g_pQForkControl->inMemoryBuffersControlOffset * x);
            }
        }
        for (int x = 0; x < MAXSENDBUFFER; x++) {
            dupSendBuffer[x].Assign(shParent, sfvMasterQForkControl->doSendBuffer[x]);
            g_pQForkControl->doSendBuffer[x] = dupSendBuffer[x];
            dupSentBuffer[x].Assign(shParent, sfvMasterQForkControl->doneSentBuffer[x]);
            g_pQForkControl->doneSentBuffer[x] = dupSentBuffer[x];
        }

        IFFAILTHROW(dlmallopt(M_MMAP_THRESHOLD, cAllocationGranularity), "QForkMasterInit: DLMalloc failed initializing direct memory map threshold.");
        IFFAILTHROW(dlmallopt(M_GRANULARITY, cAllocationGranularity), "QForkmasterinit: DLMalloc failed initializing allocation granularity.");
        
        // wait for parent to signal operation start
        WaitForSingleObject(g_pQForkControl->startOperation, INFINITE);
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

        // let parent know we are done
        SetEvent(exitCode == 0 ? g_pQForkControl->operationComplete : g_pQForkControl->operationFailed);

        // parent will notify us when to quit
        WaitForSingleObject(g_pQForkControl->terminateForkedProcess, INFINITE);

        redisLog(REDIS_NOTICE, "Successfully completed background operation.  Exiting child.");

        g_pQForkControl = NULL;
        return TRUE;
    }
    catch (std::system_error syserr) {
        redisLog(REDIS_WARNING, "QForkSlaveInit: system error caught. error code=0x%08x, message=%s", syserr.code().value(), syserr.what());
        if (g_pQForkControl != NULL) {
            if(g_pQForkControl->operationFailed != NULL) {
                SetEvent(g_pQForkControl->operationFailed);
            }
            g_pQForkControl = NULL;
        }
        return FALSE;
    }
    catch(std::runtime_error runerr) {
        redisLog(REDIS_WARNING, "QForkSlaveInit: runtime error caught. message=%s", runerr.what());
        if (g_pQForkControl != NULL) {
            if (g_pQForkControl->operationFailed != NULL) {
                SetEvent(g_pQForkControl->operationFailed);
            }
            g_pQForkControl = NULL;
        }
        return FALSE;
    }
    catch (...) {
        redisLog(REDIS_WARNING, "QForkSlaveInit: other exception caught.");
        if (g_pQForkControl != NULL) {
            if (g_pQForkControl->operationFailed != NULL) {
                SetEvent(g_pQForkControl->operationFailed);
            }
            g_pQForkControl = NULL;
        }
        return FALSE;
    }
    return FALSE;
}


void CreateEventHandle(HANDLE * out) {
    HANDLE h = CreateEvent(NULL, TRUE, FALSE, NULL);
    IFFAILTHROW(h, "QForkMasterInit: CreateEvent failed.");
    
    *out = h;
}

void DeleteExistingDatFiles(char * path) {

    // FILE_FLAG_DELETE_ON_CLOSE will not clean up files in the case of a BSOD or power failure.
    // Clean up anything we can to prevent excessive disk usage.
    char heapMemoryMapWildCard[MAX_PATH];
    WIN32_FIND_DATAA fd;
    sprintf_s(
        heapMemoryMapWildCard,
        MAX_PATH,
        "%s\\%s_*.dat",
        path,
        cMapFileBaseName);
    HANDLE hFind = FindFirstFileA(heapMemoryMapWildCard, &fd);
    while (hFind != INVALID_HANDLE_VALUE) {
        // Failure likely means the file is in use by another redis instance.
        DeleteFileA(fd.cFileName);

        if (FALSE == FindNextFileA(hFind, &fd)) {
            FindClose(hFind);
            hFind = INVALID_HANDLE_VALUE;
        }
    }

}


BOOL QForkMasterInit() {

    // This will be reset to the correct value when config is processed
    setLogVerbosityLevel(REDIS_WARNING);

    FDAPI_SetForceCOWBuffer(ForceCOWBuffer);

    try {

        if (g_argMap.find(cDatFiles) != g_argMap.end()) {
            char* endPtr;
            g_DataFilePaths.countInPrimaryPath = strtoul(g_argMap[cDatFiles].at(0).at(0).c_str(), &endPtr, 10);
            char* end = NULL;
            strcpy_s(g_DataFilePaths.PrimaryPath, g_argMap[cDatFiles].at(0).at(1).c_str());
        }


        // allocate file map for qfork control so it can be passed to the forked process
        g_hQForkControlFileMap = CreateFileMappingW(
            INVALID_HANDLE_VALUE,
            NULL,
            PAGE_READWRITE,
            0, sizeof(QForkControl),
            NULL);
        IFFAILTHROW(g_hQForkControlFileMap, "QForkMasterInit: CreateFileMapping failed");
        

        g_pQForkControl = (QForkControl*)MapViewOfFile(
            g_hQForkControlFileMap, 
            FILE_MAP_ALL_ACCESS,
            0, 0,
            0);
        IFFAILTHROW(g_pQForkControl, "QForkMasterInit: MapViewOfFile failed");
        

        g_pQForkControl->inMemoryBuffersControl = NULL;
        g_pQForkControl->inMemoryBuffersControlHandle = NULL;


        // This must be called only once per process! Calling it more times than that will not recreate existing 
        // section, and dlmalloc will ultimately fail with an access violation. Once is good.
        IFFAILTHROW(dlmallopt(M_MMAP_THRESHOLD, cAllocationGranularity), "QForkMasterInit: DLMalloc failed initializing direct memory map threshold.");
        IFFAILTHROW(dlmallopt(M_GRANULARITY, cAllocationGranularity), "QForkMasterInit: DLMalloc failed initializing allocation granularity.");
        
        g_pQForkControl->heapBlockSize = cAllocationGranularity;

        {
            MEMORYSTATUSEX memstatus;
            memstatus.dwLength = sizeof(MEMORYSTATUSEX);

            IFFAILTHROW(GlobalMemoryStatusEx(&memstatus), "QForkMasterInit: Cannot get global memory status.");
            DWORDLONG cMaxMemory = memstatus.ullTotalPhys * 10;
            if (cMaxMemory > cMaxBlocks * cAllocationGranularity) {
                cMaxMemory = cMaxBlocks * cAllocationGranularity;
            }
            g_pQForkControl->maxAvailableBlockInHeap = (int)(cMaxMemory / cAllocationGranularity);
        }

        g_pQForkControl->availableBlocksInHeap = 0;


        DeleteExistingDatFiles(".");
        if (g_DataFilePaths.countInPrimaryPath) {
            DeleteExistingDatFiles(g_DataFilePaths.PrimaryPath);
        }

        SIZE_T mmSize = g_pQForkControl->maxAvailableBlockInHeap * cAllocationGranularity;
            
        // Find a place in the virtual memory space where we can reserve space for our allocations that is likely
        // to be available in the forked process.  (If this ever fails in the forked process, we will have to launch
        // the forked process and negotiate for a shared memory address here.)
        LPVOID pHigh = VirtualAllocEx( 
            GetCurrentProcess(),
            NULL,
            mmSize,
            MEM_RESERVE | MEM_TOP_DOWN, 
            PAGE_READWRITE);
        IFFAILTHROW(pHigh, "QForkMasterInit: VirtualAllocEx failed.");

        IFFAILTHROW(VirtualFree(pHigh, 0, MEM_RELEASE), "PhysicalMapMemory: VirtualFree failed.");

        g_pQForkControl->heapStart = pHigh;        

        for (int n = 0; n < g_pQForkControl->maxAvailableBlockInHeap; n++) {
            LPVOID reserved = VirtualAlloc(
                (byte*)pHigh + n * g_pQForkControl->heapBlockSize,
                g_pQForkControl->heapBlockSize,
                MEM_RESERVE,
                PAGE_READWRITE);
            IFFAILTHROW(reserved, "QForkMasterInit: VirtualAllocEx of reserve segment failed.");

            g_pQForkControl->heapBlockMap[n].state = 
                ((n < g_pQForkControl->availableBlocksInHeap) ?
                BlockState::bsUNMAPPED : BlockState::bsINVALID);
        }

        g_pQForkControl->typeOfOperation = OperationType::otINVALID;

        CreateEventHandle(&g_pQForkControl->forkedProcessReady);
        CreateEventHandle(&g_pQForkControl->startOperation);
        CreateEventHandle(&g_pQForkControl->operationComplete);
        CreateEventHandle(&g_pQForkControl->operationFailed);
        CreateEventHandle(&g_pQForkControl->terminateForkedProcess);
        for (int x = 0; x < MAXSENDBUFFER; x++) {
            CreateEventHandle(&g_pQForkControl->doSendBuffer[x]);
            CreateEventHandle(&g_pQForkControl->doneSentBuffer[x]);
        }
        
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
    int memtollerr = 0;

    if (g_argMap.find(cQFork) != g_argMap.end()) {
        // slave command line looks like: --QFork [QForkConrolMemoryMap handle] [parent process id]
        foundSlaveFlag = true;
        char* endPtr;
        QForkConrolMemoryMapHandle = (HANDLE)strtoul(g_argMap[cQFork].at(0).at(0).c_str(),&endPtr,10);
        char* end = NULL;
        PPID = strtoul(g_argMap[cQFork].at(0).at(1).c_str(), &end, 10);
    }

    if (foundSlaveFlag) {
        return QForkSlaveInit( QForkConrolMemoryMapHandle, PPID ) ? StartupStatus::ssSLAVE_EXIT : StartupStatus::ssFAILED;
    } else {
        return QForkMasterInit() ? StartupStatus::ssCONTINUE_AS_MASTER : StartupStatus::ssFAILED;
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
        for (int x = 0; x < MAXSENDBUFFER; x++) {
            CloseEventHandle(&g_pQForkControl->doneSentBuffer[x]);
            CloseEventHandle(&g_pQForkControl->doSendBuffer[x]);
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


void ForceCleanupOfPreviousFork()
{
    if (g_CleanupState.currentState != osUNSTARTED) {
        redisLog(REDIS_VERBOSE, "Cleaning up old fork.");
        GetForkOperationStatus(TRUE); // this will kill the child
        AdvanceCleanupForkOperation(TRUE, NULL);
        EndForkOperation(NULL);
        redisLog(REDIS_VERBOSE, "Cleaned up old fork.");
    }
}


BOOL BeginForkOperation(OperationType type, char* fileName, int sendBufferSize, LPVOID globalData, int sizeOfGlobalData, DWORD* childPID, uint32_t dictHashSeed) {
    try {
        ForceCleanupOfPreviousFork();
        redisLog(REDIS_NOTICE, "Starting to fork parent process.");
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
        for (int x = 0; x < MAXSENDBUFFER; x++) {
            ResetEventHandle(g_pQForkControl->doSendBuffer[x]);
            ResetEventHandle(g_pQForkControl->doneSentBuffer[x]);
        }

        if (type == otRDBINMEMORY) {

            size_t size = sizeof(InMemoryBuffersControl);
            if (sendBufferSize < 1024) sendBufferSize = 1024;
            size_t SPBufferSize = offsetof(SPBuffer, b) + sendBufferSize;
            SPBufferSize = (SPBufferSize + 0x0f) & ~0x0f;
            size += SPBufferSize * MAXSENDBUFFER;
            g_pQForkControl->inMemoryBuffersControlOffset = (int)SPBufferSize;

            if (g_pQForkControl->inMemoryBuffersControl) {
                IFFAILTHROW(UnmapViewOfFile(g_pQForkControl->inMemoryBuffersControl), "BeginForkOperation: UnmapViewOfFile failed");
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
            IFFAILTHROW(g_pQForkControl->inMemoryBuffersControlHandle, "BeginForkOperation: CreateFileMapping failed");
            
            g_pQForkControl->inMemoryBuffersControl = (InMemoryBuffersControl*)MapViewOfFile(
                g_pQForkControl->inMemoryBuffersControlHandle,
                FILE_MAP_ALL_ACCESS,
                0, 0,
                0);
            IFFAILTHROW(g_pQForkControl->inMemoryBuffersControl, "BeginForkOperation: MapViewOfFile failed");
            
            for (int x = 0; x < MAXSENDBUFFER; x++) {
                g_pQForkControl->inMemoryBuffersControl->buffer[x] = (SPBuffer*)(((char*)g_pQForkControl->inMemoryBuffersControl) + sizeof(InMemoryBuffersControl) + SPBufferSize * x);
            }
            g_pQForkControl->inMemoryBuffersControl->bufferSize = sendBufferSize;

            SetupInMemoryBuffersMasterParent(g_pQForkControl->inMemoryBuffersControl,g_pQForkControl->doSendBuffer, g_pQForkControl->doneSentBuffer);
        } else {
            g_pQForkControl->inMemoryBuffersControlHandle = NULL;
            g_pQForkControl->inMemoryBuffersControl = NULL;
        }

        // protect both the heap and the fork control map from propagating local changes 
        DWORD oldProtect = 0;
        IFFAILTHROW(VirtualProtect(g_pQForkControl, sizeof(QForkControl), PAGE_WRITECOPY, &oldProtect), "BeginForkOperation: VirtualProtect 1 failed");
        
        redisLog(REDIS_VERBOSE, "Protecting heap");
        for (int x = 0; x < g_pQForkControl->availableBlocksInHeap; x++) {
            IFFAILTHROW(VirtualProtect(
                (byte*)g_pQForkControl->heapStart + x * g_pQForkControl->heapBlockSize,
                g_pQForkControl->heapBlockSize,
                PAGE_READONLY,
                &oldProtect),
                "BeginForkOperation: VirtualProtect 2 failed - Most likely your swap file is too small or system has too much memory pressure.");
        }
        redisLog(REDIS_VERBOSE, "Protected heap");

        // Launch the "forked" process
        char fileName[MAX_PATH];
        IFFAILTHROW(GetModuleFileNameA(NULL, fileName, MAX_PATH), "Failed to get module name.");
        
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
        sprintf_s(arguments, _MAX_PATH, "%s --%s %llu %lu", fileName, cQFork.c_str(), (uint64_t) g_hQForkControlFileMap, GetCurrentProcessId());
        redisLog(REDIS_VERBOSE, "Launching child");
        IFFAILTHROW(CreateProcessA(fileName, arguments, NULL, NULL, TRUE, 0, NULL, NULL, &si, &pi), "Problem creating slave process" );
        
        g_hForkedProcess = pi.hProcess; // must CloseHandle on this
        CloseHandle(pi.hThread);

        redisLog(REDIS_VERBOSE, "Waiting on forked process");

        HANDLE handles[3];
        handles[0] = g_pQForkControl->forkedProcessReady;
        handles[1] = g_pQForkControl->operationFailed;
        handles[2] = g_hForkedProcess;

        // wait for "forked" process to map memory
        IFFAILTHROW(WaitForMultipleObjects(3, handles, FALSE, 100000) == WAIT_OBJECT_0, "Forked Process did not respond successfully in a timely manner.");

        // signal the 2nd process that we want to do some work
        SetEvent(g_pQForkControl->startOperation);

        EndForkOperation(NULL);
        g_CleanupState.currentState = osINPROGRESS;
        g_CleanupState.inMemory = (type == otRDBINMEMORY);
        g_CleanupState.heapBlocksToCleanup = g_pQForkControl->availableBlocksInHeap;
        g_CleanupState.heapEnd = (char*)g_pQForkControl->heapStart + g_pQForkControl->heapBlockSize * g_pQForkControl->availableBlocksInHeap;
        g_CleanupState.pageBitMap = (uint64_t*)calloc(g_pQForkControl->heapBlockSize * g_pQForkControl->availableBlocksInHeap / pageSize / (8 * 8), sizeof(uint64_t));

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
            TerminateProcess(g_hForkedProcess, 1);
            CloseHandle(g_hForkedProcess);
            g_hForkedProcess = NULL;
        }
        DWORD oldProtect;
        for (int x = 0; x < g_pQForkControl->availableBlocksInHeap; x++) {
            IFFAILTHROW(VirtualProtect(
                (byte*)g_pQForkControl->heapStart + x * g_pQForkControl->heapBlockSize,
                g_pQForkControl->heapBlockSize,
                PAGE_READWRITE,
                &oldProtect),
                "BeginForkOperation: Cannot reset back to read-write");
        }
        IFFAILTHROW(VirtualProtect(g_pQForkControl, sizeof(QForkControl), PAGE_READWRITE, &oldProtect), "BeginForkOperation: Cannot reset control back to read-write");
        
        if (g_pQForkControl->inMemoryBuffersControl) {
            IFFAILTHROW(UnmapViewOfFile(g_pQForkControl->inMemoryBuffersControl), "BeginForkOperation: UnMapViewOfFile failed");            
            g_pQForkControl->inMemoryBuffersControl = NULL;
        }
        if (g_pQForkControl->inMemoryBuffersControlHandle) {
            CloseHandle(g_pQForkControl->inMemoryBuffersControlHandle);
            g_pQForkControl->inMemoryBuffersControlHandle = NULL;
        }
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


OperationStatus GetForkOperationStatus(BOOL forceEnd) {
    try {
     
        if (g_CleanupState.currentState == osINPROGRESS) {
            g_CleanupState.failed = (WaitForSingleObject(g_pQForkControl->operationFailed, 0) == WAIT_OBJECT_0);
            if (g_CleanupState.failed || WaitForSingleObject(g_pQForkControl->operationComplete, 0) == WAIT_OBJECT_0) {
                g_CleanupState.currentState = osCOMPLETE;
            } else if (g_hForkedProcess && (WaitForSingleObject(g_hForkedProcess, 0) == WAIT_OBJECT_0)) {
                g_CleanupState.failed = TRUE;
                g_CleanupState.currentState = osEXITED;
            }
            if (!forceEnd) 
                return (OperationStatus)(g_CleanupState.currentState | (g_CleanupState.failed ? osFAILED : 0) | (g_CleanupState.inMemory ? osINMEMORY : 0));
        }
        if (g_CleanupState.currentState == osCOMPLETE) {
            time_t now;
            time(&now);
            g_CleanupState.forkExitTimeout = now + cDeadForkWait;
            SetEvent(g_pQForkControl->terminateForkedProcess);
            g_CleanupState.currentState = osWAITINGFOREXIT;
            if (!forceEnd)
                return (OperationStatus)(g_CleanupState.currentState | (g_CleanupState.failed ? osFAILED : 0) | (g_CleanupState.inMemory ? osINMEMORY : 0));
        }
        if (g_CleanupState.currentState == osWAITINGFOREXIT) {
            time_t now;
            time(&now);
            DWORD rval = 0;
            if (!g_hForkedProcess || (rval = WaitForSingleObject(g_hForkedProcess, 0)) == WAIT_OBJECT_0 || now > g_CleanupState.forkExitTimeout || forceEnd) {
                if (g_hForkedProcess) {
                    if (rval != WAIT_OBJECT_0 && (now > g_CleanupState.forkExitTimeout || forceEnd)) {
                        redisLog(REDIS_WARNING, "Force killing child");
                        TerminateProcess(g_hForkedProcess, 1);
                    } // otherwise we know it exited
                    GetExitCodeProcess(g_hForkedProcess, (DWORD*)&g_CleanupState.exitCode);
                    CloseHandle(g_hForkedProcess);
                    g_hForkedProcess = NULL;
                    redisLog(REDIS_VERBOSE, "Child exited");
                }
                g_CleanupState.currentState = osEXITED;
            }
        }

        return (OperationStatus)(g_CleanupState.currentState | (g_CleanupState.failed ? osFAILED : 0) | (g_CleanupState.inMemory ? osINMEMORY : 0));

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

void GetCOWStats(int * cowPages, int * copiedPages, int * scannedPages, int * totalPages)
{
    *cowPages = g_CleanupState.cowPages;
    *copiedPages = g_CleanupState.copiedPages;
    *scannedPages = g_CleanupState.scannedPages;
    *totalPages = g_CleanupState.heapBlocksToCleanup * (int)(g_pQForkControl->heapBlockSize / pageSize);
}


void EndForkOperation(int * pExitCode)
{
    if (pExitCode != NULL) {
        *pExitCode = g_CleanupState.exitCode;
    }
    _ASSERT(g_CleanupState.currentState == osCLEANEDUP || g_CleanupState.currentState == osUNSTARTED);
    if (g_CleanupState.pageBitMap) free(g_CleanupState.pageBitMap);
    memset(&g_CleanupState, 0, sizeof(g_CleanupState));
}

#ifndef PAGE_REVERT_TO_FILE_MAP
#define PAGE_REVERT_TO_FILE_MAP     0x80000000     
#endif

void AdvanceCleanupForkOperation(BOOL forceEnd, int *exitCode) {
    try {
        if (exitCode != NULL) {
            *exitCode = g_CleanupState.exitCode;
        }

        if (g_CleanupState.currentState == osEXITED) {

            redisLog(REDIS_VERBOSE, "Reseting Global state.");

            // ensure events are in the correct state
            ResetEventHandle(g_pQForkControl->operationComplete);
            ResetEventHandle(g_pQForkControl->operationFailed);
            ResetEventHandle(g_pQForkControl->startOperation);
            ResetEventHandle(g_pQForkControl->forkedProcessReady);
            ResetEventHandle(g_pQForkControl->terminateForkedProcess);
            for (int x = 0; x < MAXSENDBUFFER; x++) {
                ResetEventHandle(g_pQForkControl->doSendBuffer[x]);
                ResetEventHandle(g_pQForkControl->doneSentBuffer[x]);
            }

            if (g_pQForkControl->inMemoryBuffersControl) {
                if (!UnmapViewOfFile(g_pQForkControl->inMemoryBuffersControl)) {
                    throw std::system_error(
                        GetLastError(),
                        system_category(),
                        "AdvanceForkCleanup: UnmapViewOfFile failed");
                }
                g_pQForkControl->inMemoryBuffersControl = NULL;
            }

            if (g_pQForkControl->inMemoryBuffersControlHandle) {
                CloseHandle(g_pQForkControl->inMemoryBuffersControlHandle);
                g_pQForkControl->inMemoryBuffersControlHandle = NULL;
            }

            // restore protection constants on shared memory blocks 
            DWORD oldProtect = 0;
            IFFAILTHROW(VirtualProtect(g_pQForkControl, sizeof(QForkControl), PAGE_READWRITE, &oldProtect), "AdvanceForkCleanup: VirtualProtect 3 failed.");            

            LPVOID controlCopy = malloc(sizeof(QForkControl));
            IFFAILTHROW(controlCopy, "AdvanceForkCleanup: allocation failed.");

            memcpy(controlCopy, g_pQForkControl, sizeof(QForkControl));
            IFFAILTHROW(UnmapViewOfFile(g_pQForkControl), "AdvanceForkCleanup: UnmapViewOfFile failed.");
            
            g_pQForkControl = (QForkControl*)
                MapViewOfFileEx(
                g_hQForkControlFileMap,
                FILE_MAP_ALL_ACCESS,
                0, 0,
                0,
                g_pQForkControl);
            IFFAILTHROW(g_pQForkControl, "AdvanceForkCleanup: Remapping ForkControl failed.");
            
            memcpy(g_pQForkControl, controlCopy, sizeof(QForkControl));
            delete controlCopy;


            g_CleanupState.copyBatchSize = 1024;

            g_CleanupState.currentState = osCLEANING;
            redisLog(REDIS_VERBOSE, "Reseting Global state finished");
            if (!forceEnd)
                return;
        }

        if (g_CleanupState.currentState == osCLEANING) {

            HANDLE hProcess = GetCurrentProcess();

            size_t size = g_CleanupState.copyBatchSize * pageSize;
            do {

                if (g_CleanupState.offsetCopied + size > g_pQForkControl->availableBlocksInHeap * g_pQForkControl->heapBlockSize) {
                    size = g_pQForkControl->availableBlocksInHeap * g_pQForkControl->heapBlockSize - g_CleanupState.offsetCopied;
                }

                int block = (int)(g_CleanupState.offsetCopied / g_pQForkControl->heapBlockSize);
                void * heapAltRegion = MapViewOfFileEx(g_pQForkControl->heapBlockMap[block].heapMemoryMap,
                    FILE_MAP_ALL_ACCESS,
                    HIDWORD(g_CleanupState.offsetCopied - g_pQForkControl->heapBlockSize * block),
                    LODWORD(g_CleanupState.offsetCopied - g_pQForkControl->heapBlockSize * block),
                    size,
                    0);
                IFFAILTHROW(heapAltRegion, "MapViewOfFileEx failure");

                int pages = (int)(size / pageSize);

                DWORD oldProtect;
                IFFAILTHROW(VirtualProtect(
                    (BYTE*)g_pQForkControl->heapStart + g_CleanupState.offsetCopied,
                    size,
                    PAGE_READWRITE,
                    &oldProtect),
                    "AdvanceForkCleanup: VirtualProtect 4 failed.");

                for (int page = 0; page < pages; page++) {
                    g_CleanupState.scannedPages++;
                    size_t offset = g_CleanupState.offsetCopied + page * pageSize;
                    LPVOID addr = (BYTE*)g_pQForkControl->heapStart + offset;
                    int bit;
                    uint64_t slot = AddrToBitSlot(addr, &bit);
                    if ((g_CleanupState.pageBitMap[slot] & (1ULL << bit)) != 0) {

                        g_CleanupState.copiedPages++;
                        memcpy(
                            (BYTE*)heapAltRegion + page * pageSize,
                            (BYTE*)addr,
                            pageSize);
                        IFFAILTHROW(VirtualProtect(addr, pageSize, PAGE_READWRITE | PAGE_REVERT_TO_FILE_MAP, &oldProtect), "EndForkOperation: Revert to file map failed.");
                    } else {
                        g_CleanupState.pageBitMap[slot] |= (1ULL << bit);
                    }
                }

                IFFAILTHROW(UnmapViewOfFile(heapAltRegion), "AdvanceForkCleanup: UnmapViewOfFile failed.");
                g_CleanupState.offsetCopied += size;
                if (g_CleanupState.offsetCopied == g_CleanupState.heapBlocksToCleanup * g_pQForkControl->heapBlockSize) {
                    g_CleanupState.currentState = osCLEANEDUP;
                    break;
                }
            } while (forceEnd);

            if (g_CleanupState.currentState == osCLEANEDUP) {
                ClearInMemoryBuffersMasterParent();
                redisLog(REDIS_NOTICE, "Copied changed pages: %d", g_CleanupState.copiedPages);
            }
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


void AbortForkOperation(BOOL blockUntilCleanedup)
{
    try {
        redisLog(REDIS_NOTICE, "Aborting child process");
        TransitionToFreeWindow(TRUE);
        if (blockUntilCleanedup)
            SetEvent(g_pQForkControl->operationFailed);
        OperationStatus os = GetForkOperationStatus(blockUntilCleanedup);
        int failed = os & osFAILED;
        int inMemory = os & osINMEMORY;
        os = (OperationStatus)(os & ~(osFAILED | osINMEMORY));

        if (blockUntilCleanedup) {
            switch (os) {
            default:
            case osUNSTARTED:
                redisLog(REDIS_WARNING, "Unknown OS: %d.  Aborting", os);
                exit(1);
                break;
            case osINPROGRESS:
            case osCOMPLETE:
            case osWAITINGFOREXIT:
                redisLog(REDIS_WARNING, "Disallowed OS: %d.  Aborting", os);
                exit(1);
                break;
            case osEXITED:
            case osCLEANING:
                AdvanceCleanupForkOperation(TRUE, NULL);
                break;
            case osCLEANEDUP:
                break;
            }
            EndForkOperation(NULL);
        } else {
            switch (os) {
            default:
            case osUNSTARTED:
                redisLog(REDIS_WARNING, "Unknown OS: %d.  Aborting", os);
                exit(1);
                break;
            case osINPROGRESS:
                SetEvent(g_pQForkControl->operationFailed);
                GetForkOperationStatus(TRUE); // this will kill the child, and advance to exited
                break;
            case osCOMPLETE:
            case osWAITINGFOREXIT:
            case osEXITED:
            case osCLEANING:
            case osCLEANEDUP:
                break;
            }
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


BOOL PhysicalMapMemory(int block)
{
    try {
    AGAIN:
        char * path;
        if (g_DataFilePaths.countInPrimaryPath) {
            g_DataFilePaths.countInPrimaryPath--;
            path = g_DataFilePaths.PrimaryPath;
        } else {
            path = ".";
        }
        char heapMemoryMapPath[MAX_PATH];
        sprintf_s(
            heapMemoryMapPath,
            MAX_PATH,
            "%s\\%s_%d_%d.dat",
            path,
            cMapFileBaseName,
            GetCurrentProcessId(),
            block);

        HANDLE file =
            CreateFileA(
            heapMemoryMapPath,
            GENERIC_READ | GENERIC_WRITE,
            0,
            NULL,
            CREATE_ALWAYS,
            FILE_ATTRIBUTE_NORMAL | FILE_FLAG_DELETE_ON_CLOSE,
            NULL);
        if (file == INVALID_HANDLE_VALUE && path == g_DataFilePaths.PrimaryPath) {
            g_DataFilePaths.countInPrimaryPath = 0;
            goto AGAIN;
        }

        IFFAILTHROW(file != INVALID_HANDLE_VALUE, "PhysicalMapMemory: CreateFileA failed.");

        SIZE_T mmSize = g_pQForkControl->heapBlockSize;

        HANDLE map =
            CreateFileMappingW(
            file,
            NULL,
            PAGE_READWRITE,
            HIDWORD(mmSize),
            LODWORD(mmSize),
            NULL);
        IFFAILTHROW(map, "PhysicalMapMemory: CreateFileMapping failed.");

        LPVOID addr = (byte*)g_pQForkControl->heapStart + block * mmSize;

        IFFAILTHROW(VirtualFree(addr, 0, MEM_RELEASE), "PhysicalMapMemory: VirtualFree failed.");

        LPVOID realAddr = MapViewOfFileEx(map, FILE_MAP_ALL_ACCESS, 0, 0, 0, addr);
        IFFAILTHROW(realAddr, "PhysicalMapMemory: MapViewOfFileEx failed.");

        g_pQForkControl->heapBlockMap[block].heapMemoryMap = map;
        g_pQForkControl->heapBlockMap[block].heapMemoryMapFile = file;
        g_pQForkControl->heapBlockMap[block].state = bsMAPPED;

        return TRUE;

    }
    catch (std::system_error syserr) {
        redisLog(REDIS_WARNING, "PhysicalMapMemory: system error caught. error code=0x%08x, message=%s", syserr.code().value(), syserr.what());
    }
    catch (std::runtime_error runerr) {
        redisLog(REDIS_WARNING, "PhysicalMapMemory: runtime error caught. message=%s", runerr.what());
    }
    catch (...) {
        redisLog(REDIS_WARNING, "PhysicalMapMemory: other exception caught.");
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

    int startIndex = allocateHigh ? g_pQForkControl->availableBlocksInHeap - 1 : contiguousBlocksToAllocate - 1;
    int endIndex = allocateHigh ? -1 : g_pQForkControl->availableBlocksInHeap - contiguousBlocksToAllocate + 1;
    int direction = allocateHigh ? -1 : 1;
    int blockIndex = 0;
    int contiguousBlocksFound = 0;
    for(blockIndex = startIndex; 
        blockIndex != endIndex; 
        blockIndex += direction) {
        for (int n = 0; n < contiguousBlocksToAllocate; n++) {
            if (g_pQForkControl->heapBlockMap[blockIndex + n * direction].state == BlockState::bsUNMAPPED) {
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
            g_pQForkControl->heapBlockMap[allocationStart+n].state = BlockState::bsMAPPED;
            blocksMapped++;
        }
        retPtr = blockStart;
    }
    else if (g_pQForkControl->availableBlocksInHeap - contiguousBlocksFound + contiguousBlocksToAllocate <= g_pQForkControl->maxAvailableBlockInHeap) {
        LPVOID blockStart = reinterpret_cast<byte*>(g_pQForkControl->heapStart) + (g_pQForkControl->heapBlockSize * (g_pQForkControl->availableBlocksInHeap - contiguousBlocksFound));
        for (int x = g_pQForkControl->availableBlocksInHeap - contiguousBlocksFound; x < g_pQForkControl->availableBlocksInHeap - contiguousBlocksFound + contiguousBlocksToAllocate; x++) {
            if (x >= g_pQForkControl->availableBlocksInHeap) {
                if (!PhysicalMapMemory(x)) {
                    errno = ENOMEM;
                    return NULL;
                }
            } else {
                g_pQForkControl->heapBlockMap[x].state = BlockState::bsMAPPED;
                blocksMapped++;
            }
        }
        g_pQForkControl->availableBlocksInHeap = g_pQForkControl->availableBlocksInHeap - contiguousBlocksFound + contiguousBlocksToAllocate;
        retPtr = blockStart;
    } else {
        errno = ENOMEM;
        return NULL;
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
        g_pQForkControl->heapBlockMap[blockIndex + n].state = BlockState::bsUNMAPPED;
    }
    return TRUE;
}

void SetupLogging() {
    bool serviceRun = g_argMap.find(cServiceRun) != g_argMap.end();
    string syslogEnabledValue = (g_argMap.find(cSyslogEnabled) != g_argMap.end() ? g_argMap[cSyslogEnabled].at(0).at(0) : cNo);
    bool syslogEnabled = (syslogEnabledValue.compare(cYes) == 0) || serviceRun;
    string syslogIdent = (g_argMap.find(cSyslogIdent) != g_argMap.end() ? g_argMap[cSyslogIdent].at(0).at(0) : cDefaultSyslogIdent);
    string logFileName = (g_argMap.find(cLogfile) != g_argMap.end() ? g_argMap[cLogfile].at(0).at(0) : cDefaultLogfile);

    setSyslogEnabled(syslogEnabled);
    if (syslogEnabled) {
        setSyslogIdent(syslogIdent.c_str());
    } else {
        setLogFile(logFileName.c_str());
    }
}


void GetHeapExtent(HeapExtent * pextent) {
    pextent->heapStart = g_pQForkControl->heapStart;
    pextent->heapEnd = (char*)g_pQForkControl->heapStart + (g_pQForkControl->availableBlocksInHeap * g_pQForkControl->heapBlockSize);
}

extern "C"
{
    // The external main() is redefined as redis_main() by Win32_QFork.h.
    // The CRT will call this replacement main() before the previous main()
    // is invoked so that the QFork allocator can be setup prior to anything 
    // Redis will allocate.
    int main(int argc, char* argv[]) {
        try {
            ParseCommandLineArguments(argc, argv);
            SetupLogging();
        } catch (runtime_error &re) {
            cout << re.what() << endl;
            exit(-1);
        }
        
        RegisterMiniDumpHandler();

        try {
#ifdef DEBUG_WITH_PROCMON
            hProcMonDevice =
                CreateFile(
                L"\\\\.\\Global\\ProcmonDebugLogger",
                GENERIC_READ | GENERIC_WRITE,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                NULL,
                OPEN_EXISTING,
                FILE_ATTRIBUTE_NORMAL,
                NULL);
#endif

            // service commands do not launch an instance of redis directly
            if (HandleServiceCommands(argc, argv) == TRUE)
                return 0;

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
        } catch (std::system_error syserr) {
            ::redisLog(REDIS_WARNING, "main: system error caught. error code=0x%08x, message=%s\n", syserr.code().value(), syserr.what());
        } catch (std::runtime_error runerr) {
            ::redisLog(REDIS_WARNING, "main: runtime error caught. message=%s\n", runerr.what());
        } catch (...) {
            ::redisLog(REDIS_WARNING, "main: other exception caught.\n");
        }
    }
}



