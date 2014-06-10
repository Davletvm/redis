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

#include "..\redis.h"
#include "..\rdb.h"
#include "..\rio.h"
#include "Win32_QFork_impl.h"

void SetupGlobals(LPVOID globalData, size_t globalDataSize, uint32_t dictHashSeed)
{
#ifndef NO_QFORKIMPL
    memcpy(&server, globalData, globalDataSize);
    dictSetHashFunctionSeed(dictHashSeed);
    setLogVerbosityLevel(server.verbosity);
#endif
}

int do_rdbSave(char* filename)
{
#ifndef NO_QFORKIMPL
    server.rdb_child_pid = GetCurrentProcessId();
    if( rdbSave(filename) != REDIS_OK ) {
        redisLog(REDIS_WARNING,"rdbSave failed in qfork: %s", strerror(errno));
        return REDIS_ERR;
    }
#endif
    return REDIS_OK;
}

int do_aofSave(char* filename)
{
#ifndef NO_QFORKIMPL
    int rewriteAppendOnlyFile(char *filename);

    server.aof_child_pid = GetCurrentProcessId();
    if( rewriteAppendOnlyFile(filename) != REDIS_OK ) {
        redisLog(REDIS_WARNING,"rewriteAppendOnlyFile failed in qfork: %s", strerror(errno));
        return REDIS_ERR;
    }
#endif

    return REDIS_OK;
}

void ClearInMemoryBuffersMasterParent()
{
#ifndef NO_QFORKIMPL
    aeClearCallbacks(server.el);
    if (server.repl_inMemorySend) {
        zfree(server.repl_inMemorySend);
        server.repl_inMemorySend = NULL;
    }
#endif
}
static int control_id = 2;

void aeHandleEventCallbackProc(aeEventLoop * el, void * param)
{
#ifndef NO_QFORKIMPL
    int id = (int)param;
    if (id < 0) {
        sendInMemoryBuffersToSlavePing(el, -id);
    } else {
        sendInMemoryBuffersToSlave(el, id);
    }
#endif
}


void SetupInMemoryBuffersMasterParent(InMemoryBuffersControl * control, HANDLE doSend[MAXSENDBUFFER], HANDLE doneSent[MAXSENDBUFFER], HANDLE pingHandle)
{
#ifndef NO_QFORKIMPL
    control->id = control_id++;

    server.repl_inMemorySend = zcalloc(sizeof(redisInMemoryReplSend));
    server.repl_inMemorySend->id = control->id;
    server.repl_inMemorySend->bufferSize = control->bufferSize;
    server.repl_inMemorySend->doSendEvents = doSend;
    server.repl_inMemorySend->sentDoneEvents = doneSent;
    server.repl_inMemorySend->sequence = control->bufferSequence;
    server.repl_inMemorySend->sendState = control->bufferState;
    server.repl_inMemorySend->pingHandle = pingHandle;
    HANDLE sendHandles[MAXSENDBUFFER + 1];
    int IDs[MAXSENDBUFFER + 1];
    for (int x = 0; x < MAXSENDBUFFER; x++) {
        server.repl_inMemorySend->controlAlias[x] = server.repl_inMemorySend->buffer[x] = control->buffer[x][0].b;
        server.repl_inMemorySend->controlAlias[x]->countOfOOB = 0;
        server.repl_inMemorySend->sizeFilled[x] = &(control->buffer[x][0].s);
        server.repl_inMemorySend->sizeFilled[x][0] = sizeof(redisInMemoryReplSendControl);
        server.repl_inMemorySend->sendState[x] = INMEMORY_STATE_READYTOFILL;
        server.repl_inMemorySend->sequence[x] = -1;
        IDs[x] = server.repl_inMemorySend->id;
        sendHandles[x] = doSend[x];
    }
    IDs[MAXSENDBUFFER] = -server.repl_inMemorySend->id;
    sendHandles[MAXSENDBUFFER] = pingHandle;
    aeSetCallbacks(server.el, aeHandleEventCallbackProc, MAXSENDBUFFER + 1, sendHandles, IDs);
#endif
}


void SendBuffer(redisInMemoryReplSend * inm, int which, int sequence)
{
    ResetEvent(inm->sentDoneEvents[which]);
    inm->sendState[which] = INMEMORY_STATE_READYTOSEND;
    inm->sequence[which] = sequence;
    redisLog(REDIS_DEBUG, "Sending partially filled buffer %d", which);
    ResetEvent(inm->sentDoneEvents[which]);
    SetEvent(inm->doSendEvents[which]);
}

int do_rdbSaveInMemory(InMemoryBuffersControl * buffers, HANDLE doSend[2], HANDLE doneSent[2], HANDLE pingHandle)
{
#ifndef NO_QFORKIMPL
    redisInMemoryReplSend inMemoryRepl;
    DWORD rval;
    memset(&inMemoryRepl, 0, sizeof(inMemoryRepl));
    inMemoryRepl.id = buffers->id;
    inMemoryRepl.bufferSize = buffers->bufferSize;
    for (int x = 0; x < MAXSENDBUFFER; x++) {
        inMemoryRepl.controlAlias[x] = inMemoryRepl.buffer[x] = buffers->buffer[x][0].b;
        inMemoryRepl.sizeFilled[x] = &(buffers->buffer[x][0].s);
        inMemoryRepl.virtualSize[x] = 0;
    }
    inMemoryRepl.pingHandle = pingHandle;
    inMemoryRepl.doSendEvents = doSend;
    inMemoryRepl.sentDoneEvents = doneSent;
    inMemoryRepl.sequence = buffers->bufferSequence;
    inMemoryRepl.sendState = buffers->bufferState;
    inMemoryRepl.heapOffset = buffers->heapOffset;
    inMemoryRepl.prevActiveBuffer = -1;
    server.repl_inMemorySend = &inMemoryRepl;
    server.rdb_child_pid = GetCurrentProcessId();
    redisLog(REDIS_VERBOSE, "Child: Save inmemory starting");
    if (rdbSave(NULL) != REDIS_OK) {
        redisLog(REDIS_WARNING, "rdbSave failed in qfork: %s", strerror(errno));
        return REDIS_ERR;
    }
    redisLog(REDIS_VERBOSE, "Child: Save inmemory finished");
    SendActiveBufferIM(&inMemoryRepl);
    inMemoryRepl.activeBuffer = -1;
    SendActiveBufferIM(&inMemoryRepl);
    for (int x = 0; x < MAXSENDBUFFER; x++) {
        if (inMemoryRepl.sendState[x] == INMEMORY_STATE_READYTOSEND) {
            redisLog(REDIS_DEBUG, "Waiting for send complete on buffer %d", x);
            rval = WaitForSingleObject(doneSent[x], INFINITE);
            if (rval != WAIT_OBJECT_0) return REDIS_ERR;
            redisLog(REDIS_DEBUG, "Send complete received on %d", x);
        }
    }
#endif
    return REDIS_OK;

}



