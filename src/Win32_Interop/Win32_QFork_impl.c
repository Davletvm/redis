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
static int control_id = 0;

void aeHandleEventCallbackProc(aeEventLoop * el, void * param)
{
#ifndef NO_QFORKIMPL
    int id = (int)param;
    sendInMemoryBuffersToSlave(el, id);
#endif
}


void SetupInMemoryBuffersMasterParent(InMemoryBuffersControl * control, HANDLE doSend[2], HANDLE doneSent[2])
{
#ifndef NO_QFORKIMPL
    control->id = control_id++;

    server.repl_inMemorySend = zcalloc(sizeof(redisInMemoryReplSend));
    server.repl_inMemorySend->id = control->id;
    server.repl_inMemorySend->buffer[0] = control->buffer[0].b;
    server.repl_inMemorySend->buffer[1] = control->buffer[1].b;
    server.repl_inMemorySend->sizeFilled[0] = &(control->buffer[0].s);
    server.repl_inMemorySend->sizeFilled[1] = &(control->buffer[1].s);
    server.repl_inMemorySend->bufferSize = InMemoryMasterBufferSize;
    server.repl_inMemorySend->doSendEvents = doSend;
    server.repl_inMemorySend->sentDoneEvents = doneSent;
    server.repl_inMemorySend->sequence = control->bufferSequence;
    server.repl_inMemorySend->sendState = control->bufferState;
    server.repl_inMemorySend->sendState[0] = INMEMORY_STATE_INVALID;
    server.repl_inMemorySend->sendState[1] = INMEMORY_STATE_INVALID;
    server.repl_inMemorySend->sequence[0] = -1;
    server.repl_inMemorySend->sequence[1] = -1;

    aeSetCallbacks(server.el, aeHandleEventCallbackProc, 2, doSend, server.repl_inMemorySend->id);
#endif
}


int do_rdbSaveInMemory(InMemoryBuffersControl * buffers, HANDLE doSend[2], HANDLE doneSent[2])
{
#ifndef NO_QFORKIMPL
    redisInMemoryReplSend inMemoryRepl;
    DWORD rval;
    memset(&inMemoryRepl, 0, sizeof(inMemoryRepl));
    inMemoryRepl.id = buffers->id;
    inMemoryRepl.buffer[0] = buffers->buffer[0].b;
    inMemoryRepl.buffer[1] = buffers->buffer[1].b;
    inMemoryRepl.sizeFilled[0] = &(buffers->buffer[0].s);
    inMemoryRepl.sizeFilled[1] = &(buffers->buffer[1].s);
    inMemoryRepl.bufferSize = InMemoryMasterBufferSize;
    inMemoryRepl.doSendEvents = doSend;
    inMemoryRepl.sentDoneEvents = doneSent;
    inMemoryRepl.sequence = buffers->bufferSequence;
    inMemoryRepl.sendState = buffers->bufferState;
    server.repl_inMemorySend = &inMemoryRepl;
    server.rdb_child_pid = GetCurrentProcessId();
    redisLog(REDIS_NOTICE, "Save inmemory starting");
    if (rdbSave(NULL) != REDIS_OK) {
        redisLog(REDIS_WARNING, "rdbSave failed in qfork: %s", strerror(errno));
        return REDIS_ERR;
    }
    redisLog(REDIS_NOTICE, "Save inmemory finished");
    int activeBuffer = inMemoryRepl.activeBuffer;
    int inActiveBuffer = activeBuffer ? 0 : 1;
    // If the buffer had been full, it would have been sent already.
    if (*(inMemoryRepl.sizeFilled[activeBuffer]) != inMemoryRepl.bufferSize && *(inMemoryRepl.sizeFilled[activeBuffer])) {
        ResetEvent(doneSent[activeBuffer]);
        inMemoryRepl.sendState[activeBuffer] = INMEMORY_STATE_READYTOSEND;
        inMemoryRepl.sequence[activeBuffer] = INT32_MAX;
        redisLog(REDIS_NOTICE, "Sending partially filled buffer %d", activeBuffer);
        SetEvent(doSend[activeBuffer]);
    }
    if (*(inMemoryRepl.sizeFilled[activeBuffer])) {
        redisLog(REDIS_NOTICE, "Waiting for send complete on buffer %d", activeBuffer);
        rval = WaitForSingleObject(doneSent[activeBuffer], INFINITE);
        if (rval != WAIT_OBJECT_0) return REDIS_ERR;
        redisLog(REDIS_NOTICE, "Send complete received on %d", activeBuffer);
    }
    // The other buffer had been sent if it was full
    if (*(inMemoryRepl.sizeFilled[inActiveBuffer])) {
        redisAssert(*(inMemoryRepl.sizeFilled[inActiveBuffer]) == inMemoryRepl.bufferSize);
        redisLog(REDIS_NOTICE, "Waiting for send complete on buffer %d", inActiveBuffer);
        DWORD rval = WaitForSingleObject(doneSent[inActiveBuffer], INFINITE);
        if (rval != WAIT_OBJECT_0) return REDIS_ERR;
        redisLog(REDIS_NOTICE, "Send complete received on %d", inActiveBuffer);
    }
#endif
    return REDIS_OK;

}



