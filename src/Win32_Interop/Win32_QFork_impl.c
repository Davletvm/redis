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
    server.repl_inMemorySend->buffer[0] = control->buffer[0][0].b;
    server.repl_inMemorySend->buffer[1] = control->buffer[1][0].b;
    server.repl_inMemorySend->sizeFilled[0] = &(control->buffer[0][0].s);
    server.repl_inMemorySend->sizeFilled[1] = &(control->buffer[1][0].s);
    server.repl_inMemorySend->sizeFilled[0][0] = 0;
    server.repl_inMemorySend->sizeFilled[1][0] = 0;
    server.repl_inMemorySend->bufferSize = control->bufferSize;
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


void SendBuffer(redisInMemoryReplSend * inm, int which, int sequence)
{
    ResetEvent(inm->sentDoneEvents[which]);
    inm->sendState[which] = INMEMORY_STATE_READYTOSEND;
    inm->sequence[which] = sequence;
    redisLog(REDIS_DEBUG, "Sending partially filled buffer %d", which);
    ResetEvent(inm->sentDoneEvents[which]);
    SetEvent(inm->doSendEvents[which]);
}

int do_rdbSaveInMemory(InMemoryBuffersControl * buffers, HANDLE doSend[2], HANDLE doneSent[2])
{
#ifndef NO_QFORKIMPL
    redisInMemoryReplSend inMemoryRepl;
    DWORD rval;
    memset(&inMemoryRepl, 0, sizeof(inMemoryRepl));
    inMemoryRepl.id = buffers->id;
    inMemoryRepl.buffer[0] = buffers->buffer[0][0].b;
    inMemoryRepl.buffer[1] = buffers->buffer[1][0].b;
    inMemoryRepl.sizeFilled[0] = &(buffers->buffer[0][0].s);
    inMemoryRepl.sizeFilled[1] = &(buffers->buffer[1][0].s);
    inMemoryRepl.bufferSize = buffers->bufferSize;
    inMemoryRepl.doSendEvents = doSend;
    inMemoryRepl.sentDoneEvents = doneSent;
    inMemoryRepl.sequence = buffers->bufferSequence;
    inMemoryRepl.sendState = buffers->bufferState;
    server.repl_inMemorySend = &inMemoryRepl;
    server.rdb_child_pid = GetCurrentProcessId();
    redisLog(REDIS_DEBUG, "Save inmemory starting");
    if (rdbSave(NULL) != REDIS_OK) {
        redisLog(REDIS_WARNING, "rdbSave failed in qfork: %s", strerror(errno));
        return REDIS_ERR;
    }
    redisLog(REDIS_DEBUG, "Save inmemory finished");
    int activeBuffer = inMemoryRepl.activeBuffer;
    int inActiveBuffer = activeBuffer ? 0 : 1;
    int sentEmpty = 0;
    // If the buffer had been full, it would have been sent already.
    if (inMemoryRepl.sizeFilled[activeBuffer][0] != inMemoryRepl.bufferSize) {
        SendBuffer(&inMemoryRepl, activeBuffer, INT32_MAX - 1);
        if (!inMemoryRepl.sizeFilled[activeBuffer][0])
            sentEmpty = 1;
    }
    // do we have an available empty buffer to send?
    if (!sentEmpty) {
        if (inMemoryRepl.sendState[activeBuffer] != INMEMORY_STATE_READYTOSEND) {
            redisAssert(!inMemoryRepl.sizeFilled[activeBuffer][0]);
            SendBuffer(&inMemoryRepl, activeBuffer, INT32_MAX);
            sentEmpty = 1;
        } else if (inMemoryRepl.sendState[inActiveBuffer] != INMEMORY_STATE_READYTOSEND) {
            redisAssert(!inMemoryRepl.sizeFilled[inActiveBuffer][0]);
            SendBuffer(&inMemoryRepl, inActiveBuffer, INT32_MAX);
            sentEmpty = 1;
        }
    }
    // If all buffers are in flight, we need to wait for the first one
    if (!sentEmpty) {
        redisLog(REDIS_DEBUG, "Waiting for send complete on both buffers");
        rval = WaitForMultipleObjects(2, doneSent, FALSE, INFINITE);
        if (rval < WAIT_OBJECT_0 || rval > WAIT_OBJECT_0 + 1)
            return REDIS_ERR;
        redisLog(REDIS_DEBUG, "Send complete received on %d", rval - WAIT_OBJECT_0);
        inMemoryRepl.sizeFilled[rval - WAIT_OBJECT_0][0] = 0;
        SendBuffer(&inMemoryRepl, rval - WAIT_OBJECT_0, INT32_MAX);
    }
    // Now just wait for all to have been sent
    if (inMemoryRepl.sendState[0] == INMEMORY_STATE_READYTOSEND) {
        redisLog(REDIS_DEBUG, "Waiting for send complete on buffer %d", 0);
        rval = WaitForSingleObject(doneSent[0], INFINITE);
        if (rval != WAIT_OBJECT_0) return REDIS_ERR;
        redisLog(REDIS_DEBUG, "Send complete received on %d", 0);
    }
    if (inMemoryRepl.sendState[1] == INMEMORY_STATE_READYTOSEND) {
        redisLog(REDIS_DEBUG, "Waiting for send complete on buffer %d", 1);
        rval = WaitForSingleObject(doneSent[1], INFINITE);
        if (rval != WAIT_OBJECT_0) return REDIS_ERR;
        redisLog(REDIS_DEBUG, "Send complete received on %d", 1);
    }
#endif
    return REDIS_OK;

}



