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

#define MAXSENDBUFFER 4
typedef struct SPBuffer {
    int s;
    char b[1024];
}SPBuffer;
typedef struct InMemoryBuffersControl {
    int bufferSequence[MAXSENDBUFFER];
    int bufferState[MAXSENDBUFFER];
    int id;
    int bufferSize;
    size_t heapOffset;
    SPBuffer * buffer[MAXSENDBUFFER];
} InMemoryBuffersControl;


#ifdef __cplusplus
extern "C" {
#endif

    void SetupGlobals(LPVOID globalData, size_t globalDataSize, unsigned __int32 dictHashKey);
    int do_rdbSave(char* filename);
    int do_aofSave(char* filename);
    int do_rdbSaveInMemory(InMemoryBuffersControl * buffers, HANDLE doSend[2], HANDLE doneSent[2]);
    void SetupInMemoryBuffersMasterParent(InMemoryBuffersControl * control, HANDLE doSend[2], HANDLE doneSent[2]);
    void ClearInMemoryBuffersMasterParent();

#ifdef __cplusplus
}
#endif