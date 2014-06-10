/* rio.c is a simple stream-oriented I/O abstraction that provides an interface
 * to write code that can consume/produce data using different concrete input
 * and output devices. For instance the same rdb.c code using the rio
 * abstraction can be used to read and write the RDB format using in-memory
 * buffers or files.
 *
 * A rio object provides the following methods:
 *  read: read from stream.
 *  write: write to stream.
 *  tell: get the current offset.
 *
 * It is also possible to set a 'checksum' method that is used by rio.c in order
 * to compute a checksum of the data written or read, or to query the rio object
 * for the current checksum.
 *
 * ----------------------------------------------------------------------------
 *
 * Copyright (c) 2009-2012, Pieter Noordhuis <pcnoordhuis at gmail dot com>
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */


#include "fmacros.h"
#include <string.h>
#include <stdio.h>
#ifndef _WIN32
#include <unistd.h>
#endif
#include "util.h"
#include "config.h"
#include "redis.h"
#include "rio.h"
#include "crc64.h"
#include "config.h"
#include "redis.h"

/* Returns 1 or 0 for success/failure. */
static size_t rioBufferWrite(rio *r, const void *buf, size_t len) {
    r->io.buffer.ptr = sdscatlen(r->io.buffer.ptr,(char*)buf,len);
    r->io.buffer.pos += (off_t)len;
    return 1;
}

/* Returns 1 or 0 for success/failure. */
static size_t rioBufferRead(rio *r, void *buf, size_t len) {
    if (sdslen(r->io.buffer.ptr)-r->io.buffer.pos < len)
        return 0; /* not enough buffer to return len bytes. */
    memcpy(buf,r->io.buffer.ptr+r->io.buffer.pos,len);
    r->io.buffer.pos += (off_t)len;
    return 1;
}

/* Returns read/write position in buffer. */
static off_t rioBufferTell(rio *r) {
    return r->io.buffer.pos;
}

/* Returns 1 or 0 for success/failure. */
static size_t rioFileWrite(rio *r, const void *buf, size_t len) {
    size_t retval;

    retval = fwrite(buf,len,1,r->io.file.fp);
    r->io.file.buffered += (off_t)len;

    if (r->io.file.autosync &&
        r->io.file.buffered >= r->io.file.autosync)
    {
        aof_fsync(fileno(r->io.file.fp));
        r->io.file.buffered = 0;
    }
    return retval;
}

/* Returns 1 or 0 for success/failure. */
static size_t rioFileRead(rio *r, void *buf, size_t len) {
    return fread(buf,len,1,r->io.file.fp);
}

/* Returns read/write position in file. */
static off_t rioFileTell(rio *r) {
    return (off_t)ftello(r->io.file.fp);
}

static void SendActiveBuffer(rio * r) {
    SendActiveBufferIM(r->io.memorySend.inMemory);
}


void SendActiveBufferIM(redisInMemoryReplSend * inm)
{
    if (inm->prevActiveBuffer != -1) {
        if (inm->activeBuffer != -1)
            inm->controlAlias[inm->prevActiveBuffer]->sizeOfNext = inm->sizeFilled[inm->activeBuffer][0];
        else
            inm->controlAlias[inm->prevActiveBuffer]->sizeOfNext = 0;
        inm->sendState[inm->prevActiveBuffer] = INMEMORY_STATE_READYTOSEND;
        inm->sequence[inm->prevActiveBuffer] = inm->curSequence++;
        redisLog(REDIS_DEBUG, "Ready to send buffer %d, sequence:%d", inm->prevActiveBuffer, inm->sequence[inm->prevActiveBuffer]);
        ResetEvent(inm->sentDoneEvents[inm->prevActiveBuffer]);
        SetEvent(inm->doSendEvents[inm->prevActiveBuffer]);
        server.unixtime = time(NULL);
        inm->lastPingMS = server.unixtime;
    }
    if (inm->activeBuffer != -1) {
        inm->prevActiveBuffer = inm->activeBuffer;
        inm->sendState[inm->activeBuffer] = INMEMORY_STATE_FILLED;
        inm->controlAlias[inm->activeBuffer]->sizeOfInline = inm->sizeFilled[inm->activeBuffer][0] - sizeof(redisInMemoryReplSendControl);
        inm->controlAlias[inm->activeBuffer]->sizeOfThis = inm->sizeFilled[inm->activeBuffer][0];
        int x = 0; int y = inm->controlAlias[inm->activeBuffer]->countOfOOB - 1;
        WSABUF * buffer = inm->buffer[inm->activeBuffer] + inm->bufferSize - inm->controlAlias[inm->activeBuffer]->countOfOOB * sizeof(WSABUF);
        while (x < y) {
            WSABUF temp;
            temp = buffer[x];
            buffer[x] = buffer[y];
            buffer[y] = temp;
            x++;
            y--;
        }
    }
}

static int WaitForFreeBuffer(rio * r)
{
    redisLog(REDIS_DEBUG, "Waiting for free buffers.");
    redisInMemoryReplSend * inm = r->io.memorySend.inMemory;
    WaitForMultipleObjects(INMEMORY_SEND_MAXSENDBUFFER, inm->sentDoneEvents, FALSE, INFINITE);
    BOOL found = FALSE;
    for (int x = 0; x < INMEMORY_SEND_MAXSENDBUFFER; x++) {
        DWORD rval = WaitForSingleObject(inm->sentDoneEvents[x], 0);
        if (rval == WAIT_OBJECT_0) {
            redisLog(REDIS_DEBUG, "Got free buffer %d", x);
            ResetEvent(inm->sentDoneEvents[x]);
            inm->virtualSize[x] = inm->sizeFilled[x][0] = sizeof(redisInMemoryReplSendControl);
            found = TRUE;
            inm->sendState[x] = INMEMORY_STATE_READYTOFILL;
            inm->controlAlias[x]->countOfOOB = 0;
        } else if (rval != WAIT_TIMEOUT) {
            return 0;
        }
    }
    return found;
}


/* Returns 1 or 0 for success/failure.
    This is only called by the forked child
    It writes to all r members.
    The parent only reads the r members, and only
    when signaled to do so.
*/
static size_t rioMemoryWrite(rio *r, const void *buf, size_t len) {
    char * buffer;
    ssize_t leftInActiveBuffer;
    size_t lenToCopy;
    redisInMemoryReplSend * inm = r->io.memorySend.inMemory;
    while (server.repl_inMemorySend == inm) {
        if (inm->sendState[inm->activeBuffer] != INMEMORY_STATE_READYTOFILL && inm->sendState[inm->activeBuffer] != INMEMORY_STATE_BEINGFILLED) {
            int activeBufferPrev = inm->activeBuffer;
            while (1) {
                inm->activeBuffer++;
                if (inm->activeBuffer == INMEMORY_SEND_MAXSENDBUFFER) inm->activeBuffer = 0;
                if (inm->sendState[inm->activeBuffer] == INMEMORY_STATE_READYTOFILL || inm->activeBuffer == activeBufferPrev)
                    break;
            }
        }
        if (inm->sendState[inm->activeBuffer] != INMEMORY_STATE_READYTOFILL && inm->sendState[inm->activeBuffer] != INMEMORY_STATE_BEINGFILLED) {
            if (!WaitForFreeBuffer(r))
                return 0;
            continue;
        }
        leftInActiveBuffer = inm->bufferSize - inm->sizeFilled[inm->activeBuffer][0] - (inm->controlAlias[inm->activeBuffer]->countOfOOB  + 1) * sizeof(WSABUF);
        if (len > INMEMORY_SEND_MINLENGTHOOB) {
            lenToCopy = sizeof(WSABUF);
        } else {
            lenToCopy = len;
        }
        if (lenToCopy > leftInActiveBuffer || inm->virtualSize[inm->activeBuffer] > inm->bufferSize) {
            SendActiveBuffer(r);
            continue;
        }
        inm->sendState[inm->activeBuffer] = INMEMORY_STATE_BEINGFILLED;
        buffer = inm->buffer[inm->activeBuffer] + *(inm->sizeFilled[inm->activeBuffer]);
        int l = (int)len;
        if (len >= INMEMORY_SEND_MINLENGTHOOB) {
            inm->virtualSize[inm->activeBuffer] += l;
            WSABUF temp;
            temp.len = (ULONG) l;
            temp.buf = (CHAR*)buf + inm->heapOffset;
            l = (int) (buffer - inm->buffer[inm->activeBuffer]);
            buffer = inm->buffer[inm->activeBuffer] - ++(inm->controlAlias[inm->activeBuffer]->countOfOOB) * sizeof(WSABUF);
            memcpy(buffer, &temp, sizeof(WSABUF));
        } else {
            inm->virtualSize[inm->activeBuffer] +=  len;
            inm->sizeFilled[inm->activeBuffer][0] += len;
            memcpy(buffer, buf, len);
        }
        return 1;
    }
    return 0;
}


static int PollForRead(redisInMemoryReplReceive * inm)
{
    server.unixtime = time(NULL);

    int timeout = server.repl_timeout - (server.unixtime - server.repl_transfer_lastio);
    if (timeout <= 0) {
        redisLog(REDIS_WARNING, "Error while reading: timeout");
        return 0;
    }

    aeProcessEvents(server.el, AE_FILE_EVENTS, timeout);

    if (server.repl_inMemoryReceive != inm) {
        redisLog(REDIS_WARNING, "Disconnected while reading");
        return 0;
    }
    if (inm->endStateFlags & INMEMORY_ENDSTATE_ERROR) {
        redisLog(REDIS_WARNING, "Error while reading: %d", inm->endStateFlags);
        return 0;
    }
    return 1;
}


static int ReadOOBItems(redisInMemoryReplReceive * inm)
{
    if (inm->posBufferRead != inm->posBufferWritten || inm->posBufferWritten != inm->sendControl->sizeOfThis) {
        redisLog(REDIS_WARNING, "Error while reading: Too many OOB items");
        return 0;
    }
    if (!(inm->endStateFlags & INMEMORY_ENDSTATE_ENDINLINE)) {
        redisLog(REDIS_WARNING, "Error while reading: long inline buffer");
        return 0;
    }
    if (inm->numOOBItems != inm->sendControl->countOfOOB) {
        redisLog(REDIS_WARNING, "Error while reading: OOB count mismatch");
        return 0;
    }
    for (int x = 0; x < inm->numOOBItems; x++) {
        WSABUF * wsa = inm->buffer + inm->bufferSize - sizeof(WSABUF)* x;
        inm->shortcutBuffer = wsa->buf;
        inm->shortCutBufferSize = wsa->len;
        while (inm->shortCutBufferSize) {
            if (!PollForRead(inm))
                return 0;
        }
    }
    inm->numOOBItems = 0;
    inm->posBufferWritten = 0;
    inm->endStateFlags &= ~INMEMORY_ENDSTATE_ENDINLINE;
    inm->posBufferRead = sizeof(redisInMemoryReplSendControl);
    return 1;
}

/* Returns 1 or 0 for success/failure. */
static size_t rioMemoryRead(rio *r, void *buf, size_t len) {
    ssize_t leftInActiveBuffer;
    size_t lenToCopy;
    redisInMemoryReplReceive * inm = r->io.memoryReceive.inMemory;
    while (server.repl_inMemoryReceive == inm) {
        BOOL handled = 0;
        if (inm->posBufferWritten >= sizeof(redisInMemoryReplSendControl)) {
            if (len >= INMEMORY_SEND_MINLENGTHOOB && inm->numOOBItems < inm->sendControl->countOfOOB) {
                WSABUF * wsa = inm->buffer + inm->bufferSize - sizeof(WSABUF)* inm->numOOBItems++;
                wsa->buf = buf;
                wsa->len = (ULONG)len;
                handled = 1;
            } else if (len >= INMEMORY_SEND_MINLENGTHOOB) {  // this one belongs to the next packet
                if (!ReadOOBItems(inm))
                    return 0;
                continue;
            } else { // Inline items
                leftInActiveBuffer = inm->posBufferWritten - inm->posBufferRead;
                if (len <= leftInActiveBuffer) {
                    memcpy(buf, inm->buffer + inm->posBufferRead, len);
                    inm->posBufferRead += len;
                    handled = 1;
                }
            }
            if (handled) {
                if (inm->posBufferRead == inm->posBufferWritten && inm->posBufferWritten == inm->sendControl->sizeOfThis && inm->sendControl->countOfOOB == inm->numOOBItems)
                    if (!ReadOOBItems(inm))
                        return 0;
                return 1;
            }
        }
        if (!PollForRead(inm)) {
            return 0;
        }
    }
    return 0;
}

/* Returns read/write position in file. */
static off_t rioMemoryTell(rio *r) {
    return r->io.memoryReceive.inMemory->totalRead;
}


static const rio rioBufferIO = {
    rioBufferRead,
    rioBufferWrite,
    rioBufferTell,
    NULL,           /* update_checksum */
    0,              /* current checksum */
    0,              /* bytes read or written */
    0,              /* read/write chunk size */
    { { NULL, 0 } } /* union for io-specific vars */
};

static const rio rioFileIO = {
    rioFileRead,
    rioFileWrite,
    rioFileTell,
    NULL,           /* update_checksum */
    0,              /* current checksum */
    0,              /* bytes read or written */
    0,              /* read/write chunk size */
    { { NULL, 0 } } /* union for io-specific vars */
};

static const rio rioMemoryReceiveIO = {
    rioMemoryRead,
    NULL,
    rioMemoryTell,
    NULL,           /* update_checksum */
    0,              /* current checksum */
    0,              /* bytes read or written */
    0,              /* read/write chunk size */
    { { NULL, 0 } } /* union for io-specific vars */
};

static const rio rioMemorySendIO = {
    NULL,
    rioMemoryWrite,
    NULL,
    NULL,           /* update_checksum */
    0,              /* current checksum */
    0,              /* bytes read or written */
    0,              /* read/write chunk size */
    { { NULL, 0 } } /* union for io-specific vars */
};


void rioInitWithFile(rio *r, FILE *fp) {
    *r = rioFileIO;
    r->io.file.fp = fp;
    r->io.file.buffered = 0;
    r->io.file.autosync = 0;
}

void rioInitWithBuffer(rio *r, sds s) {
    *r = rioBufferIO;
    r->io.buffer.ptr = s;
    r->io.buffer.pos = 0;
}

void rioInitWithMemoryReceive(rio *r, redisInMemoryReplReceive * inMemory) {
    *r = rioMemoryReceiveIO;
    r->io.memoryReceive.inMemory = inMemory;
}

void rioInitWithMemorySend(rio *r, redisInMemoryReplSend * inMemory) {
    *r = rioMemorySendIO;
    r->io.memorySend.sequence = 0;
    r->io.memorySend.inMemory = inMemory;
}


/* This function can be installed both in memory and file streams when checksum
 * computation is needed. */
void rioGenericUpdateChecksum(rio *r, const void *buf, size_t len) {
    r->cksum = crc64(r->cksum,buf,len);
}

/* Set the file-based rio object to auto-fsync every 'bytes' file written.
 * By default this is set to zero that means no automatic file sync is
 * performed.
 *
 * This feature is useful in a few contexts since when we rely on OS write
 * buffers sometimes the OS buffers way too much, resulting in too many
 * disk I/O concentrated in very little time. When we fsync in an explicit
 * way instead the I/O pressure is more distributed across time. */
void rioSetAutoSync(rio *r, off_t bytes) {
    redisAssert(r->read == rioFileIO.read);
    r->io.file.autosync = bytes;
}

/* ------------------------------ Higher level interface ---------------------------
 * The following higher level functions use lower level rio.c functions to help
 * generating the Redis protocol for the Append Only File. */

/* Write multi bulk count in the format: "*<count>\r\n". */
size_t rioWriteBulkCount(rio *r, char prefix, int count) {
    char cbuf[128];
    int clen;

    cbuf[0] = prefix;
    clen = 1+ll2string(cbuf+1,sizeof(cbuf)-1,count);
    cbuf[clen++] = '\r';
    cbuf[clen++] = '\n';
    if (rioWrite(r,cbuf,clen) == 0) return 0;
    return clen;
}

/* Write binary-safe string in the format: "$<count>\r\n<payload>\r\n". */
size_t rioWriteBulkString(rio *r, const char *buf, size_t len) {
    size_t nwritten;

    if ((nwritten = rioWriteBulkCount(r,'$',len)) == 0) return 0;
    if (len > 0 && rioWrite(r,buf,len) == 0) return 0;
    if (rioWrite(r,"\r\n",2) == 0) return 0;
    return nwritten+len+2;
}

/* Write a long long value in format: "$<count>\r\n<payload>\r\n". */
size_t rioWriteBulkLongLong(rio *r, long long l) {
    char lbuf[32];
    unsigned int llen;

    llen = ll2string(lbuf,sizeof(lbuf),l);
    return rioWriteBulkString(r,lbuf,llen);
}

/* Write a double value in the format: "$<count>\r\n<payload>\r\n" */
size_t rioWriteBulkDouble(rio *r, double d) {
    char dbuf[128];
    unsigned int dlen;

    dlen = snprintf(dbuf,sizeof(dbuf),"%.17g",d);
    return rioWriteBulkString(r,dbuf,dlen);
}
