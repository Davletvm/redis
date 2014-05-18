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

static void PollForRead()
{
    aeProcessEvents(server.el, AE_FILE_EVENTS);
}

static void SendActiveBuffer(rio * r)
{
    ResetEvent(r->io.memory.inMemory->master.sentDoneEvents[r->io.memory.inMemory->master.activeBuffer]);
    SetEvent(r->io.memory.inMemory->master.doSendEvents[r->io.memory.inMemory->master.activeBuffer]);
}

static int WaitForFreeBuffer(rio * r)
{
    redisInMemoryRepl * inm = r->io.memory.inMemory;
    WaitForMultipleObjects(2, inm->master.sentDoneEvents, FALSE, INFINITE);
    DWORD rval = WaitForSingleObject(inm->master.sentDoneEvents[0], 0);
    if (rval == WAIT_OBJECT_0) {
        ResetEvent(inm->master.sentDoneEvents[0]);
        inm->master.sizeFilled[0] = 0;
    } else if (rval != WAIT_TIMEOUT) {
        return 0;
    }
    rval = WaitForSingleObject(inm->master.sentDoneEvents[1], 0);
    if (rval == WAIT_OBJECT_0) {
        ResetEvent(inm->master.sentDoneEvents[1]);
        inm->master.sizeFilled[1] = 0;
    } else if (rval != WAIT_TIMEOUT) {
        return 0;
    }
    return !inm->master.sizeFilled[0] || !inm->master.sizeFilled[1];
}

/* Returns 1 or 0 for success/failure.
    This is only called by the forked child
    It writes to all r members.
    The parent only reads the r members, and only
    when signaled to do so.
*/
static size_t rioMemoryWrite(rio *r, const void *buf, size_t len) {
    ssize_t leftInActiveBuffer;
    size_t lenToCopy;
    redisInMemoryRepl * inm = r->io.memory.inMemory;
    while (server.repl_inMemory && !inm->abortRequested) {
        leftInActiveBuffer = inm->bufferSize - inm->master.sizeFilled[inm->master.activeBuffer];
        if (leftInActiveBuffer == 0) {
            server.repl_inMemory->master.activeBuffer++;
            if (server.repl_inMemory->master.activeBuffer == 2) server.repl_inMemory->master.activeBuffer = 0;
        }
        leftInActiveBuffer = inm->bufferSize - inm->master.sizeFilled[inm->master.activeBuffer];
        if (leftInActiveBuffer == 0) {
            if (!WaitForFreeBuffer(r))
                return 0;
            continue;
        }
        if (len > leftInActiveBuffer)
            lenToCopy = leftInActiveBuffer;
        else
            lenToCopy = len;
        memcpy(inm->buffer[inm->master.activeBuffer] + inm->master.sizeFilled[inm->master.activeBuffer], buf, lenToCopy);
        len -= lenToCopy;
        leftInActiveBuffer -= lenToCopy;
        inm->master.sizeFilled[inm->master.activeBuffer] += lenToCopy;
        buf = ((char*)buf) + lenToCopy;
        if (leftInActiveBuffer == 0)
            SendActiveBuffer(r);
        if (len == 0) return 1;
    }
    return 0;
}



/* Returns 1 or 0 for success/failure. */
static size_t rioMemoryRead(rio *r, void *buf, size_t len) {
    ssize_t leftInActiveBuffer;
    size_t lenToCopy;
    redisInMemoryRepl * inm = r->io.memory.inMemory;
    while (server.repl_inMemory && !inm->abortRequested) {
        leftInActiveBuffer = inm->slave.posBufferWritten[inm->slave.activeBufferRead] - inm->slave.posBufferRead[inm->slave.activeBufferRead];
        if (leftInActiveBuffer == 0) {
            server.repl_inMemory->slave.activeBufferRead++;
            if (server.repl_inMemory->slave.activeBufferRead == 2) server.repl_inMemory->slave.activeBufferRead = 0;
        }
        leftInActiveBuffer = inm->slave.posBufferWritten[inm->slave.activeBufferRead] - inm->slave.posBufferRead[inm->slave.activeBufferRead];
        if (leftInActiveBuffer > 0) {
            if (len > leftInActiveBuffer)
                lenToCopy = leftInActiveBuffer;
            else
                lenToCopy = len;
            memcpy(buf, inm->buffer[inm->slave.activeBufferRead], lenToCopy);
            inm->slave.posBufferRead[inm->slave.activeBufferRead] += lenToCopy;
            buf = ((char*)buf) + lenToCopy;
            len -= lenToCopy;
            leftInActiveBuffer -= lenToCopy;
            if (leftInActiveBuffer == 0) {
                inm->slave.posBufferRead[inm->slave.activeBufferRead] = inm->slave.posBufferWritten[inm->slave.activeBufferRead] = 0;
            }
            if (len == 0) return 1;
        }
        redisAssert(inm->slave.posBufferWritten[inm->slave.activeBufferRead] == inm->slave.posBufferRead[inm->slave.activeBufferRead]);
        if (len > inm->bufferSize) {
            inm->slave.shortcutBuffer = buf;
            inm->slave.shortcutBufferSize = len;
        }
        PollForRead();
        if (!server.repl_inMemory) return 0;
        if (inm->slave.shortcutBuffer && inm->slave.shortcutBufferSize == 0) {
            inm->slave.shortcutBuffer = NULL;
            return 1;
        }
    }
    return 0;
}

/* Returns read/write position in file. */
static off_t rioMemoryTell(rio *r) {
    return r->io.memory.inMemory->totalRead;
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

static const rio rioMemoryIO = {
    rioMemoryRead,
    rioMemoryWrite,
    rioMemoryTell,
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

void rioInitWithMemory(rio *r, redisInMemoryRepl * inMemory) {
    *r = rioMemoryIO;
    r->io.memory.inMemory = inMemory;
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
