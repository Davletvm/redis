/*
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

#include "redis.h"
#include "zipmap.h"
#include "endianconv.h"

#include <math.h>
#include <sys/types.h>
#ifndef _WIN32
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <arpa/inet.h>
#else
#include <stdio.h>
#endif
#include <sys/stat.h>


BOOL checkPtr(HeapExtent * ex, void * ptr) {
    if (ptr > ex->heapStart && ptr < ex->heapEnd)
        return TRUE;
    else
        return FALSE;
}

BOOL checkSDS(HeapExtent * ex, sds s)
{
    size_t len = sdslen(s);
    size_t avail = sdsavail(s);
    return checkPtr(ex, s) && checkPtr(ex, s + len + avail);
}


BOOL checkStringObject(HeapExtent * ex, robj *obj) {
    if (!checkPtr(ex, obj)) return FALSE;
    if (obj->encoding == REDIS_ENCODING_INT) {
        return TRUE;
    } else {
        if (obj->encoding != REDIS_ENCODING_RAW) 
            return FALSE;
        return checkSDS(ex, obj->ptr);
    }
}

BOOL checkObj(HeapExtent * ex, robj *o) {
    dictIterator *di = NULL;

    if (!checkPtr(ex, o)) goto werr;

    if (o->type == REDIS_STRING) {
        if (!checkStringObject(ex, o)) goto werr;
    } else if (o->type == REDIS_LIST) {
        if (o->encoding == REDIS_ENCODING_ZIPLIST) {
            size_t l = ziplistBlobLen((unsigned char*)o->ptr);
            if (!checkPtr(ex, o->ptr) || !checkPtr(ex, (char*)o->ptr + l)) goto werr;
        } else if (o->encoding == REDIS_ENCODING_LINKEDLIST) {
            list *list = o->ptr;
            listNode *ln;
            listIter li;

            listRewind(list, &li);
            while ((ln = listNext(&li))) {
                robj *eleobj = listNodeValue(ln);
                if (!checkStringObject(ex, eleobj)) goto werr;
            }
        } else {
            goto werr;
        }
    } else if (o->type == REDIS_SET) {
        if (o->encoding == REDIS_ENCODING_HT) {
            dict *set = o->ptr;
            dictEntry *de;
            di = dictGetIterator(set);

            while ((de = dictNext(di)) != NULL) {
                robj *eleobj;
                if (!checkPtr(ex, de)) goto werr;

                eleobj = dictGetKey(de);
                if (!checkStringObject(ex, eleobj)) goto werr;
            }
            dictReleaseIterator(di);
            di = NULL;
        } else if (o->encoding == REDIS_ENCODING_INTSET) {
            size_t l = intsetBlobLen((intset*)o->ptr);

            if (!checkPtr(ex, o->ptr) || !checkPtr(ex, (char*)o->ptr + l)) goto werr;
        } else {
            goto werr;
        }
    } else if (o->type == REDIS_ZSET) {
        if (o->encoding == REDIS_ENCODING_ZIPLIST) {
            size_t l = ziplistBlobLen((unsigned char*)o->ptr);

            if (!checkPtr(ex, o->ptr) || !checkPtr(ex, (char*)o->ptr + l)) goto werr;
        } else if (o->encoding == REDIS_ENCODING_SKIPLIST) {
            zset *zs = o->ptr;
            dictEntry *de;
            di = dictGetIterator(zs->dict);

            while ((de = dictNext(di)) != NULL) {
                robj *eleobj;
                double *score;

                if (!checkPtr(ex, de)) goto werr;

                eleobj = dictGetKey(de);
                score = dictGetVal(de);

                if (!checkStringObject(ex, eleobj)) goto werr;
                if (!checkPtr(ex, score)) goto werr;
            }
            dictReleaseIterator(di);
            di = NULL;
        } else {
            goto werr;
        }
    } else if (o->type == REDIS_HASH) {
        if (o->encoding == REDIS_ENCODING_ZIPLIST) {
            size_t l = ziplistBlobLen((unsigned char*)o->ptr);

            if (!checkPtr(ex, o->ptr) || !checkPtr(ex, (char*)o->ptr + l)) goto werr;

        } else if (o->encoding == REDIS_ENCODING_HT) {
            di = dictGetIterator(o->ptr);
            dictEntry *de;

            while ((de = dictNext(di)) != NULL) {
                robj *key;
                robj *val;

                if (!checkPtr(ex, de)) goto werr;

                key = dictGetKey(de);
                val = dictGetVal(de);

                if (!checkStringObject(ex, key)) goto werr;
                if (!checkStringObject(ex, val)) goto werr;

            }
            dictReleaseIterator(di);
            di = NULL;
        } else {
            goto werr;
        }

    } else {
        goto werr;
    }
    return TRUE;

werr:
    if (di) dictReleaseIterator(di);
    return FALSE;
}


int dbCheck() {
    dictIterator *di = NULL;
    dictEntry *de;
    int j;
    HeapExtent extent;

    GetHeapExtent(&extent);

    for (j = 0; j < server.dbnum; j++) {
        redisDb *db = server.db + j;
        dict *d = db->dict;
        if (dictSize(d) == 0) continue;
        di = dictGetSafeIterator(d);
        if (!di) {
            return REDIS_ERR;
        }

        while ((de = dictNext(di)) != NULL) {
            sds keystr = dictGetKey(de);
            robj key, *o;
            if (!checkPtr(&extent, keystr)) goto werr;
            if (!checkSDS(&extent, keystr)) goto werr;
            o = dictGetVal(de);

            initStaticStringObject(key, keystr);
            de = dictFind(db->expires, key.ptr);
            if (de && !checkPtr(&extent, de)) goto werr;

            if (!checkObj(&extent, o)) goto werr;
        }
        dictReleaseIterator(di);
    }
    di = NULL; /* So that we don't release it again on error. */

    return REDIS_OK;

werr:

    if (di) dictReleaseIterator(di);
    return REDIS_ERR;
}

int dbcheckBackground() {
    pid_t childpid;
    long long start;

    if (server.rdb_child_pid != -1) return REDIS_ERR;

    start = ustime();
    {
        if (!BeginForkOperation(otDBCHECK, NULL, 0, &server, sizeof(server), &childpid, dictGetHashFunctionSeed())) {
            childpid = -1;
        }
        /* Parent */
        server.stat_fork_time = ustime() - start;
        if (childpid == -1) {
            return REDIS_ERR;
        }
        redisLog(REDIS_NOTICE, "Background dbcheck started by pid %d", childpid);
        server.rdb_child_pid = childpid;
        updateDictResizePolicy();
        return REDIS_OK;
    }
    return REDIS_OK; /* unreached */
}

void dbcheckCommand(redisClient *c) {
    if (server.privilidgeEnabled && !(c->flags & REDIS_PRIVILIDGED_CLIENT)) {
        addReplyError(c, "Privilige required");
        return;
    }
    if (dbCheck() == REDIS_OK) {
        addReply(c, shared.ok);
    } else {
        addReply(c, shared.err);
    }
}

void bgdbcheckCommand(redisClient *c) {
    if (server.rdb_child_pid != -1) {
        addReplyError(c, "Background save in progress");
    } else if (server.aof_child_pid != -1) {
        addReplyError(c, "AOF log rewriting is in progress");
    } else if (dbcheckBackground() == REDIS_OK) {
        addReplyStatus(c, "Background dbcheck started");
    } else {
        addReply(c, shared.err);
    }
}
