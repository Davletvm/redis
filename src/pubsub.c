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

/*-----------------------------------------------------------------------------
 * Pubsub low level API
 *----------------------------------------------------------------------------*/

void freePubsubPattern(void *p) {
    pubsubPattern *pat = p;

    decrRefCount(pat->pattern);
    zfree(pat);
}

void freePubsubScript(void *p) {
    pubsubScript *pat = p;

    decrRefCount(pat->patternEvent);
    decrRefCount(pat->patternKey);
    decrRefCount(pat->script);
    decrRefCount(pat->scriptSha);
    zfree(pat);
}

void freePubsubQueuedScript(void *p) {
    pubsubQueuedScript *pat = p;

    decrRefCount(pat->event);
    decrRefCount(pat->key);
    zfree(pat);
}

int listMatchPubsubPattern(void *a, void *b) {
    pubsubPattern *pa = a, *pb = b;

    return (pa->client == pb->client) &&
           (equalStringObjects(pa->pattern,pb->pattern));
}

int listMatchPubsubScript(void *a, void *b) {
    pubsubScript *pa = a, *pb = b;

    return equalStringObjects(pa->patternKey, pb->patternKey) && equalStringObjects(pa->patternEvent, pb->patternEvent);
}

/* Return the number of channels + patterns a client is subscribed to. */
int clientSubscriptionsCount(redisClient *c) {
    return (int)dictSize(c->pubsub_channels)+
           listLength(c->pubsub_patterns);
}

/* Subscribe a client to a channel. Returns 1 if the operation succeeded, or
 * 0 if the client was already subscribed to that channel. */
int pubsubSubscribeChannel(redisClient *c, robj *channel) {
    struct dictEntry *de;
    list *clients = NULL;
    int retval = 0;

    /* Add the channel to the client -> channels hash table */
    if (dictAdd(c->pubsub_channels,channel,NULL) == DICT_OK) {
        retval = 1;
        incrRefCount(channel);
        /* Add the client to the channel -> list of clients hash table */
        de = dictFind(server.pubsub_channels,channel);
        if (de == NULL) {
            clients = listCreate();
            dictAdd(server.pubsub_channels,channel,clients);
            incrRefCount(channel);
        } else {
            clients = dictGetVal(de);
        }
        listAddNodeTail(clients,c);
    }
    /* Notify the client */
    addReply(c,shared.mbulkhdr[3]);
    addReply(c,shared.subscribebulk);
    addReplyBulk(c,channel);
    addReplyLongLong(c,clientSubscriptionsCount(c));
    return retval;
}

/* Unsubscribe a client from a channel. Returns 1 if the operation succeeded, or
 * 0 if the client was not subscribed to the specified channel. */
int pubsubUnsubscribeChannel(redisClient *c, robj *channel, int notify) {
    struct dictEntry *de;
    list *clients;
    listNode *ln;
    int retval = 0;

    /* Remove the channel from the client -> channels hash table */
    incrRefCount(channel); /* channel may be just a pointer to the same object
                            we have in the hash tables. Protect it... */
    if (dictDelete(c->pubsub_channels,channel) == DICT_OK) {
        retval = 1;
        /* Remove the client from the channel -> clients list hash table */
        de = dictFind(server.pubsub_channels,channel);
        redisAssertWithInfo(c,NULL,de != NULL);
        clients = dictGetVal(de);
        ln = listSearchKey(clients,c);
        redisAssertWithInfo(c,NULL,ln != NULL);
        listDelNode(clients,ln);
        if (listLength(clients) == 0) {
            /* Free the list and associated hash entry at all if this was
             * the latest client, so that it will be possible to abuse
             * Redis PUBSUB creating millions of channels. */
            dictDelete(server.pubsub_channels,channel);
        }
    }
    /* Notify the client */
    if (notify) {
        addReply(c,shared.mbulkhdr[3]);
        addReply(c,shared.unsubscribebulk);
        addReplyBulk(c,channel);
        addReplyLongLong(c,dictSize(c->pubsub_channels)+
                       listLength(c->pubsub_patterns));

    }
    decrRefCount(channel); /* it is finally safe to release it */
    return retval;
}

/* Subscribe a client to a pattern. Returns 1 if the operation succeeded, or 0 if the client was already subscribed to that pattern. */
int pubsubSubscribePattern(redisClient *c, robj *pattern) {
    int retval = 0;

    if (listSearchKey(c->pubsub_patterns,pattern) == NULL) {
        pubsubPattern *pat;
        retval = 1;
        listAddNodeTail(c->pubsub_patterns,pattern);
        incrRefCount(pattern);
        pat = zmalloc(sizeof(*pat));
        pat->pattern = getDecodedObject(pattern);
        pat->client = c;
        listAddNodeTail(server.pubsub_patterns,pat);
    }
    /* Notify the client */
    addReply(c,shared.mbulkhdr[3]);
    addReply(c,shared.psubscribebulk);
    addReplyBulk(c,pattern);
    addReplyLongLong(c,clientSubscriptionsCount(c));
    return retval;
}

/* Unsubscribe a client from a channel. Returns 1 if the operation succeeded, or
 * 0 if the client was not subscribed to the specified channel. */
int pubsubUnsubscribePattern(redisClient *c, robj *pattern, int notify) {
    listNode *ln;
    pubsubPattern pat;
    int retval = 0;

    incrRefCount(pattern); /* Protect the object. May be the same we remove */
    if ((ln = listSearchKey(c->pubsub_patterns,pattern)) != NULL) {
        retval = 1;
        listDelNode(c->pubsub_patterns,ln);
        pat.client = c;
        pat.pattern = pattern;
        ln = listSearchKey(server.pubsub_patterns,&pat);
        listDelNode(server.pubsub_patterns,ln);
    }
    /* Notify the client */
    if (notify) {
        addReply(c,shared.mbulkhdr[3]);
        addReply(c,shared.punsubscribebulk);
        addReplyBulk(c,pattern);
        addReplyLongLong(c,dictSize(c->pubsub_channels)+
                       listLength(c->pubsub_patterns));
    }
    decrRefCount(pattern);
    return retval;
}

/* Unsubscribe from all the channels. Return the number of channels the
 * client was subscribed to. */
int pubsubUnsubscribeAllChannels(redisClient *c, int notify) {
    dictIterator *di = dictGetSafeIterator(c->pubsub_channels);
    dictEntry *de;
    int count = 0;

    while((de = dictNext(di)) != NULL) {
        robj *channel = dictGetKey(de);

        count += pubsubUnsubscribeChannel(c,channel,notify);
    }
    /* We were subscribed to nothing? Still reply to the client. */
    if (notify && count == 0) {
        addReply(c,shared.mbulkhdr[3]);
        addReply(c,shared.unsubscribebulk);
        addReply(c,shared.nullbulk);
        addReplyLongLong(c,dictSize(c->pubsub_channels)+
                       listLength(c->pubsub_patterns));
    }
    dictReleaseIterator(di);
    return count;
}

/* Unsubscribe from all the patterns. Return the number of patterns the
 * client was subscribed from. */
int pubsubUnsubscribeAllPatterns(redisClient *c, int notify) {
    listNode *ln;
    listIter li;
    int count = 0;

    listRewind(c->pubsub_patterns,&li);
    while ((ln = listNext(&li)) != NULL) {
        robj *pattern = ln->value;

        count += pubsubUnsubscribePattern(c,pattern,notify);
    }
    if (notify && count == 0) {
        /* We were subscribed to nothing? Still reply to the client. */
        addReply(c,shared.mbulkhdr[3]);
        addReply(c,shared.punsubscribebulk);
        addReply(c,shared.nullbulk);
        addReplyLongLong(c,dictSize(c->pubsub_channels)+
                       listLength(c->pubsub_patterns));
    }
    return count;
}

/* Publish a message */
int pubsubPublishMessage(robj *channel, robj *message) {
    int receivers = 0;
    struct dictEntry *de;
    listNode *ln;
    listIter li;

    /* Send to clients listening for that channel */
    de = dictFind(server.pubsub_channels,channel);
    if (de) {
        list *list = dictGetVal(de);
        listNode *ln;
        listIter li;

        listRewind(list,&li);
        while ((ln = listNext(&li)) != NULL) {
            redisClient *c = ln->value;

            addReply(c,shared.mbulkhdr[3]);
            addReply(c,shared.messagebulk);
            addReplyBulk(c,channel);
            addReplyBulk(c,message);
            receivers++;
        }
    }
    /* Send to clients listening to matching channels */
    if (listLength(server.pubsub_patterns)) {
        listRewind(server.pubsub_patterns,&li);
        channel = getDecodedObject(channel);
        while ((ln = listNext(&li)) != NULL) {
            pubsubPattern *pat = ln->value;

            if (stringmatchlen((char*)pat->pattern->ptr,
                                (int)sdslen(pat->pattern->ptr),
                                (char*)channel->ptr,
                                (int)sdslen(channel->ptr),0)) {
                addReply(pat->client,shared.mbulkhdr[4]);
                addReply(pat->client,shared.pmessagebulk);
                addReplyBulk(pat->client,pat->pattern);
                addReplyBulk(pat->client,channel);
                addReplyBulk(pat->client,message);
                receivers++;
            }
        }
        decrRefCount(channel);
    }
    return receivers;
}

/*-----------------------------------------------------------------------------
 * Pubsub commands implementation
 *----------------------------------------------------------------------------*/

void subscribeCommand(redisClient *c) {
    int j;

    for (j = 1; j < c->argc; j++)
        pubsubSubscribeChannel(c,c->argv[j]);
    c->flags |= REDIS_PUBSUB;
}

void unsubscribeCommand(redisClient *c) {
    if (c->argc == 1) {
        pubsubUnsubscribeAllChannels(c,1);
    } else {
        int j;

        for (j = 1; j < c->argc; j++)
            pubsubUnsubscribeChannel(c,c->argv[j],1);
    }
    if (clientSubscriptionsCount(c) == 0) c->flags &= ~REDIS_PUBSUB;
}

void psubscribeCommand(redisClient *c) {
    int j;

    for (j = 1; j < c->argc; j++)
        pubsubSubscribePattern(c,c->argv[j]);
    c->flags |= REDIS_PUBSUB;
}

void punsubscribeCommand(redisClient *c) {
    if (c->argc == 1) {
        pubsubUnsubscribeAllPatterns(c,1);
    } else {
        int j;

        for (j = 1; j < c->argc; j++)
            pubsubUnsubscribePattern(c,c->argv[j],1);
    }
    if (clientSubscriptionsCount(c) == 0) c->flags &= ~REDIS_PUBSUB;
}

void publishCommand(redisClient *c) {
    int receivers = pubsubPublishMessage(c->argv[1],c->argv[2]);
    forceCommandPropagation(c,REDIS_PROPAGATE_REPL);
    addReplyLongLong(c,receivers);
}

/* PUBSUB command for Pub/Sub introspection. */
void pubsubCommand(redisClient *c) {
    if (!strcasecmp(c->argv[1]->ptr,"channels") &&
        (c->argc == 2 || c->argc ==3))
    {
        /* PUBSUB CHANNELS [<pattern>] */
        sds pat = (c->argc == 2) ? NULL : c->argv[2]->ptr;
        dictIterator *di = dictGetIterator(server.pubsub_channels);
        dictEntry *de;
        long mblen = 0;
        void *replylen;

        replylen = addDeferredMultiBulkLength(c);
        while((de = dictNext(di)) != NULL) {
            robj *cobj = dictGetKey(de);
            sds channel = cobj->ptr;

            if (!pat || stringmatchlen(pat, (int)sdslen(pat),
                                       channel, (int)sdslen(channel),0))
            {
                addReplyBulk(c,cobj);
                mblen++;
            }
        }
        dictReleaseIterator(di);
        setDeferredMultiBulkLength(c,replylen,mblen);
    } else if (!strcasecmp(c->argv[1]->ptr,"numsub") && c->argc >= 2) {
        /* PUBSUB NUMSUB [Channel_1 ... Channel_N] */
        int j;

        addReplyMultiBulkLen(c,(c->argc-2)*2);
        for (j = 2; j < c->argc; j++) {
            list *l = dictFetchValue(server.pubsub_channels,c->argv[j]);

            addReplyBulk(c,c->argv[j]);
            addReplyLongLong(c,l ? listLength(l) : 0);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"numpat") && c->argc == 2) {
        /* PUBSUB NUMPAT */
        addReplyLongLong(c,listLength(server.pubsub_patterns));
    } else {
        addReplyErrorFormat(c,
            "Unknown PUBSUB subcommand or wrong number of arguments for '%s'",
            (char*)c->argv[1]->ptr);
    }
}

pubsubScript* addKeyspaceScript(robj* event, robj* key, robj* script, robj* sha)
{
    pubsubScript* scr = zmalloc(sizeof(pubsubScript));
    scr->patternEvent = getDecodedObject(event);
    scr->patternKey = getDecodedObject(key);
    scr->script = getDecodedObject(script);
    scr->scriptSha = sha;

    listAddNodeTail(server.pubsub_scripts, scr);

    return scr;
}


void setkeyspacescriptNewScript(redisClient *c)
{
    robj * patternEvent = c->argv[1];
    robj * patternKey = c->argv[2];
    robj * script = c->argv[3];

    pubsubScript pub;
    pub.patternEvent = patternEvent;
    pub.patternKey = patternKey;
    pub.script = script;
    pub.scriptSha = NULL;

    if (sdslen(script->ptr) != 0 && !onNewEventScript(c, &pub, NULL))
    {
        return;
    }

    int retval = 0;
    listNode * ln = listSearchKey(server.pubsub_scripts, &pub);
    if (ln != NULL) {
        listDelNode(server.pubsub_scripts, ln);
        retval = 1;
        server.dirty++;
    }
    if (sdslen(script->ptr) != 0) {
        addKeyspaceScript(patternEvent, patternKey, script, pub.scriptSha);
        server.dirty++;
    }
    addReplyLongLong(c, retval);
}


void setkeyspacescriptListScripts(redisClient * c, list * scripts)
{
    listNode *ln;
    listIter li;

    addReplyMultiBulkLen(c, listLength(scripts) * 3);
    if (listLength(scripts)) {
        listRewind(scripts, &li);
        while ((ln = listNext(&li)) != NULL) {
            pubsubScript *pat = ln->value;
            addReplyBulkCString(c, pat->patternEvent->ptr);
            addReplyBulkCString(c, pat->patternKey->ptr);
            addReplyBulkCString(c, pat->script->ptr);
        }
    }
}

void getMatchingScripts(robj *patternEvent, robj* patternKey, list ** matches)
{
    listNode *ln;
    listIter li;

    if (listLength(server.pubsub_scripts)) {
        listRewind(server.pubsub_scripts, &li);
        while ((ln = listNext(&li)) != NULL) {
            pubsubScript *pat = ln->value;

            if (stringmatchlen((char*)pat->patternEvent->ptr,
                (int)sdslen(pat->patternEvent->ptr),
                (char*)patternEvent->ptr,
                (int)sdslen(patternEvent->ptr), 0) &&
                stringmatchlen((char*)pat->patternKey->ptr,
                (int)sdslen(pat->patternKey->ptr),
                (char*)patternKey->ptr,
                (int)sdslen(patternKey->ptr), 0)) {
                if (*matches == NULL) *matches = listCreate();
                listAddNodeTail(*matches, pat);
            }
        }
    }
}



void setkeyspacescriptListMatchingScripts(redisClient *c)
{
    list * answers = listCreate();

    robj * patternEvent = c->argv[1];
    robj * patternKey = c->argv[2];

    getMatchingScripts(patternEvent, patternKey, &answers);

    setkeyspacescriptListScripts(c, answers);
    listRelease(answers);
}

void setkeyspacescriptClear(redisClient *c)
{
    int len = listLength(server.pubsub_scripts);
    listClear(server.pubsub_scripts);
    addReplyLongLong(c, len);
}


void setkeyspacescriptCommand(redisClient *c)
{
    if (server.lua_inKeyspaceScript) {
        addReplyErrorFormat(c,
            "SETKSSCRIPT command not allowed inside of event script");
        return;
    }

    if (c->argc == 4) {
        setkeyspacescriptNewScript(c);
    } else if (c->argc == 3) {
        setkeyspacescriptListMatchingScripts(c);
    } else if (c->argc == 2) {
        if (!strcasecmp(c->argv[1]->ptr, "clear")) {
            setkeyspacescriptClear(c);
        } else if (!strcasecmp(c->argv[1]->ptr, "list")) {
            setkeyspacescriptListScripts(c, server.pubsub_scripts);
        } else {
            addReplyErrorFormat(c,
                "Invalid SETKSSCRIPT command or wrong number of arguments for '%s'",
                (char*)c->argv[1]->ptr);
        }
    } else if (c->argc == 1) {
        addReplyLongLong(c, listLength(server.pubsub_scripts));
    } else {
        addReplyErrorFormat(c,
            "Invalid SETKSSCRIPT command or wrong number of arguments");
    }
}

void queueEventScripts(int dbid, robj *channel1, robj* event, robj* channel2, robj* key)
{
    listNode *ln;
    listIter li;

    if (listLength(server.pubsub_scripts)) {


        channel1 = getDecodedObject(channel1);
        event = getDecodedObject(event);
        channel2 = getDecodedObject(channel2);
        key = getDecodedObject(key);

        list * scripts = NULL;
        getMatchingScripts(channel1, channel2, &scripts);

        if (scripts) {
            listRewind(scripts, &li);
            while ((ln = listNext(&li)) != NULL) {
                pubsubScript *pat = ln->value;
                queueEventScript(dbid, event, key, pat);

            }
            listRelease(scripts);
        }        
        decrRefCount(channel1);
        decrRefCount(channel2);
        decrRefCount(event);
        decrRefCount(key);
    }
}


void runQueuedEventScripts()
{

    listNode *ln;

    if (!server.pubsub_script_queue || server.lua_inKeyspaceScript || server.lua_caller || !listLength(server.pubsub_script_queue)) return;

    if (!server.propagated_multi_for_queued_script) {
        propagateMultiOrExec(-1, 1, REDIS_PROPAGATE_AOF | REDIS_PROPAGATE_REPL);
    }

    for (ln = listFirst(server.pubsub_script_queue); ln; ln = listNextNode(ln)) {

        pubsubQueuedScript *pat = ln->value;
        fireEventScript(pat->dbid, pat->event, pat->key, pat->script);

        if (listLength(server.pubsub_script_queue) > server.lua_event_limit) {
            redisLog(REDIS_WARNING, "Limit of queued script events exceeded.  Stopping event scripts for duration of session.");
            server.notify_keyspace_scripts = 0;
            listClear(server.pubsub_script_queue);
            break;
        }
    }
    listClear(server.pubsub_script_queue);

    if (server.propagated_multi_for_queued_script) {
        propagateMultiOrExec(-1, 0, REDIS_PROPAGATE_AOF | REDIS_PROPAGATE_REPL);
    }


}

