/* Redis benchmark utility.
 *
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

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <sys/time.h>
#include <signal.h>
#include <assert.h>
//#include <stdatomic.h>

#include "ae.h"
#include "hiredis.h"
#include "sds.h"
#include "adlist.h"
#include "zmalloc.h"
#include "rdma-net.h"

#define REDIS_NOTUSED(V) ((void) V)
#define RANDPTR_INITIAL_SIZE 8
extern struct config_t config_rdma;
struct resources res;
int nn = 0;
pthread_mutex_t mutexsum;
char *data, *cmd;

static struct config {
    aeEventLoop *el;
    const char *hostip;
    int hostport;
    const char *hostsocket;
    int numclients;
    int liveclients;
    int requests;
    int requests_issued;
    int requests_finished;
    int keysize;
    int datasize;
    int randomkeys;
    int randomkeys_keyspacelen;
    int keepalive;
    int pipeline;
    long long start[WORKER_NUM];
    long long perlatency[WORKER_NUM];
    long long throughput[WORKER_NUM];
    long long totlatency;
    long long *latency;
    const char *title;
    list *clients;
    int quiet;
    int csv;
    int loop;
    int idlemode;
    int dbnum;
    sds dbnumstr;
    char *tests;
    char *auth;

} config;

typedef struct _client {
    redisContext *context;
    void * req;
    sds obuf;
    char **randptr;         /* Pointers to :rand: strings inside the command buf */
    size_t randlen;         /* Number of pointers in client->randptr */
    size_t randfree;        /* Number of unused pointers in client->randptr */
    unsigned int written;   /* Bytes of 'obuf' already written */
    long long start;        /* Start time of a request */
    long long  latency;      /* Request latency */
    int pending;            /* Number of pending requests (replies to consume) */
    int prefix_pending;     /* If non-zero, number of pending prefix commands. Commands
                               such as auth and select are prefixed to the pipeline of
                               benchmark commands and discarded after the first send. */
    int prefixlen;          /* Size in bytes of the pending prefix commands */
} *client;


struct thread_data{
    client c;
    int task_id;
    int requests;
    int finished_requests;
    int latency;
};
/* Prototypes */
static void writeHandler(aeEventLoop *el, int fd, void *privdata, int mask);
static void createMissingClients(client c);

/* Implementation */
static long long ustime(void) {
    struct timeval tv;
    long long ust;

    gettimeofday(&tv, NULL);
    ust = ((long)tv.tv_sec)*1000000;
    ust += tv.tv_usec;
    return ust;
}

static long long mstime(void) {
    struct timeval tv;
    long long mst;

    gettimeofday(&tv, NULL);
    mst = ((long long)tv.tv_sec)*1000;
    mst += tv.tv_usec/1000;
    return mst;
}

static void freeClient(client c) {
    listNode *ln;
    aeDeleteFileEvent(config.el,c->context->fd,AE_WRITABLE);
    aeDeleteFileEvent(config.el,c->context->fd,AE_READABLE);
    redisFree(c->context);
    sdsfree(c->obuf);
    zfree(c->randptr);
    zfree(c);
    config.liveclients--;
    ln = listSearchKey(config.clients,c);
    assert(ln != NULL);
    listDelNode(config.clients,ln);
}

static void freeAllClients(void) {
    listNode *ln = config.clients->head, *next;

    while(ln) {
        next = ln->next;
        freeClient(ln->value);
        ln = next;
    }
}

static void resetClient(client c) {
    aeDeleteFileEvent(config.el,c->context->fd,AE_WRITABLE);
    aeDeleteFileEvent(config.el,c->context->fd,AE_READABLE);
    //    aeCreateFileEvent(config.el,c->context->fd,AE_WRITABLE,writeHandler,c);
    c->written = 0;
    c->pending = config.pipeline;
}

static void randomizeClientKey(client c) {
    size_t i;

    for (i = 0; i < c->randlen; i++) {
        char *p = c->randptr[i]+11;
        size_t r = random() % config.randomkeys_keyspacelen;
        size_t j;

        for (j = 0; j < 12; j++) {
            *p = '0'+r%10;
            r/=10;
            p--;
        }
    }
//    int len = redisFormatCommand(&cmd,"SET|key%d|1",random());


}

static void clientDone(client c) {
    if (config.requests_finished == config.requests) {
        freeClient(c);
        aeStop(config.el);
        return;
    }
    if (config.keepalive) {
        resetClient(c);
    } else {
        config.liveclients--;
        createMissingClients(c);
        config.liveclients++;
        freeClient(c);
    }
}

static void readHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    client c = privdata;
    void *reply = NULL;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(fd);
    REDIS_NOTUSED(mask);

    /* Calculate latency only for the first read event. This means that the
     * server already sent the reply and we need to parse it. Parsing overhead
     * is not part of the latency, so calculate it only once, here. */
    if (c->latency < 0) c->latency = ustime()-(c->start);

    if (redisBufferRead(c->context) != REDIS_OK) {
        fprintf(stderr,"Error: %s\n",c->context->errstr);
        exit(1);
    } else {
        while(c->pending) {
            if (redisGetReply(c->context,&reply) != REDIS_OK) {
                fprintf(stderr,"Error: %s\n",c->context->errstr);
                exit(1);
            }
            if (reply != NULL) {
                if (reply == (void*)REDIS_REPLY_ERROR) {
                    fprintf(stderr,"Unexpected error reply, exiting...\n");
                    exit(1);
                }

                freeReplyObject(reply);
                /* This is an OK for prefix commands such as auth and select.*/
                if (c->prefix_pending > 0) {
                    c->prefix_pending--;
                    c->pending--;
                    /* Discard prefix commands on first response.*/
                    if (c->prefixlen > 0) {
                        size_t j;
                        sdsrange(c->obuf, c->prefixlen, -1);
                        /* We also need to fix the pointers to the strings
                        * we need to randomize. */
                        for (j = 0; j < c->randlen; j++)
                            c->randptr[j] -= c->prefixlen;
                        c->prefixlen = 0;
                    }
                    continue;
                }

                if (config.requests_finished < config.requests)
                    config.latency[config.requests_finished++] = c->latency;
                c->pending--;
                if (c->pending == 0) {
                    clientDone(c);
                    break;
                }
            } else {
                break;
            }
        }
    }
}

static void writeHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    client c = privdata;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(fd);
    REDIS_NOTUSED(mask);

    /* Initialize request when nothing was written. */
    if (c->written == 0) {
        /* Enforce upper bound to number of requests. */
        if (config.requests_issued++ >= config.requests) {
            freeClient(c);
            return;
        }

        /* Really initialize: randomize keys and set start time. */
        if (config.randomkeys) randomizeClientKey(c);
        c->start = ustime();
        c->latency = -1;
    }

    if (sdslen(c->obuf) > c->written) {
        void *ptr = c->obuf+c->written;
        int nwritten = write(c->context->fd,ptr,sdslen(c->obuf)-c->written);
        if (nwritten == -1) {
            if (errno != EPIPE)
                fprintf(stderr, "Writing to socket: %s\n", strerror(errno));
            freeClient(c);
            return;
        }
        c->written += nwritten;
        if (sdslen(c->obuf) == c->written) {
            aeDeleteFileEvent(config.el,c->context->fd,AE_WRITABLE);
            //            aeCreateFileEvent(config.el,c->context->fd,AE_READABLE,readHandler,c);

        }
    }
}

/* Create a benchmark client, configured to send the command passed as 'cmd' of
 * 'len' bytes.
 *
 * The command is copied N times in the client output buffer (that is reused
 * again and again to send the request to the server) accordingly to the configured
 * pipeline size.
 *
 * Also an initial SELECT command is prepended in order to make sure the right
 * database is selected, if needed. The initial SELECT will be discarded as soon
 * as the first reply is received.
 *
 * To create a client from scratch, the 'from' pointer is set to NULL. If instead
 * we want to create a client using another client as reference, the 'from' pointer
 * points to the client to use as reference. In such a case the following
 * information is take from the 'from' client:
 *
 * 1) The command line to use.
 * 2) The offsets of the __rand_int__ elements inside the command line, used
 *    for arguments randomization.
 *
 * Even when cloning another client, prefix commands are applied if needed.*/
static client createClient(char *cmd, size_t len, client from) {
    int j;
    client c = zmalloc(sizeof(struct _client));

    //    c->context = redisContextInit();

    if (config.hostsocket == NULL) {

        c->context = redisConnectNonBlock(config.hostip,config.hostport);
    } else {
        c->context = redisConnectUnixNonBlock(config.hostsocket);
    }
    if (c->context->err) {
        fprintf(stderr,"Could not connect to Redis at ");
        if (config.hostsocket == NULL)
            fprintf(stderr,"%s:%d: %s\n",config.hostip,config.hostport,c->context->errstr);
        else
            fprintf(stderr,"%s: %s\n",config.hostsocket,c->context->errstr);
        exit(1);
    }
    /* Suppress hiredis cleanup of unused buffers for max speed. */
    c->context->reader->maxbuf = 0;

    /* Build the request buffer:
     * Queue N requests accordingly to the pipeline size, or simply clone
     * the example client buffer. */
    c->obuf = sdsempty();
    /* Prefix the request buffer with AUTH and/or SELECT commands, if applicable.
     * These commands are discarded after the first response, so if the client is
     * reused the commands will not be used again. */
    c->prefix_pending = 0;
    if (config.auth) {
        char *buf = NULL;
        int len = redisFormatCommand(&buf, "AUTH %s", config.auth);
        c->obuf = sdscatlen(c->obuf, buf, len);
        free(buf);
        c->prefix_pending++;
    }

    /* If a DB number different than zero is selected, prefix our request
     * buffer with the SELECT command, that will be discarded the first
     * time the replies are received, so if the client is reused the
     * SELECT command will not be used again. */
    if (config.dbnum != 0) {
        c->obuf = sdscatprintf(c->obuf,"*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n",
                               (int)sdslen(config.dbnumstr),config.dbnumstr);
        c->prefix_pending++;
    }
    c->prefixlen = sdslen(c->obuf);
    /* Append the request itself. */
    if (from) {
        c->obuf = sdscatlen(c->obuf,
                            from->obuf+from->prefixlen,
                            sdslen(from->obuf)-from->prefixlen);
    } else {
        for (j = 0; j < config.pipeline; j++)
            c->obuf = sdscatlen(c->obuf,cmd,len);
    }

    c->written = 0;
    c->pending = config.pipeline+c->prefix_pending;
    c->randptr = NULL;
    c->randlen = 0;

    /* Find substrings in the output buffer that need to be randomized. */
    if (config.randomkeys) {
        if (from) {
            c->randlen = from->randlen;
            c->randfree = 0;
            c->randptr = zmalloc(sizeof(char*)*c->randlen);
            /* copy the offsets. */
            for (j = 0; j < (int)c->randlen; j++) {
                c->randptr[j] = c->obuf + (from->randptr[j]-from->obuf);
                /* Adjust for the different select prefix length. */
                c->randptr[j] += c->prefixlen - from->prefixlen;
            }
        } else {
            char *p = c->obuf;

            c->randlen = 0;
            c->randfree = RANDPTR_INITIAL_SIZE;
            c->randptr = zmalloc(sizeof(char*)*c->randfree);
            while ((p = strstr(p,"__rand_int__")) != NULL) {
                if (c->randfree == 0) {
                    c->randptr = zrealloc(c->randptr,sizeof(char*)*c->randlen*2);
                    c->randfree += c->randlen;
                }
                c->randptr[c->randlen++] = p;
                c->randfree--;
                p += 12; /* 12 is strlen("__rand_int__). */
            }
        }
    }
    if (config.idlemode == 0)
        //        aeCreateFileEvent(config.el,c->context->fd,AE_WRITABLE,writeHandler,c);
    {
        //        res.buf[0] = RDMA_REQUESTING;
        //        memcpy(res.buf+1,c->obuf,strlen(c->obuf));

        //        //rdma send request to the server
        //        if (post_send(&res, IBV_WR_RDMA_WRITE))
        //        {
        //          fprintf(stderr, "failed to post SR 3\n");
        //          return REDIS_ERR;
        //        }
        //        if (poll_completion(&res))
        //        {
        //         fprintf(stderr, "poll completion failed 3\n");
        //         return REDIS_ERR;
        //        }

    }
    listAddNodeTail(config.clients,c);
    config.liveclients++;
    //    printf("sds buffer is %s",c->obuf);
    return c;
}

static void createMissingClients(client c) {
    int n = 0;

    while(config.liveclients < config.numclients) {
        createClient(NULL,0,c);

        /* Listen backlog is quite limited on most systems */
        if (++n > 64) {
            usleep(50000);
            n = 0;
        }
    }
}

static int compareLatency(const void *a, const void *b) {
    return (*(long long*)a)-(*(long long*)b);
}

static void showLatencyReport(void) {
    int i;
    float perc, reqpersec, curlat;


    reqpersec = (float)(config.requests_finished/((float)config.totlatency/1000));
//    printf("%ld requests per second\n\n", reqpersec);
//    printf("((float)config.totlatency %ld %ld",(float)config.requests_finished,((float)config.totlatency));
    if (1) {
        printf("====== %s ======\n", config.title);
        printf("  %d requests completed in %.2f seconds\n", config.requests_finished,
              ((float)config.totlatency/1000));
        printf("  %d parallel clients\n", WORKER_NUM);
        printf("  %d bytes payload\n", config.datasize);
        printf("  keep alive: %d\n", WORKER_NUM);
        printf("\n");

        long long meanlat = 0;
//        qsort(config.latency,config.requests,sizeof(long long),compareLatency);
        for (i = 0; i < config.requests; i++) {
//            if (config.latency[i]/1000 != curlat /*|| i == (1)*/) {
                meanlat += config.latency[i];
//                printf("latency is %ld \n", config.latency[i]);
//                meanlat += curlat;
//                perc = ((float)(i+1)*100)/config.requests;
//            }
        }
        printf("average latency is %lld nanosecond\n", (long long)meanlat/config.requests_finished);
        printf("%.2f requests per second\n\n", reqpersec);
    } else if (config.csv) {
        printf("\"%s\",\"%.2f\"\n", config.title, reqpersec);
    } else {
        printf("%s: %.2f requests per second\n", config.title, reqpersec);
    }
}

static void showThroughputReport()
{
    int throughput = 0;
    for(int i = 0;i<WORKER_NUM;++i)
    {
        throughput += config.throughput[i];
        config.totlatency += config.perlatency[i];
    }
    config.totlatency = config.totlatency/WORKER_NUM;

    if (1) {
        printf("====== %s ======\n", config.title);
        printf("  %d requests completed in %.2f seconds\n", config.requests,
              ((float)config.totlatency/1000));
        printf("  %d parallel clients\n", WORKER_NUM);
        printf("  %d bytes payload\n", config.datasize);
        printf("  keep alive: %d\n", WORKER_NUM);
        printf("\n");
    }

    printf("%ld requests per second\n\n", throughput);


}

static void benchmark(char *title, char *cmd, int len) {
    client c;

    config.title = title;
    config.requests_issued = 0;
    config.requests_finished = 0;

    c = createClient(cmd,len,NULL);
//    createMissingClients(c);


    //    aeMain(config.el);
    processEvents(c);
//    config.totlatency = /*mstime()-config.start;*/
    showThroughputReport();
    freeAllClients();

}

/* Returns number of consumed options. */
int parseOptions(int argc, const char **argv) {
    int i;
    int lastarg;
    int exit_status = 1;

    for (i = 1; i < argc; i++) {
        lastarg = (i == (argc-1));

        if (!strcmp(argv[i],"-c")) {
            if (lastarg) goto invalid;
            config.numclients = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"-n")) {
            if (lastarg) goto invalid;
            config.requests = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"-k")) {
            if (lastarg) goto invalid;
            config.keepalive = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"-h")) {
            if (lastarg) goto invalid;
            config_rdma.server_name = strdup(argv[++i]);
        } else if (!strcmp(argv[i],"-p")) {
            if (lastarg) goto invalid;
            config.hostport = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"-s")) {
            if (lastarg) goto invalid;
            config.hostsocket = strdup(argv[++i]);
        } else if (!strcmp(argv[i],"-a") ) {
            if (lastarg) goto invalid;
            config.auth = strdup(argv[++i]);
        } else if (!strcmp(argv[i],"-d")) {
            if (lastarg) goto invalid;
            config.datasize = atoi(argv[++i]);
            if (config.datasize < 1) config.datasize=1;
            if (config.datasize > 1024*1024*1024) config.datasize = 1024*1024*1024;
        } else if (!strcmp(argv[i],"-P")) {
            if (lastarg) goto invalid;
            config.pipeline = atoi(argv[++i]);
            if (config.pipeline <= 0) config.pipeline=1;
        } else if (!strcmp(argv[i],"-r")) {
            if (lastarg) goto invalid;
            config.randomkeys = 1;
            config.randomkeys_keyspacelen = atoi(argv[++i]);
            if (config.randomkeys_keyspacelen < 0)
                config.randomkeys_keyspacelen = 0;
        } else if (!strcmp(argv[i],"-q")) {
            config.quiet = 1;
        } else if (!strcmp(argv[i],"--csv")) {
            config.csv = 1;
        } else if (!strcmp(argv[i],"-l")) {
            config.loop = 1;
        } else if (!strcmp(argv[i],"-I")) {
            config.idlemode = 1;
        } else if (!strcmp(argv[i],"-t")) {
            if (lastarg) goto invalid;
            /* We get the list of tests to run as a string in the form
             * get,set,lrange,...,test_N. Then we add a comma before and
             * after the string in order to make sure that searching
             * for ",testname," will always get a match if the test is
             * enabled. */
            config.tests = sdsnew(",");
            config.tests = sdscat(config.tests,(char*)argv[++i]);
            config.tests = sdscat(config.tests,",");
            sdstolower(config.tests);
        } else if (!strcmp(argv[i],"--dbnum")) {
            if (lastarg) goto invalid;
            config.dbnum = atoi(argv[++i]);
            config.dbnumstr = sdsfromlonglong(config.dbnum);
        } else if (!strcmp(argv[i],"--help")) {
            exit_status = 0;
            goto usage;
        } else {
            /* Assume the user meant to provide an option when the arg starts
             * with a dash. We're done otherwise and should use the remainder
             * as the command and arguments for running the benchmark. */
            if (argv[i][0] == '-') goto invalid;
            return i;
        }
    }

    return i;

invalid:
    printf("Invalid option \"%s\" or option argument missing\n\n",argv[i]);

usage:
    printf(
                "Usage: redis-benchmark [-h <host>] [-p <port>] [-c <clients>] [-n <requests]> [-k <boolean>]\n\n"
                " -h <hostname>      Server hostname (default 127.0.0.1)\n"
                " -p <port>          Server port (default 6379)\n"
                " -s <socket>        Server socket (overrides host and port)\n"
                " -a <password>      Password for Redis Auth\n"
                " -c <clients>       Number of parallel connections (default 50)\n"
                " -n <requests>      Total number of requests (default 100000)\n"
                " -d <size>          Data size of SET/GET value in bytes (default 2)\n"
                " -dbnum <db>        SELECT the specified db number (default 0)\n"
                " -k <boolean>       1=keep alive 0=reconnect (default 1)\n"
                " -r <keyspacelen>   Use random keys for SET/GET/INCR, random values for SADD\n"
                "  Using this option the benchmark will expand the string __rand_int__\n"
                "  inside an argument with a 12 digits number in the specified range\n"
                "  from 0 to keyspacelen-1. The substitution changes every time a command\n"
                "  is executed. Default tests use this to hit random keys in the\n"
                "  specified range.\n"
                " -P <numreq>        Pipeline <numreq> requests. Default 1 (no pipeline).\n"
                " -q                 Quiet. Just show query/sec values\n"
                " --csv              Output in CSV format\n"
                " -l                 Loop. Run the tests forever\n"
                " -t <tests>         Only run the comma separated list of tests. The test\n"
                "                    names are the same as the ones produced as output.\n"
                " -I                 Idle mode. Just open N idle connections and wait.\n\n"
                "Examples:\n\n"
                " Run the benchmark with the default configuration against 127.0.0.1:6379:\n"
                "   $ redis-benchmark\n\n"
                " Use 20 parallel clients, for a total of 100k requests, against 192.168.1.1:\n"
                "   $ redis-benchmark -h 192.168.1.1 -p 6379 -n 100000 -c 20\n\n"
                " Fill 127.0.0.1:6379 with about 1 million keys only using the SET test:\n"
                "   $ redis-benchmark -t set -n 1000000 -r 100000000\n\n"
                " Benchmark 127.0.0.1:6379 for a few commands producing CSV output:\n"
                "   $ redis-benchmark -t ping,set,get -n 100000 --csv\n\n"
                " Benchmark a specific command line:\n"
                "   $ redis-benchmark -r 10000 -n 10000 eval 'return redis.call(\"ping\")' 0\n\n"
                " Fill a list with 10000 random elements:\n"
                "   $ redis-benchmark -r 10000 -n 10000 lpush mylist __rand_int__\n\n"
                " On user specified command lines __rand_int__ is replaced with a random integer\n"
                " with a range of values selected by the -r option.\n"
                );
    exit(exit_status);
}

int showThroughput(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    REDIS_NOTUSED(eventLoop);
    REDIS_NOTUSED(id);
    REDIS_NOTUSED(clientData);

    if (config.liveclients == 0) {
        fprintf(stderr,"All clients disconnected... aborting.\n");
        exit(1);
    }
    if (config.csv) return 250;
    if (config.idlemode == 1) {
        printf("clients: %d\r", config.liveclients);
        fflush(stdout);
        return 250;
    }
    float dt = (float)(mstime()-config.start[0])/1000.0;
    float rps = (float)config.requests_finished/dt;
    printf("%s: %.2f\r", config.title, rps);
    fflush(stdout);
    return 250; /* every 250ms */
}

/* Return true if the named test was selected using the -t command line
 * switch, or if all the tests are selected (no -t passed by user). */
int test_is_selected(char *name) {
    char buf[256];
    int l = strlen(name);

    if (config.tests == NULL) return 1;
    buf[0] = ',';
    memcpy(buf+1,name,l);
    buf[l+1] = ',';
    buf[l+2] = '\0';
    return strstr(config.tests,buf) != NULL;
}



void *processEventOnce(void *t_data)
{
    int i = 0;
    struct thread_data* t_data_;
    t_data_ = (struct thread_data*) t_data;
    client c = t_data_->c;
    int task_id = t_data_->task_id;
    int requests = t_data_->requests;
    int finished_requests = t_data_->finished_requests;
    int nn = finished_requests;


    struct ibv_wc wc;
    config.start[task_id] = mstime();
    int getSetFlags = 0;
    if (test_is_selected("set")) {
        getSetFlags = 1;
     }
    else
    {
       getSetFlags = 0;
    }
    redisSetRequest set_req;
    redisGetRequest get_req;
    while(nn < requests)
    {

//         printf("thread starts here111\n");
//        __sync_fetch_and_sub(&nn, 1);
//        randomizeClientKey(c);
        if(getSetFlags)
        {
            //set cmd
            strcpy(set_req.cmd, "SET");
            sprintf(set_req.key,"key");

            memset(set_req.value,'x',1);
            c->req = (void*) (&set_req);
        }
        else //get cmd
        {
            strcpy(get_req.cmd, "GET");
            sprintf(get_req.key,"key");
            c->req = (void*) (&get_req);
//            printf("obuf is  c->obuf");
        }

        memcpy((uintptr_t)res.buf + (task_id * 2 + 1) * MSG_SIZE,c->req, sizeof(c->req));
//        IBV_WR_RDMA_WRITE_WITH_IMM
        if (post_send(&res, task_id * 2 + 1,  IBV_WR_RDMA_WRITE_WITH_IMM, true))
        {
//            fprintf(stderr, "failed to post SR 3\n");
            return REDIS_ERR;
        }

        wc.qp_num = 1;
        // poll local write cq event
        while(find_local_qp_by_id(wc.qp_num) %2 != 0)
        {
            if (poll_completion_(&res, task_id, &wc))
            {
                fprintf(stderr, "poll completion second task_id %d\n",task_id);
                //return REDIS_ERR;
            }
        }
        if (post_receive(&res, task_id * 2))
        {
            fprintf(stderr, "failed to post RR\n");
        }

//        if (config.requests_finished < config.requests)
//        {
//            if (c->latency < 0)
//            {
////                c->latency = ustime()- c->start;
//            }

////            config.latency[finished_requests++] = c->latency;
////                __sync_fetch_and_sub(&config.requests_finished, 1);
//        }

            nn += 1;
            finished_requests += 1;
         }
        if (finished_requests == requests) {

//            config.requests_finished = finished_requests;
//            printf("config.requests_finished%d",config.requests_finished);
//            config.perlatency[task_id] = mstime()-config.start[task_id];
//            if(config.perlatency[task_id] > config.totlatency)config.totlatency = config.perlatency[task_id];
            config.perlatency[task_id] = mstime()-config.start[task_id];
//            printf("xx %ld yy % ld \n", finished_requests, config.perlatency[task_id]);
            config.throughput[task_id] = (long long)config.requests/WORKER_NUM/config.perlatency[task_id]*1000;
            return;
        }

}
//struct thread_data thread_data_array[NUM_THREADS];
struct thread_data t_data[WORKER_NUM];
void processEvents(client c)
{
    //   int n = config.requests;
    int NUM_THREADS = WORKER_NUM;
    int rc, t;
    pthread_t threads[NUM_THREADS];
    for(t = 0; t < NUM_THREADS; t++){
//        char *cmd;
//        int len = redisFormatCommand(&cmd,"GET key:key%d",t/2);
//        printf("xxxxxxxxxxxxxx%s", cmd);
        t_data[t].c = c;
        t_data[t].task_id = t;
        t_data[t].requests = config.requests / (WORKER_NUM)* (t+1);
        t_data[t].finished_requests = config.requests / WORKER_NUM * t;
//        printf("In main: creating thread %d\n", t);
        rc = pthread_create(&threads[t], NULL, processEventOnce, (void *)&t_data[t]);
        if (rc){
            printf("ERROR; return code from pthread_create() is %d\n", rc);
            exit(-1);
        }

    }
//     pthread_exit(0);

    void *status;
    for(t=0; t<NUM_THREADS; t++)
       {
          rc = pthread_join(threads[t], &status);
          if (rc)
          {
             printf("ERROR; return code from pthread_join()is %d\n", rc);
             exit(-1);
          }
          printf("Completed join with thread %d status= %ld\n",t, (long)status);
       }


//        pthread_exit(NULL);
    //    while(--n)
    //    {
    ////        printf("obuf is %s\n", c->obuf);
    //        res.buf[0] = RDMA_REQUESTING;
    //        memcpy(res.buf+1,c->obuf,strlen(c->obuf));

    //        //send requests
    //        if (post_send(&res, 0, IBV_WR_RDMA_WRITE_WITH_IMM))
    //        {
    //          fprintf(stderr, "failed to post SR 3\n");
    //          return REDIS_ERR;
    //        }

    //        struct ibv_wc wc;
    //        if (poll_completion(&res, &wc))
    //        {
    //         fprintf(stderr, "poll completion failed 3\n");
    ////         return REDIS_ERR;
    //        }

    //        if (poll_completion(&res, &wc))
    //        {
    //         fprintf(stderr, "poll completion failed 3\n");
    ////         return REDIS_ERR;
    //        }
    //        else if(RDMA_REPLYING == res.buf[0])
    //        {
    //            if (config.requests_finished < config.requests)
    //                {
    //                 if (c->latency < 0) c->latency = ustime()-(c->start);
    //                    config.latency[config.requests_finished++] = c->latency;
    //                }

    //            res.buf[0] = RDMA_REPLYED;
    //            if (post_receive(&res))
    //            {
    //                fprintf(stderr, "failed to post RR\n");
    //                int rc = 1;
    //            }
    ////            printf("config.requests_finished%d",config.requests_finished);
    ////            break;
    //        }
    //        if (config.requests_finished == config.requests) {
    ////            printf("config.requests_finished%d",config.requests_finished);
    ////            break;
    //        }
    //    }


    ////        //receive responses
    ////        while(1)
    ////        {
    ////            if(RDMA_REPLYING != res.buf[0])
    ////            {
    //////             printf("time %ld\n",ustime()-(c->start));
    ////             continue;
    ////            }
    ////            if (config.requests_finished < config.requests)
    ////                {
    ////                 if (c->latency < 0) c->latency = ustime()-(c->start);
    ////                    config.latency[config.requests_finished++] = c->latency;
    ////                }

    ////            res.buf[0] = RDMA_REPLYED;
    //////            printf("config.requests_finished%d",config.requests_finished);
    ////            break;
    ////        }
    ////        if (config.requests_finished == config.requests) {
    //////            printf("config.requests_finished%d",config.requests_finished);
    ////            break;
    ////        }

}

int main(int argc, const char **argv) {



    int i;
    int len;

    client c;

    //    rdma part

    int rc = 1;
    char temp_char;
    config_rdma.tcp_port = 10086;
    config_rdma.ib_port = 1;
    config_rdma.gid_idx = -1;
    config_rdma.server_name = "10.11.6.113";

    srandom(time(NULL));
    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);

    config.numclients = 50;
    config.requests = 100000;
    config.liveclients = 0;
    config.el = aeCreateEventLoop(1024*10);
    aeCreateTimeEvent(config.el,1,showThroughput,NULL,NULL);
    config.keepalive = 1;
    config.datasize = 3;
    config.pipeline = 1;
    config.randomkeys = 0;
    config.randomkeys_keyspacelen = 0;
    config.quiet = 0;
    config.csv = 0;
    config.loop = 0;
    config.idlemode = 0;
    config.latency = NULL;
    config.clients = listCreate();
    config.hostip = "127.0.0.1";
    config.hostport = 6379;
    config.hostsocket = NULL;
    config.tests = NULL;
    config.dbnum = 0;
    config.auth = NULL;

    i = parseOptions(argc,argv);
    argc -= i;
    argv += i;

    resources_init(&res);

    if(resources_create(&res))
    {
        fprintf(stderr, "failed to create resources\n");
        return REDIS_ERR;
    }

    /* connect the QPs */
    if (connect_qp(&res))
    {
        fprintf(stderr, "failed to connect QPs\n");
        return REDIS_ERR;
    }

    config.latency = zmalloc(sizeof(long long)*config.requests);

    if (config.keepalive == 0) {
        printf("WARNING: keepalive disabled, you probably need 'echo 1 > /proc/sys/net/ipv4/tcp_tw_reuse' for Linux and 'sudo sysctl -w net.inet.tcp.msl=1000' for Mac OS X in order to use a lot of clients/requests\n");
    }

    if (config.idlemode) {
        printf("Creating %d idle connections and waiting forever (Ctrl+C when done)\n", config.numclients);
        c = createClient("",0,NULL); /* will never receive a reply */
        createMissingClients(c);
        //        aeMain(config.el);
        /* and will wait for every */
    }

    /* Run benchmark with command in the remainder of the arguments. */
    if (argc) {
        sds title = sdsnew(argv[0]);
        for (i = 1; i < argc; i++) {
            title = sdscatlen(title, " ", 1);
            title = sdscatlen(title, (char*)argv[i], strlen(argv[i]));
        }

        do {
            len = redisFormatCommandArgv(&cmd,argc,argv,NULL);
            benchmark(title,cmd,len);
            free(cmd);
        } while(config.loop);

        return 0;
    }

    /* Run default benchmark suite. */
    data = zmalloc(config.datasize+1);
    do {
        memset(data,'x',config.datasize);
        data[config.datasize] = '\0';

        if (test_is_selected("ping_inline") || test_is_selected("ping"))
            benchmark("PING_INLINE","PING\r\n",6);

        if (test_is_selected("ping_mbulk") || test_is_selected("ping")) {
            len = redisFormatCommand(&cmd,"PING");
            benchmark("PING_BULK",cmd,len);
            free(cmd);
        }

        if (test_is_selected("set")) {
            len = redisFormatCommand(&cmd,"SET key:__rand_int__ %s",data);
            benchmark("SET",cmd,len);
            free(cmd);
        }

        if (test_is_selected("get")) {
            len = redisFormatCommand(&cmd,"GET key:key");
            benchmark("GET",cmd,len);
            free(cmd);
        }

        if (test_is_selected("incr")) {
            len = redisFormatCommand(&cmd,"INCR counter:__rand_int__");
            benchmark("INCR",cmd,len);
            free(cmd);
        }

        if (test_is_selected("lpush")) {
            len = redisFormatCommand(&cmd,"LPUSH mylist %s",data);
            benchmark("LPUSH",cmd,len);
            free(cmd);
        }

        if (test_is_selected("lpop")) {
            len = redisFormatCommand(&cmd,"LPOP mylist");
            benchmark("LPOP",cmd,len);
            free(cmd);
        }

        if (test_is_selected("sadd")) {
            len = redisFormatCommand(&cmd,
                                     "SADD myset element:__rand_int__");
            benchmark("SADD",cmd,len);
            free(cmd);
        }

        if (test_is_selected("spop")) {
            len = redisFormatCommand(&cmd,"SPOP myset");
            benchmark("SPOP",cmd,len);
            free(cmd);
        }

        if (test_is_selected("lrange") ||
                test_is_selected("lrange_100") ||
                test_is_selected("lrange_300") ||
                test_is_selected("lrange_500") ||
                test_is_selected("lrange_600"))
        {
            len = redisFormatCommand(&cmd,"LPUSH mylist %s",data);
            benchmark("LPUSH (needed to benchmark LRANGE)",cmd,len);
            free(cmd);
        }

        if (test_is_selected("lrange") || test_is_selected("lrange_100")) {
            len = redisFormatCommand(&cmd,"LRANGE mylist 0 99");
            benchmark("LRANGE_100 (first 100 elements)",cmd,len);
            free(cmd);
        }

        if (test_is_selected("lrange") || test_is_selected("lrange_300")) {
            len = redisFormatCommand(&cmd,"LRANGE mylist 0 299");
            benchmark("LRANGE_300 (first 300 elements)",cmd,len);
            free(cmd);
        }

        if (test_is_selected("lrange") || test_is_selected("lrange_500")) {
            len = redisFormatCommand(&cmd,"LRANGE mylist 0 449");
            benchmark("LRANGE_500 (first 450 elements)",cmd,len);
            free(cmd);
        }

        if (test_is_selected("lrange") || test_is_selected("lrange_600")) {
            len = redisFormatCommand(&cmd,"LRANGE mylist 0 599");
            benchmark("LRANGE_600 (first 600 elements)",cmd,len);
            free(cmd);
        }

        if (test_is_selected("mset")) {
            const char *argv[21];
            argv[0] = "MSET";
            for (i = 1; i < 21; i += 2) {
                argv[i] = "key:__rand_int__";
                argv[i+1] = data;
            }
            len = redisFormatCommandArgv(&cmd,21,argv,NULL);
            benchmark("MSET (10 keys)",cmd,len);
            free(cmd);
        }

        if (!config.csv) printf("\n");
    } while(config.loop);

    return 0;
}
