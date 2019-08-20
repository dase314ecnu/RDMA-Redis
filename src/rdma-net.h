/*
* BUILD COMMAND:
* gcc -Wall -I/usr/local/ofed/include -O2 -o RDMA_RC_example -L/usr/local/ofed/lib64 -L/usr/local/ofed/lib -
libverbs RDMA_RC_example.c
*
*/
/******************************************************************************
*
* RDMA Aware Networks Programming Example
*
* This code demonstrates how to perform the following operations using the * VPI Verbs API:
*
* Send
* Receive
* RDMA Read
* RDMA Write
*
*****************************************************************************/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <inttypes.h>
#include <endian.h>
#include <byteswap.h>
#include <getopt.h>
#include <fcntl.h>
#include <unistd.h>
#include <poll.h>
#include <pthread.h>
#include <stdbool.h>

#include <sys/time.h>
#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
/* poll CQ timeout in millisec (2 seconds) */
#define MAX_POLL_CQ_TIMEOUT 2000
#define MSG_SIZE  (32 * 8)
#define RDMA_ADDRESS_SIZE MSG_SIZE * 10000
#define SERVER_QP_NUMBER 30
#define SERVER_CQ_NUMBER 5
#define CLIENT_QP_NUMBER 30
#define CLIENT_CQ_NUMBER 15
#define QPS_MAX_DEPTH 128
#define WORKER_NUM 15
#define SERVER_WORKER_MUM 5


#if __BYTE_ORDER == __LITTLE_ENDIAN
static inline uint64_t htonll(uint64_t x) { return bswap_64(x); }
static inline uint64_t ntohll(uint64_t x) { return bswap_64(x); }
#elif __BYTE_ORDER == __BIG_ENDIAN
static inline uint64_t htonll(uint64_t x) { return x; }
static inline uint64_t ntohll(uint64_t x) { return x; }
#else
#error __BYTE_ORDER is neither __LITTLE_ENDIAN nor __BIG_ENDIAN
#endif
/* structure of test parameters */
struct config_t
{
    const char *dev_name; /* IB device name */
    char *server_name;	  /* server host name */
    u_int32_t tcp_port;   /* server TCP port */
    int ib_port;		  /* local IB port to work with */
    int gid_idx;		  /* gid index to use */
};
typedef struct redisTestRequest{
    char msg[MSG_SIZE];
}redisTestRequest;

typedef struct redisSetRequest{
    char cmd[4];
    char key[15];
    char value[MSG_SIZE - 30];
}redisSetRequest;

typedef struct redisGetRequest{
    char cmd[4];
    char key[15];
}redisGetRequest;

/* structure to exchange data which is needed to connect the QPs */
struct cm_con_data_t
{
    uint64_t addr;   /* Buffer address */
    uint32_t rkey;   /* Remote key */
    uint32_t qp_num[CLIENT_QP_NUMBER]; /* QP number */
    uint16_t lid;	/* LID of the IB port */
    uint8_t gid[16]; /* gid */
} __attribute__((packed));

/* structure of system resources */
//struct resources
//{
//    struct ibv_device_attr device_attr;
//    /* Device attributes */
//    struct ibv_port_attr port_attr;	/* IB port attributes */
//    struct cm_con_data_t remote_props; /* values to connect to remote side */
//    struct ibv_context *ib_ctx;		   /* device handle */
//    struct ibv_comp_channel *channel;  /* completion channel*/
//    struct ibv_pd *pd;				   /* PD handle  Protection Domain */
//    struct ibv_cq *cq;				   /* CQ handle */
//    struct ibv_qp *qp;				   /* QP handle */
//    struct ibv_mr *mr;				   /* MR handle for buf */
//    char *buf;						   /* memory buffer pointer, used for RDMA and send
//ops */
////    char *res_buf;                     /* memory buffer pointer, used for RDMA response */
//    int sock;						   /* TCP socket file descriptor */
//};
struct resources
{
    struct ibv_device_attr device_attr;
    /* Device attributes */
    struct ibv_port_attr port_attr;	/* IB port attributes */
    struct cm_con_data_t remote_props; /* values to connect to remote side */
    struct ibv_context *ib_ctx;		   /* device handle */
    struct ibv_comp_channel *channel;  /* completion channel*/
    struct ibv_pd *pd;				   /* PD handle  Protection Domain */
    struct ibv_cq **cq;	   /* CQ handle */
    struct ibv_qp **qp;	   /* QP handle */
    uint32_t *qp_num;
    struct ibv_mr *mr;				   /* MR handle for buf */
    char *buf;						   /* memory buffer pointer, used for RDMA and send
ops */
//    char *res_buf;                     /* memory buffer pointer, used for RDMA response */
    int sock;						   /* TCP socket file descriptor */
};



int sock_connect(const char *servername, int port);
int sock_sync_data_(int sock, int xfer_size, char *local_data, char *remote_data);
int poll_completion(struct resources *res, int id, struct ibv_wc *wc);
int poll_completion_(struct resources *res, int id, struct ibv_wc *wc);
int post_send(struct resources *res, int id ,int opcode, bool batch);
int post_receive(struct resources *res,int i);
void resources_init(struct resources *res);
int resources_create(struct resources *res);
int modify_qp_to_init(struct ibv_qp *qp);
int modify_qp_to_rtr(struct ibv_qp *qp, uint32_t remote_qpn, uint16_t dlid, uint8_t *dgid);
int modify_qp_to_rts(struct ibv_qp *qp);
int connect_qp_once(struct resources *res, int i,int j);
int connect_qp(struct resources *res);
int resources_destroy(struct resources *res);
int find_local_qp_by_id(uint32_t qp_num);

