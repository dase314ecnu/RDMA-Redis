#include "rdma-net.h"
struct config_t config_rdma = {
    NULL,  /* dev_name */
    NULL,  /* server_name */
    10086, /* tcp_port */
    1,	 /* ib_port */
    -1 /* gid_idx */};
struct cm_con_data_t local_con_data;
struct cm_con_data_t remote_con_data;
struct cm_con_data_t tmp_con_data;
union ibv_gid my_gid;
int cqNum = SERVER_CQ_NUMBER;
int qpNum = SERVER_QP_NUMBER;
int Mode = 0;

/******************************************************************************
Socket operations
******************************************************************************/
/*******************************
For simplicity, the example program uses TCP sockets to exchange control
information. If a TCP/IP stack/connection is not available, connection manager
(CM) may be used to pass this information. Use of CM is beyond the scope of
this example*********** ************************************
* Function: sock_connect
*
* Input
* servername URL of server to connect to (NULL for server
*
* )
* port port of service
*
* Output
* none
*
* Returns
* socket (fd) on success, negative error code on failure
*
* Description
* Connect a socket. If servername is specified a client connection will be
* initiated to the indicated server and port. Otherwise listen on the
* indicated port for an incoming connection.
*
******************************************************************************/
int sock_connect(const char *servername, int port)
{
    struct addrinfo *resolved_addr = NULL;
    struct addrinfo *iterator;
    char service[6];
    int sockfd = -1;
    int listenfd = 0;
    int tmp;
    struct addrinfo hints =
        {
            .ai_flags = AI_PASSIVE,
            .ai_family = AF_INET,
            .ai_socktype = SOCK_STREAM};
    if (sprintf(service, "%d", port) < 0)
        goto sock_connect_exit;
    /* Resolve DNS address, use sockfd as temp storage */
    sockfd = getaddrinfo(servername, service, &hints, &resolved_addr);
    if (sockfd < 0)
    {
        fprintf(stderr, "%s for %s:%d\n", gai_strerror(sockfd), servername, port);
        goto sock_connect_exit;
    }
    /* Search through results and find the one we want */
    for (iterator = resolved_addr; iterator; iterator = iterator->ai_next)
    {
        sockfd = socket(iterator->ai_family, iterator->ai_socktype, iterator->ai_protocol);
        if (sockfd >= 0)
        {
            if (servername){
                /* Client mode. Initiate connection to remote */
                if ((tmp = connect(sockfd, iterator->ai_addr, iterator->ai_addrlen)))
                {
                    fprintf(stdout, "failed connect \n");
                    close(sockfd);
                    sockfd = -1;
                }
            }
            else
            {
                    /* Server mode. Set up listening socket an accept a connection */
                    listenfd = sockfd;
                    sockfd = -1;
                    if (bind(listenfd, iterator->ai_addr, iterator->ai_addrlen))
                        goto sock_connect_exit;
                    listen(listenfd, 1);
                    sockfd = accept(listenfd, NULL, 0);
            }
        }
    }
sock_connect_exit:
    if (listenfd)
        close(listenfd);
    if (resolved_addr)
        freeaddrinfo(resolved_addr);
    if (sockfd < 0)
    {
        if (servername)
            fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
        else
        {
            perror("server accept");
            fprintf(stderr, "accept() failed\n");
        }
    }
    return sockfd;
}
/******************************************************************************
* Function: sock_sync_data_
*
* Input
* sock socket to transfer data on
* xfer_size size of data to transfer
* local_data pointer to data to be sent to remote
*
* Output
* remote_data pointer to buffer to receive remote data
*
* Returns
* 0 on success, negative error code on failure
*
* Description
* Sync data across a socket. The indicated local data will be sent to the
* remote. It will then wait for the remote to send its data back. It is
* assumed that the two sides are in sync and call this function in the proper
* order. Chaos will ensue if they are not. :)
*
* Also note this is a blocking function and will wait for the full data to be
* received from the remote.
*
******************************************************************************/
int sock_sync_data_(int sock, int xfer_size, char *local_data, char *remote_data)
{
    int rc;
    int read_bytes = 0;
    int total_read_bytes = 0;
    rc = write(sock, local_data, xfer_size);
    if (rc < xfer_size)
        fprintf(stderr, "Failed writing data during sock_sync_data_\n");
    else
        rc = 0;
    while (!rc && total_read_bytes < xfer_size)
    {
        read_bytes = read(sock, remote_data, xfer_size);
        if (read_bytes > 0)
            total_read_bytes += read_bytes;
        else
            rc = read_bytes;
    }
    return rc;
}
/******************************************************************************
End of socket operations
******************************************************************************/
/* poll_completion */
/******************************************************************************
* Function: poll_completion
*
* Input
* res pointer to resources structure
*
* Output
* none
*
* Returns
* 0 on success, 1 on failure
*
* Description
* Poll the completion queue for a single event. This function will continue to
* poll the queue until MAX_POLL_CQ_TIMEOUT milliseconds have passed.
*
******************************************************************************/
int poll_completion_(struct resources *res, int id,struct ibv_wc *wc)
{
//    struct ibv_wc wc;
    unsigned long start_time_msec;
    unsigned long cur_time_msec;
    struct timeval cur_time;
    int poll_result;
    int rc = 0;
    /* poll the completion for a while before giving up of doing it .. */
//    gettimeofday(&cur_time, NULL);
//    start_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
    do
    {
        poll_result = ibv_poll_cq(res->cq[id], 1, wc);
//        gettimeofday(&cur_time, NULL);
//        cur_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
    } while ((poll_result < 1) /*&& ((cur_time_msec - start_time_msec) < MAX_POLL_CQ_TIMEOUT)*/);
    if (poll_result < 0)
    {
        /* poll CQ failed */
        fprintf(stderr, "poll CQ failed\n");
        rc = 1;
    }
    else if (poll_result == 0)
    { /* the CQ is empty */
        fprintf(stderr, "%d  completion wasn't found in the CQ after timeout\n", id);
        rc = 1;
    }
    else
    {
        /* let the client post RR to be prepared for incoming messages */
//        rc = post_receive(res);
//        if (rc)
//        {
//            fprintf(stderr, "failed to post RR\n");
//            rc = 1;
//        }
        /* CQE found */
//        fprintf(stdout, "completion was found in CQ with status 0x%x\n", wc->status);
        /* check the completion status (here we don't care about the completion opcode */
        if (wc->status != IBV_WC_SUCCESS)
        {
            fprintf(stderr, "got bad completion with status: 0x%x, vendor syndrome: 0x%x\n", wc->status,
                    wc->vendor_err);
            rc = 1;
        }
    }
//    if (post_receive(res,0))
//    {
//        fprintf(stderr, "failed to post RR\n");
//        rc = 1;
//    }

    return rc;
}

int poll_completion(struct resources *res, int id, struct ibv_wc *wc)
{
    unsigned long start_time_msec;
    unsigned long cur_time_msec;
    struct timeval cur_time;

    int rc = 0;
    struct pollfd my_pollfd;
    struct ibv_cq *ev_cq;
    void *ev_ctx;
    int ne;
    int ms_timeout = 10;
//    printf("poll_completion01\n");
    /*
     * poll the channel until it has an event and sleep ms_timeout
     * milliseconds between any iteration
     */
    my_pollfd.fd      = res->channel->fd;
    my_pollfd.events  = POLLIN;
    my_pollfd.revents = 0;

    do {
        rc = poll(&my_pollfd, 1, ms_timeout);
    } while (rc == 0);
    if (rc < 0) {
            fprintf(stderr, "poll failed\n");
            return -1;
    }
    ev_cq = res->cq[id];
    /* Wait for the completion event */
    rc = ibv_get_cq_event(res->channel, &ev_cq, &ev_ctx);
    if (rc) {
            fprintf(stderr, "Failed to get cq_event\n");
            return -1;
    }
    /* Ack the event */
    ibv_ack_cq_events(ev_cq, 1);

    /* Request notification upon the next completion event */
    rc = ibv_req_notify_cq(ev_cq, 0);
    if (rc) {
            fprintf(stderr, "Couldn't request CQ notification\n");
            return -1;
    }

//    printf("poll_completion02\n");
    /* poll the completion for a while before giving up of doing it .. */
    gettimeofday(&cur_time, NULL);
    start_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
    /* Empty the CQ: poll all of the completions from the CQ (if any exist) */
    do {
            ne = ibv_poll_cq(ev_cq, 1, wc);
            gettimeofday(&cur_time, NULL);
            cur_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
            if (ne < 0) {
                    fprintf(stderr, "Failed to poll completions from the CQ: ret = %d\n",
                            ne);
                    return 1;
            }
            /* there may be an extra event with no completion in the CQ */
            if (ne == 0)
                    continue;

            if ( wc->status != IBV_WC_SUCCESS) {
                    fprintf(stderr, "Completion with status 0x%x was found\n",
    wc->status);
                    return 1;
            }
    } while (ne&& ((cur_time_msec - start_time_msec) < MAX_POLL_CQ_TIMEOUT));
//    printf("poll_completion03\n");

//    if(res.)
//    if (post_receive(res))
//    {
//        fprintf(stderr, "failed to post RR\n");
//        rc = 1;
//    }

    return rc;
}
/******************************************************************************
* Function: post_send
*
* Input
* res pointer to resources structure
* opcode IBV_WR_SEND, IBV_WR_RDMA_READ or IBV_WR_RDMA_WRITE
*
* Output
* none
*
* Returns
* 0 on success, error code on failure
*
* Description
* This function will create and post a send work request
******************************************************************************/
int post_send(struct resources *res, int id ,int opcode, bool batch)
{
    if(id >= qpNum)
    {
        fprintf(stderr, "qp id exceed the maximum!");
        return 1;
    }
    struct ibv_send_wr sr;
    struct ibv_sge sge;
    struct ibv_send_wr *bad_wr = NULL;
    int rc;
    /* prepare the scatter/gather entry */
    memset(&sge, 0, sizeof(sge));
//    sge.addr = (uintptr_t)res->buf + id * MSG_SIZE;
    sge.addr = (uintptr_t)res->buf+ id * MSG_SIZE;
    sge.length = MSG_SIZE;
    sge.lkey = res->mr->lkey;
    /* prepare the send work request */
    memset(&sr, 0, sizeof(sr));
    sr.next = NULL;
    sr.wr_id = 0;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.opcode = opcode;
    sr.send_flags = 0;
    if(batch == true)
    {
      sr.send_flags = IBV_SEND_SIGNALED;
    }
    if (opcode != IBV_WR_SEND)
    {
        // spit the buffer into req/res fragment
        sr.wr.rdma.remote_addr = res->remote_props.addr + id * MSG_SIZE;
        sr.wr.rdma.rkey = res->remote_props.rkey;
    }
    /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
//    redisTestRequest *req = (redisTestRequest *)((uintptr_t)res->buf + id * MSG_SIZE);
//    printf("redisTestRequest %d %s \n ", id ,req->msg);
    rc = ibv_post_send(res->qp[id], &sr, &bad_wr);
    if (rc)
        fprintf(stderr, "failed to post SR\n");
    else
    {
//        switch (opcode)
//        {
//        case IBV_WR_SEND:
////            fprintf(stdout, "Send Request was posted\n");
//            break;
//        case IBV_WR_RDMA_READ:
////            fprintf(stdout, "RDMA Read Request was posted\n");
//            break;
//        case IBV_WR_RDMA_WRITE:
////            fprintf(stdout, "RDMA Write Request was posted\n");
//            break;
//        default:
////            fprintf(stdout, "Unknown Request was posted\n");
//            break;
//        }
    }
    return rc;
}
/******************************************************************************
* Function: post_receive
*
* Input
* res pointer to resources structure
*
* Output
* none
*
* Returns
* 0 on success, error code on failure
*
* Description
*
******************************************************************************/
int post_receive(struct resources *res,int i)
{
    struct ibv_recv_wr rr;
    struct ibv_sge sge;
    struct ibv_recv_wr *bad_wr;
    int rc;
    /* prepare the scatter/gather entry */
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)res->buf + i * MSG_SIZE;
    sge.length = MSG_SIZE;
    sge.lkey = res->mr->lkey;

    /* prepare the receive work request */
    memset(&rr, 0, sizeof(rr));
    rr.next = NULL;
    rr.wr_id = 0;
    rr.sg_list = &sge;
    rr.num_sge = 1;
    /* post the Receive Request to the RQ */
    rc = ibv_post_recv(res->qp[i], &rr, &bad_wr);
    if (rc)
        fprintf(stderr, "failed to post RR\n");
    else
//        fprintf(stdout, "Receive Request was posted\n");

    return rc;
}
/******************************************************************************
* Function: resources_init
*
* Input
* res pointer to resources structure
*
* Output
* res is initialized
*
* Returns
* none
*
* Description
* res is initialized to default values
******************************************************************************/
void resources_init(struct resources *res)
{
    char *temp_char;
    memset(res, 0, sizeof *res);
    if(config_rdma.server_name)
    {

        cqNum = CLIENT_CQ_NUMBER;
        qpNum = CLIENT_QP_NUMBER;

    }
    res->cq = (struct ibv_cq **)malloc(cqNum * sizeof(struct ibv_cq *));
//    for (int i = 0; i < cqNum; i++)
//        res->cq[i] = NULL;

    res->qp = (struct ibv_qp **)malloc(qpNum * sizeof(struct ibv_qp *));
    res->qp_num = (uint32_t *)malloc(qpNum * sizeof(uint32_t));
    for (int i = 0; i < CLIENT_QP_NUMBER; i++)
    {
        local_con_data.qp_num[i] = 0;
        remote_con_data.qp_num[i] = 0;
        tmp_con_data.qp_num[i] = 0;
    }

    res->sock = -1;
}
/******************************************************************************
* Function: resources_create
*
* Input
* res pointer to resources structure to be filled in
*
* Output
* res filled in with resources
*
* Returns
* 0 on success, 1 on failure
*
* Description
*
* This function creates and allocates all necessary system resources. These
* are stored in res.
*****************************************************************************/
int resources_create(struct resources *res)
{
    struct ibv_device **dev_list = NULL;
    struct ibv_device *ib_dev = NULL;
    size_t size;
    int i;
    int mr_flags = 0;
    int cq_size = QPS_MAX_DEPTH;
    int num_devices;
    int rc = 0;

    /* if client side */
    if (config_rdma.server_name)
    {
        res->sock = sock_connect(config_rdma.server_name, config_rdma.tcp_port);
        if (res->sock < 0)
        {
            fprintf(stderr, "failed to establish TCP connection to server %s, port %d\n",
                    config_rdma.server_name, config_rdma.tcp_port);
            rc = -1;
            goto resources_create_exit;
        }
    }
    else
    {
        fprintf(stdout, "waiting on port %d for TCP connection\n", config_rdma.tcp_port);
        res->sock = sock_connect(NULL, config_rdma.tcp_port);
        if (res->sock < 0)
        {
            fprintf(stderr, "failed to establish TCP connection with client on port %d\n",
                    config_rdma.tcp_port);
            rc = -1;
            goto resources_create_exit;
        }
    }
    fprintf(stdout, "TCP connection was established\n");
    fprintf(stdout, "searching for IB devices in host\n");
    /* get device names in the system */
    dev_list = ibv_get_device_list(&num_devices);
    if (!dev_list)
    {
        fprintf(stderr, "failed to get IB devices list\n");
        rc = 1;
        goto resources_create_exit;
    }
    /* if there isn't any IB device in host */
    if (!num_devices)
    {
        fprintf(stderr, "found %d device(s)\n", num_devices);
        rc = 1;
        goto resources_create_exit;
    }
    fprintf(stdout, "found %d device(s)\n", num_devices);
    /* search for the specific device we want to work with */
    for (i = 0; i < num_devices; i++)
    {
        if (!config_rdma.dev_name)
        {
            config_rdma.dev_name = strdup(ibv_get_device_name(dev_list[i]));
            fprintf(stdout, "device not specified, using first one found: %s\n", config_rdma.dev_name);

        }
        if (!strcmp(ibv_get_device_name(dev_list[i]), config_rdma.dev_name))
        {
            ib_dev = dev_list[i];
            break;
        }
    }

    /* if the device wasn't found in host */
    if (!ib_dev)
    {
        fprintf(stderr, "IB device %s wasn't found\n", config_rdma.dev_name);
        rc = 1;
        goto resources_create_exit;
    }
    /* get device handle */
    res->ib_ctx = ibv_open_device(ib_dev);
    if (!res->ib_ctx)
    {
        fprintf(stderr, "failed to open device %s\n", config_rdma.dev_name);
        rc = 1;
        goto resources_create_exit;
    }
    /* We are now done with device list, free it */
    ibv_free_device_list(dev_list);
    dev_list = NULL;
    ib_dev = NULL;
    /* query port properties */
    if (ibv_query_port(res->ib_ctx, config_rdma.ib_port, &res->port_attr))
    {
        fprintf(stderr, "ibv_query_port on port %u failed\n", config_rdma.ib_port);
        rc = 1;
        goto resources_create_exit;
    }
    /* allocate Protection Domain */
    res->pd = ibv_alloc_pd(res->ib_ctx);
    if (!res->pd)
    {
        fprintf(stderr, "ibv_alloc_pd failed\n");
        rc = 1;
        goto resources_create_exit;
    }
    /* each side will send only one WR, so Completion Queue with 1 entry is enough */

//    struct ibv_cq *ibv_create_cq(struct ibv_context *context, int cqe,
//                   void *cq_context, struct ibv_comp_channel *channel, int comp_vector)

//    res->channel = ibv_create_comp_channel(res->ib_ctx);
//    if (!res->channel) {
//        fprintf(stderr, "Error, ibv_create_comp_channel() failed\n");
//        return -1;
//    }
    for (i = 0; i < cqNum; i++) {

        res->cq[i] = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);
        if (!res->cq[i])
        {
            fprintf(stderr, "failed to create CQ with %u entries\n", cq_size);
            rc = 1;
            goto resources_create_exit;
        }
//        printf("ibv_create_cq 01\n");

        /* Request notification before any completion can be created (to prevent races) */
//        rc = ibv_req_notify_cq(res->cq[i], 0);
//        if (rc) {
//                fprintf(stderr, "Couldn't request CQ notification\n");
//                return -1;
//        }
    }

      printf("ibv_create_cq 02\n");
    /* change the blocking mode of the completion channel */
//    int flags;
//    flags = fcntl(res->channel->fd, F_GETFL);
//    rc = fcntl(res->channel->fd, F_SETFL, flags | O_NONBLOCK);
//    if (rc < 0) {
//            fprintf(stderr, "Failed to change file descriptor of Completion Event Channel\n");
//            return -1;
//    }
    printf("ibv_create_cq 03\n");

    /* allocate the memory buffer that will hold the data */

    size = RDMA_ADDRESS_SIZE;

    res->buf = (char *)malloc(size);
//    res->res_buf = (char *)malloc(size);
    if (!(res->buf))
    {
        fprintf(stderr, "failed to malloc %Zu bytes to memory buffer\n", size);
        rc = 1;
        goto resources_create_exit;
    }
    memset(res->buf, 0, size);
//    memset(res->res_buf, 0, size);
    /* only in the server side put the message in the memory buffer */
//    if (!config_rdma.server_name)
//    {
////        strcpy(res->buf, MSG);
//        fprintf(stdout, "going to send the message: '%s'\n", res->buf);
//    }
//    else
//    {
//        memset(res->buf, 0, size);
////        memset(res->res_buf, 0, size);
//    }
    /* register the memory buffer */
//    mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;

    res->mr = ibv_reg_mr(res->pd, res->buf, size, mr_flags);
    if (!res->mr)
    {
        fprintf(stderr, "ibv_reg_mr failed with mr_flags=0x%x\n", mr_flags);
        rc = 1;
        goto resources_create_exit;
    }
    fprintf(stdout, "MR was registered with addr=%p, lkey=0x%x, rkey=0x%x, flags=0x%x\n",
            res->buf, res->mr->lkey, res->mr->rkey, mr_flags);

    for(int i = 0; i<qpNum; i++)
    {
        /* create the Queue Pair */
        struct ibv_qp_init_attr qp_init_attr;
        memset(&qp_init_attr, 0, sizeof(qp_init_attr));
        if(Mode == 0)
        {
          qp_init_attr.qp_type = IBV_QPT_RC;
        }
        else
        {
          qp_init_attr.qp_type = IBV_QPT_UC;
        }
        qp_init_attr.sq_sig_all = 0;
        /* 0->0 1->0 2->1 3->1*/
        if(config_rdma.server_name)
        {
            qp_init_attr.send_cq = res->cq[i/2];
            qp_init_attr.recv_cq = res->cq[i/2];
            printf("cq num %d \n", i/2);
        }
        else
        {

            qp_init_attr.send_cq = res->cq[i/(2*(CLIENT_CQ_NUMBER/SERVER_CQ_NUMBER))];
            qp_init_attr.recv_cq = res->cq[i/(2*(CLIENT_CQ_NUMBER/SERVER_CQ_NUMBER))];
            printf("cq num %d \n", i/(2*(CLIENT_CQ_NUMBER/SERVER_CQ_NUMBER)));

        }
        qp_init_attr.cap.max_send_wr = QPS_MAX_DEPTH;
        qp_init_attr.cap.max_recv_wr = QPS_MAX_DEPTH;
        qp_init_attr.cap.max_send_sge = 1;
        qp_init_attr.cap.max_recv_sge = 1;
        qp_init_attr.cap.max_inline_data = 0;
        printf("ibv_create_cq 04\n");
        res->qp[i] = ibv_create_qp(res->pd, &qp_init_attr);
        if (!res->qp[i])
        {
            fprintf(stderr, "failed to create QP\n");
            rc = 1;
            goto resources_create_exit;
        }
        printf("ibv_create_cq 05\n");
        res->qp_num[i] = res->qp[i]->qp_num;
        fprintf(stdout, "QP was created, QP number=0x%x\n", res->qp[i]->qp_num);
        printf("ibv_create_cq 06\n");
    }
    return rc;
resources_create_exit:
    if (rc)
    {
        /* Error encountered, cleanup */
        for(int i = 0; i<qpNum; i++){
            if (res->qp[i])
            {
                ibv_destroy_qp(res->qp[i]);
                res->qp[i] = NULL;
            }
        }
        if (res->mr)
        {
            ibv_dereg_mr(res->mr);
            res->mr = NULL;
        }
        if (res->buf)
        {
            free(res->buf);
            res->buf = NULL;
        }
        for(int i = 0; i<qpNum; i++){
            if (res->cq[i])
            {
                ibv_destroy_cq(res->cq[i]);
                res->cq[i] = NULL;
            }
        }
        if (res->pd)
        {
            ibv_dealloc_pd(res->pd);
            res->pd = NULL;
        }
        if (res->ib_ctx)
        {
            ibv_close_device(res->ib_ctx);
            res->ib_ctx = NULL;
        }
        if (dev_list)
        {
            ibv_free_device_list(dev_list);
            dev_list = NULL;
        }
        if (res->sock >= 0)
        {
            if (close(res->sock))
                fprintf(stderr, "failed to close socket\n");
            res->sock = -1;
        }
    }
    return rc;
}
/******************************************************************************
* Function: modify_qp_to_init
*
* Input
* qp QP to transition
*
* Output
* none
*
* Returns
* 0 on success, ibv_modify_qp failure code on failure
*
* Description
* Transition a QP from the RESET to INIT state
******************************************************************************/
int modify_qp_to_init(struct ibv_qp *qp)
{
    struct ibv_qp_attr attr;
    int flags;
    int rc;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = config_rdma.ib_port;
    attr.pkey_index = 0;
//    attr.qp_access_flags = IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
    if (Mode == 0) {
        attr.qp_access_flags = IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
    } else if (Mode == 1) {
    attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE;
    }
//    flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
    rc = ibv_modify_qp(qp, &attr, flags);
    if (rc)
        fprintf(stderr, "failed to modify QP state to INIT\n");
    return rc;
}
/******************************************************************************
* Function: modify_qp_to_rtr
*
* Input
* qp QP to transition
* remote_qpn remote QP number
* dlid destination LID
* dgid destination GID (mandatory for RoCEE)
*
* Output
* none
*
* Returns
* 0 on success, ibv_modify_qp failure code on failure
*
* Description
* Transition a QP from the INIT to RTR state, using the specified QP number
******************************************************************************/
int modify_qp_to_rtr(struct ibv_qp *qp, uint32_t remote_qpn, uint16_t dlid, uint8_t *dgid)
{
    struct ibv_qp_attr attr;
    int flags;
    int rc;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = IBV_MTU_4096;
    attr.dest_qp_num = remote_qpn;
    attr.rq_psn = 0;
//    attr.max_dest_rd_atomic = 1;
//    attr.min_rnr_timer = 0x12;
    attr.ah_attr.is_global = 0;
    attr.ah_attr.dlid = dlid;
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = config_rdma.ib_port;
//    if (config_rdma.gid_idx >= 0)
//    {
//        attr.ah_attr.is_global = 1;
//        attr.ah_attr.port_num = 1;
//        memcpy(&attr.ah_attr.grh.dgid, dgid, 16);
//        attr.ah_attr.grh.flow_label = 0;
//        attr.ah_attr.grh.hop_limit = 1;
//        attr.ah_attr.grh.sgid_index = config_rdma.gid_idx;
//        attr.ah_attr.grh.traffic_class = 0;
//    }
    flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN;
    if (Mode == 0) {
        attr.max_dest_rd_atomic = 16;
        attr.min_rnr_timer = 12;
        flags |= IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
    }

//    flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
    rc = ibv_modify_qp(qp, &attr, flags);
    if (rc)
        fprintf(stderr, "failed to modify QP state to RTR\n");
    return rc;
}
/******************************************************************************
* Function: modify_qp_to_rts
*
* Input
* qp QP to transition
*
* Output
* none
*
* Returns
* 0 on success, ibv_modify_qp failure code on failure
*
* Description
* Transition a QP from the RTR to RTS state
******************************************************************************/
int modify_qp_to_rts(struct ibv_qp *qp)
{
    struct ibv_qp_attr attr;
    int flags;
    int rc;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
//    attr.timeout = 0x12;
//    attr.retry_cnt = 6;
//    attr.rnr_retry = 0;
    attr.sq_psn = 0;
    flags = IBV_QP_STATE | IBV_QP_SQ_PSN;

    if (Mode == 0) {
        attr.timeout = 14;
        attr.retry_cnt = 6;
        attr.rnr_retry = 0;
        attr.max_rd_atomic = 16;
        attr.max_dest_rd_atomic = 16;
        flags |= IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_MAX_QP_RD_ATOMIC;
    }
//    flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
//            IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;

    rc = ibv_modify_qp(qp, &attr, flags);
    if (rc)
        fprintf(stderr, "failed to modify QP state to RTS\n");
    return rc;
}
/******************************************************************************
* Function: connect_qp
*
* Input
* res pointer to resources structure
*
* Output
* none
*
* Returns
* 0 on success, error code on failure
*
* Description
* Connect the QP. Transition the server side to RTR, sender side to RTS
******************************************************************************/
int connect_qp_once(struct resources *res, int i,int j)
{
    int rc = 0;
    /* modify the QP to init */
    rc = modify_qp_to_init(res->qp[i]);
    if (rc)
    {
        fprintf(stderr, "change QP state to INIT failed\n");
        return rc;
    }
    /* let the client post RR to be prepared for incoming messages */
//    rc = post_receive(res);
//    if (rc)
//    {
//        fprintf(stderr, "failed to post RR\n");
//        goto connect_qp_exit;
//    }
    /* modify the QP to RTR */
    rc = modify_qp_to_rtr(res->qp[i], remote_con_data.qp_num[j], remote_con_data.lid, remote_con_data.gid);
    if (rc)
    {
        fprintf(stderr, "failed to modify QP state to RTR\n");
        return rc;
    }

    rc = modify_qp_to_rts(res->qp[i]);
    if (rc)
    {
        fprintf(stderr, "failed to modify QP state to RTR\n");
        return rc;
    }
    fprintf(stdout, "QP state was change to RTS\n");

//    rc = post_receive(res);
//    if (rc)
//    {
//        fprintf(stderr, "failed to post RR\n");
//        goto connect_qp_exit;
//    }
    for(int k = 0; k < qpNum; k++)
    {
        for(int j = 0;j < 1; j++)
        {
            rc = post_receive(res,k);
            if (rc)
            {
                fprintf(stderr, "failed to post RR\n");
                return rc;
            }
        }
    }

}
int connect_qp(struct resources *res)
{
    int rc = 0;
    char * temp_char;
    printf("connect_qp 01\n");
    if (config_rdma.gid_idx >= 0)
    {
        rc = ibv_query_gid(res->ib_ctx, config_rdma.ib_port, config_rdma.gid_idx, &my_gid);
        if (rc)
        {
            fprintf(stderr, "could not get gid for port %d, index %d\n", config_rdma.ib_port, config_rdma.gid_idx);
            return rc;
        }
    }
    else
        memset(&my_gid, 0, sizeof my_gid);

    /* exchange using TCP sockets info required to connect QPs */
    local_con_data.addr = htonll((uintptr_t)res->buf);
    local_con_data.rkey = htonl(res->mr->rkey);
    printf("size of local %d \n ",sizeof(struct cm_con_data_t));
    for(int i=0; i<qpNum;i++)
    {
//     memcpy(local_con_data.qp_num[i], res->qp[i]->qp_num, sizeof(uint32_t));
       local_con_data.qp_num[i] = htonl(res->qp[i]->qp_num);
       printf("local QP number = 0x%x\n", local_con_data.qp_num[i]);
    }


    printf("connect_qp 02\n");
    local_con_data.lid = htons(res->port_attr.lid);
    memcpy(local_con_data.gid, &my_gid, 16);
    fprintf(stdout, "\nLocal LID = 0x%x\n", res->port_attr.lid);
    if (sock_sync_data_(res->sock, sizeof(struct cm_con_data_t), (char *)&local_con_data, (char *)&tmp_con_data) < 0)
    {
        fprintf(stderr, "failed to exchange connection data between sides\n");
        rc = 1;
//        goto connect_qp_exit;
    }
    printf("connect_qp 03\n");
    remote_con_data.addr = ntohll(tmp_con_data.addr);
    remote_con_data.rkey = ntohl(tmp_con_data.rkey);
    printf("connect_qp 04\n");
    for(int i=0; i<qpNum;i++)
    {

      remote_con_data.qp_num[i] = ntohl(tmp_con_data.qp_num[i]);
      printf("Remote QP number = 0x%x\n", remote_con_data.qp_num[i]);
    }
    printf("connect_qp 05\n");

    remote_con_data.lid = ntohs(tmp_con_data.lid);
    memcpy(remote_con_data.gid, tmp_con_data.gid, 16);
    /* save the remote side attributes, we will need it for the post SR */
    res->remote_props = remote_con_data;
    fprintf(stdout, "Remote address = 0x% PRIx64 \n", remote_con_data.addr);
    fprintf(stdout, "Remote rkey = 0x%x\n", remote_con_data.rkey);
//    fprintf(stdout, "Remote QP number = 0x%x\n", remote_con_data.qp_num);
    fprintf(stdout, "Remote LID = 0x%x\n", remote_con_data.lid);
    if (config_rdma.gid_idx >= 0)
    {
        uint8_t *p = remote_con_data.gid;
        fprintf(stdout, "Remote GID =%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n ",p[0],
                  p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10], p[11], p[12], p[13], p[14], p[15]);
    }


    //server side has two qps, 0 to receive, 1 to send.
    for(int i = 0;i< qpNum;i++)
    {
//        int j = 0;
//        j = (i + 1) % SERVER_QP_NUMBER;
//        if(config_rdma.server_name)
//        {
            connect_qp_once(res, i, i);
//        }
//        else
//        {
//            connect_qp_once(res, j, i);
//        }

    }

    /* sync to make sure that both sides are in states that they can connect to prevent packet loose */
    if (sock_sync_data_(res->sock, 1, "Q", &temp_char)) /* just send a dummy char back and forth */
    {
        fprintf(stderr, "sync error after QPs are were moved to RTS\n");
        rc = 1;
    }
    return rc;

}
/******************************************************************************
* Function: resources_destroy
*
* Input
* res pointer to resources structure
*
* Output
* none
*
* Returns
* 0 on success, 1 on failure
*
* Description
* Cleanup and deallocate all resources used
******************************************************************************/
int resources_destroy(struct resources *res)
{
    int rc = 0;
    for(int i = 0; i<qpNum; i++){
        if (res->qp[i])
            if (ibv_destroy_qp(res->qp[i]))
            {
                fprintf(stderr, "failed to destroy QP\n");
                rc = 1;
            }
    }
    if (res->mr)
        if (ibv_dereg_mr(res->mr))
        {
            fprintf(stderr, "failed to deregister MR\n");
            rc = 1;
        }
    if (res->buf)
        free(res->buf);
    for(int i = 0; i<qpNum; i++){
        if (res->cq[i])
            if (ibv_destroy_cq(res->cq[i]))
            {
                fprintf(stderr, "failed to destroy CQ\n");
                rc = 1;
            }
    }
    if (res->pd)
        if (ibv_dealloc_pd(res->pd))
        {
            fprintf(stderr, "failed to deallocate PD\n");
            rc = 1;
        }
    if (res->ib_ctx)
        if (ibv_close_device(res->ib_ctx))
        {
            fprintf(stderr, "failed to close device context\n");
            rc = 1;
        }
    if (res->sock >= 0)
        if (close(res->sock))
        {
            fprintf(stderr, "failed to close socket\n");
            rc = 1;
        }
    return rc;
}
/******************************************************************************
* Function: print_config_rdma
*
* Input
* none
*
* Output
* none
*
* Returns
* none
*
* Description
* Print out config_rdma information
******************************************************************************/
void print_config_rdma(void)
{
    fprintf(stdout, " ------------------------------------------------\n");
    fprintf(stdout, " Device name : \"%s\"\n", config_rdma.dev_name);
    fprintf(stdout, " IB port : %u\n", config_rdma.ib_port);
    if (config_rdma.server_name)
        fprintf(stdout, " IP : %s\n", config_rdma.server_name);
    fprintf(stdout, " TCP port : %u\n", config_rdma.tcp_port);
    if (config_rdma.gid_idx >= 0)
        fprintf(stdout, " GID index : %u\n", config_rdma.gid_idx);
    fprintf(stdout, " ------------------------------------------------\n\n");
}

/******************************************************************************
* Function: usage
*
* Input
* argv0 command line arguments
*
* Output
* none
*
* Returns
* none
*
* Description
* print a description of command line syntax
******************************************************************************/
void usage_(const char *argv0)
{
    fprintf(stdout, "Usage:\n");
    fprintf(stdout, " %s start a server and wait for connection\n", argv0);
    fprintf(stdout, " %s <host> connect to server at <host>\n", argv0);
    fprintf(stdout, "\n");
    fprintf(stdout, "Options:\n");
    fprintf(stdout, " -p, --port <port> listen on/connect to port <port> (default 18515)\n");
    fprintf(stdout, " -d, --ib-dev <dev> use IB device <dev> (default first device found)\n");
    fprintf(stdout, " -i, --ib-port <port> use port <port> of IB device (default 1)\n");
    fprintf(stdout, " -g, --gid_idx <git index> gid index to be used in GRH (default not used)\n");
}
/******************************************************************************
* Function: main
*
* Input
* argc number of items in argv
* argv command line parameters
*
* Output
* none
*
* Returns
* 0 on success, 1 on failure
*
* Description
* Main program code
//******************************************************************************/
//int main(int argc, char *argv[])
//{
//    struct resources res;
//    int rc = 1;
//    char temp_char;
//    /* parse the command line parameters */
//    while (1)
//    {
//        int c;
//        static struct option long_options[] = {
//            {.name = "port", .has_arg = 1, .val = 'p'},
//            {.name = "ib-dev", .has_arg = 1, .val = 'd'},
//            {.name = "ib-port", .has_arg = 1, .val = 'i'},
//            {.name = "gid-idx", .has_arg = 1, .val = 'g'},
//            {.name = NULL, .has_arg = 0, .val = '\0'}
//        };
//        c = getopt_long(argc, argv, "p:d:i:g:", long_options, NULL);
//        if (c == -1)
//            break;
//        switch (c)
//        {
//        case 'p':
//            config_rdma.tcp_port = strtoul(optarg, NULL, 0);
//            break;
//        case 'd':
//            config_rdma.dev_name = strdup(optarg);
//            break;
//        case 'i':
//            config_rdma.ib_port = strtoul(optarg, NULL, 0);
//            if (config_rdma.ib_port < 0)
//            {
//                usage_(argv[0]);
//                return 1;
//            }
//            break;
//        case 'g':
//            config_rdma.gid_idx = strtoul(optarg, NULL, 0);
//            if (config_rdma.gid_idx < 0)
//            {
//                usage(argv[0]);
//                return 1;
//            }
//            break;
//        default:
//            usage(argv[0]);
//            return 1;
//        }
//    }
//    /* parse the last parameter (if exists) as the server name */
//    if (optind == argc - 1)
//        config_rdma.server_name = argv[optind];
//    if(config_rdma.server_name){
//        printf("servername=%s\n",config_rdma.server_name);
//    }
//    else if (optind < argc)
//    {
//        usage_(argv[0]);
//        return 1;
//    }
//    /* print the used parameters for info*/
//    print_config_rdma();
//    /* init all of the resources, so cleanup will be easy */
//    resources_init(&res);
//    /* create resources before using them */
//    if (resources_create(&res))
//    {
//        fprintf(stderr, "failed to create resources\n");
//        goto main_exit;
//    }
//    /* connect the QPs */
//    if (connect_qp(&res))
//    {
//        fprintf(stderr, "failed to connect QPs\n");
//        goto main_exit;
//    }
//    /* let the server post the sr */
//    if (!config_rdma.server_name)
//        if (post_send(&res, IBV_WR_SEND))
//        {
//            fprintf(stderr, "failed to post sr\n");
//            goto main_exit;
//        }
//    /* in both sides we expect to get a completion */
//    if (poll_completion(&res))
//    {
//        fprintf(stderr, "poll completion failed\n");
//        goto main_exit;
//    }
//    /* after polling the completion we have the message in the client buffer too */
//    if (config_rdma.server_name)
//        fprintf(stdout, "Message is: '%s'\n", res.buf);
//    else
//    {
//        /* setup server buffer with read message */
//        strcpy(res.buf, RDMAMSGR);
//    }
//    /* Sync so we are sure server side has data ready before client tries to read it */
//    if (sock_sync_data_(res.sock, 1, "R", &temp_char)) /* just send a dummy char back and forth */
//    {
//        fprintf(stderr, "sync error before RDMA ops\n");
//        rc = 1;
//        goto main_exit;
//    }
//    /* Now the client performs an RDMA read and then write on server.
//Note that the server has no idea these events have occured */
//    if (config_rdma.server_name)
//    {
//        /* First we read contens of server's buffer */
//        if (post_send(&res, IBV_WR_RDMA_READ))
//        {
//            fprintf(stderr, "failed to post SR 2\n");
//            rc = 1;
//            goto main_exit;
//        }
//        if (poll_completion(&res))
//        {
//            fprintf(stderr, "poll completion failed 2\n");
//            rc = 1;
//            goto main_exit;
//        }
//        fprintf(stdout, "Contents of server's buffer: '%s'\n", res.buf);
//        /* Now we replace what's in the server's buffer */
//        strcpy(res.buf, RDMAMSGW);
//        fprintf(stdout, "Now replacing it with: '%s'\n", res.buf);
//        if (post_send(&res, IBV_WR_RDMA_WRITE))
//        {
//            fprintf(stderr, "failed to post SR 3\n");
//            rc = 1;
//            goto main_exit;
//        }
//        if (poll_completion(&res))
//        {
//            fprintf(stderr, "poll completion failed 3\n");
//            rc = 1;
//            goto main_exit;
//        }
//    }
//    /* Sync so server will know that client is done mucking with its memory */
//    if (sock_sync_data_(res.sock, 1, "W", &temp_char)) /* just send a dummy char back and forth */
//    {
//        fprintf(stderr, "sync error after RDMA ops\n");
//        rc = 1;
//        goto main_exit;
//    }
//    if (!config_rdma.server_name)
//        fprintf(stdout, "Contents of server buffer: '%s'\n", res.buf);
//    rc = 0;
//main_exit:
//    if (resources_destroy(&res))
//    {
//        fprintf(stderr, "failed to destroy resources\n");
//        rc = 1;
//    }
//    if (config_rdma.dev_name)
//        free((char *)config_rdma.dev_name);
//    fprintf(stdout, "\ntest result is %d\n", rc);
//    return rc;
//}
int find_local_qp_by_id(uint32_t qp_num){
    uint32_t temp = htonl(qp_num);
    for(int i =0 ;i < qpNum; i++)
    {
//        printf("%x,%x \n",ntohl(local_con_data.qp_num[i]),qp_num);
        if(local_con_data.qp_num[i] == temp)
            return i;
    }
    return -1;

}
