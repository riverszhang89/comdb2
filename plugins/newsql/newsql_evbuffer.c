/*
   Copyright 2021 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <event2/buffer.h>
#include <event2/event.h>

#include <akbuf.h>
#include <bdb_api.h>
#include <hostname_support.h>
#include <intern_strings.h>
#include <net_appsock.h>
#include <net_int.h>
#include <rtcpu.h>
#include <sql.h>
#include <sqlwriter.h>
#include <str0.h>

#include <newsql.h>

#if WITH_SSL
#include <ssl_support.h>
#include <ssl_io.h>
#ifdef my_ssl_println
#undef my_ssl_println
#endif
#ifdef my_ssl_eprintln
#undef my_ssl_eprintln
#endif
#define my_ssl_println(fmt, ...) ssl_println("LIBEVENT-IO", fmt, ##__VA_ARGS__)
#define my_ssl_eprintln(fmt, ...)                                              \
    ssl_eprintln("LIBEVENT-IO", "%s: " fmt, __func__, ##__VA_ARGS__)
#endif

static void rd_hdr(int, short, void *);

struct newsql_appdata_evbuffer {
    NEWSQL_APPDATA_COMMON /* Must be first */

    int fd;
    struct sqlclntstate clnt;
    struct newsqlheader hdr;
    struct event *ping_ev;
    int ping_status;

#if WITH_SSL
    sslio *ssl;
#endif

    struct evbuffer *rd_buf;
    struct event *rd_hdr_ev;
    struct event *rd_payload_ev;
    unsigned active : 1; /* We count against MAXAPPSOCKSLIMIT */
    unsigned initial : 1; /* New connection or called newsql_reset */
    unsigned local : 1;

    struct sqlwriter *writer;
};

static void free_newsql_appdata_evbuffer(int dummy_fd, short what, void *arg)
{
    struct newsql_appdata_evbuffer *appdata = arg;
    struct sqlclntstate *clnt = &appdata->clnt;
    if (appdata->ping_ev) {
        event_free(appdata->ping_ev);
        appdata->ping_ev = NULL;
    }
    if (appdata->rd_hdr_ev) {
        event_free(appdata->rd_hdr_ev);
        appdata->rd_hdr_ev = NULL;
    }
    if (appdata->rd_payload_ev) {
        event_free(appdata->rd_payload_ev);
        appdata->rd_payload_ev = NULL;
    }
    if (appdata->rd_buf) {
        evbuffer_free(appdata->rd_buf);
        appdata->rd_buf = NULL;
    }
    sqlwriter_free(appdata->writer);
    shutdown(appdata->fd, SHUT_RDWR);
    close(appdata->fd);
    rem_lru_evbuffer(clnt);
    if (appdata->active) {
        rem_appsock_connection_evbuffer(clnt);
    }
    free_newsql_appdata(clnt);
}

static void newsql_cleanup(int fd, short what, void *arg)
{
    check_appsock_timer_thd();
    struct newsql_appdata_evbuffer *appdata = arg;
    sql_disable_heartbeat(appdata->writer);
    sql_disable_timeout(appdata->writer);
    event_once(appsock_rd_base, free_newsql_appdata_evbuffer, appdata);
}

static int newsql_flush_evbuffer(struct sqlclntstate *clnt)
{
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    return sql_flush(appdata->writer);
}

static void newsql_read_hdr(int fd, short what, void *arg)
{
    struct newsql_appdata_evbuffer *appdata = arg;
    add_lru_evbuffer(&appdata->clnt);
    rd_hdr(-1, 0, appdata);
}

static void newsql_read_again(int fd, short what, void *arg)
{
    struct newsql_appdata_evbuffer *appdata = arg;
    sql_disable_heartbeat(appdata->writer);
    sql_disable_timeout(appdata->writer);
    struct sqlclntstate *clnt = &appdata->clnt;
    if (clnt->query_rc) {
        if (in_client_trans(clnt)) {
            clnt->had_errors = 1;
        } else {
            /* newsql over sbuf and fastsql drop the connection in this
             * situation - we can do better; connections are expensive */
            reset_clnt_flags(clnt);
        }
    }
    event_once(appsock_rd_base, newsql_read_hdr, appdata);
}

static void newsql_reset_evbuffer(struct newsql_appdata_evbuffer *appdata)
{
    appdata->initial = 1;
    newsql_reset(&appdata->clnt);
}

static int newsql_done_cb(struct sqlclntstate *clnt)
{
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    if (clnt->query_rc == CDB2ERR_IO_ERROR) { /* dispatch timed out */
        return event_once(appsock_timer_base, newsql_cleanup, appdata);
    }
    if (clnt->osql.replay == OSQL_RETRY_DO) {
        clnt->done_cb = NULL;
        srs_tran_replay_inline(clnt);
        clnt->done_cb = newsql_done_cb;
    } else if (clnt->osql.history && clnt->ctrl_sqlengine == SQLENG_NORMAL_PROCESS) {
        srs_tran_destroy(clnt);
    } else if (appdata->query) {
        cdb2__query__free_unpacked(appdata->query, NULL);
        appdata->query = NULL;
    }
    if (sql_done(appdata->writer) == 0) {
        event_once(appsock_timer_base, newsql_read_again, appdata);
    } else {
        event_once(appsock_timer_base, newsql_cleanup, appdata);
    }
    return 0;
}

static int newsql_get_fileno_evbuffer(struct sqlclntstate *clnt)
{
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    return appdata->fd;
}

static int newsql_get_x509_attr_evbuffer(struct sqlclntstate *clnt, int nid, void *out, int outsz)
{
#   if WITH_SSL
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    return sslio_x509_attr(appdata->ssl, nid, out, outsz);
#   else
    return 0;
#   endif
}

static int newsql_has_ssl_evbuffer(struct sqlclntstate *clnt)
{
#   if WITH_SSL
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    return sslio_has_ssl(appdata->ssl);
#   else
    return 0;
#   endif
}

static int newsql_has_x509_evbuffer(struct sqlclntstate *clnt)
{
#   if WITH_SSL
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    return sslio_has_x509(appdata->ssl);
#   else
    return 0;
#   endif
}

static int newsql_local_check_evbuffer(struct sqlclntstate *clnt)
{
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    return appdata->local;
}

static int newsql_peer_check_evbuffer(struct sqlclntstate *clnt)
{
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    return sql_peer_check(appdata->writer);
}

static int newsql_set_timeout_evbuffer(struct sqlclntstate *clnt, int timeout_ms)
{
    /* nop */
    return 0;
}

static void pong(int fd, short what, void *arg)
{
    struct newsql_appdata_evbuffer *appdata = arg;
    struct event_base *wrbase = sql_wrbase(appdata->writer);
    if (what & EV_TIMEOUT) {
        event_base_loopbreak(wrbase);
        return;
    }
    if (evbuffer_read(appdata->rd_buf, appdata->fd, -1) <= 0) {
        appdata->ping_status = -2;
        event_base_loopbreak(wrbase);
        return;
    }
    struct newsqlheader hdr;
    if (evbuffer_get_length(appdata->rd_buf) < sizeof(hdr)) {
        return;
    }
    evbuffer_remove(appdata->rd_buf, &hdr, sizeof(hdr));
    if (ntohl(hdr.type) == RESPONSE_HEADER__SQL_RESPONSE_PONG) {
        appdata->ping_status = 0;
    } else {
        appdata->ping_status = -3;
    }
    event_base_loopbreak(wrbase);
}

static int newsql_ping_pong_evbuffer(struct sqlclntstate *clnt)
{
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    struct event_base *wrbase = sql_wrbase(appdata->writer);
    if (!appdata->ping_ev) {
        int flags = EV_READ | EV_PERSIST | EV_TIMEOUT;
        appdata->ping_ev = event_new(wrbase, appdata->fd, flags, pong, appdata);
    }
    appdata->ping_status = -1;
    struct timeval onesec = {.tv_sec = 1};
    event_add(appdata->ping_ev, &onesec);
    event_base_dispatch(wrbase);
    event_del(appdata->ping_ev);
    return appdata->ping_status;
}

static void write_dbinfo(int fd, short what, void *arg)
{
    check_appsock_timer_thd();
    struct newsql_appdata_evbuffer *appdata = arg;
    struct evbuffer *wrbuf = sql_wrbuf(appdata->writer);
    if (evbuffer_write(wrbuf, appdata->fd) <= 0) {
        newsql_cleanup(-1, 0, appdata);
        return;
    }
    if (evbuffer_get_length(wrbuf)) {
        event_base_once(appsock_timer_base, appdata->fd, EV_WRITE, write_dbinfo, appdata, NULL);
    } else {
        event_once(appsock_rd_base, rd_hdr, appdata);
    }
}

static void process_dbinfo(struct newsql_appdata_evbuffer *appdata, CDB2QUERY *query)
{
    CDB2DBINFORESPONSE__Nodeinfo *master = NULL;
    CDB2DBINFORESPONSE__Nodeinfo *nodes[REPMAX];
    CDB2DBINFORESPONSE__Nodeinfo same_dc[REPMAX], diff_dc[REPMAX];
    int num_same_dc = 0, num_diff_dc = 0;
    host_node_type *hosts[REPMAX];
    int num_hosts = get_hosts_evbuffer(REPMAX, hosts);
    int my_dc = machine_dc(gbl_myhostname);
    int process_incoherent = bdb_amimaster(thedb->bdb_env);
    for (int i = 0; i < num_hosts; ++i) {
        CDB2DBINFORESPONSE__Nodeinfo *node;
        int dc = machine_dc(hosts[i]->host);
        node = (dc == my_dc) ?  &same_dc[num_same_dc++] : &diff_dc[num_diff_dc++];
        cdb2__dbinforesponse__nodeinfo__init(node);
        node->has_room = 1;
        node->room = dc;
        node->has_port = 1;
        node->port = hosts[i]->port;
        node->name = hosts[i]->host;
        node->incoherent = process_incoherent ? is_incoherent(thedb->bdb_env, node->name) : 0;
        const char *who = bdb_whoismaster(thedb->bdb_env);
        if (who && strcmp(who, node->name) == 0) {
            master = node;
        }
    }
    int j = 0;
    for (int i = 0; i < num_same_dc; ++i, ++j) {
        nodes[j] = &same_dc[i];
        nodes[j]->number = j;
    }
    for (int i = 0; i < num_diff_dc; ++i, ++j) {
        nodes[j] = &diff_dc[i];
        nodes[j]->number = j;
    }

    // TODO: fill_sslinfo
    CDB2DBINFORESPONSE response = CDB2__DBINFORESPONSE__INIT;
    response.n_nodes = num_hosts;
    response.master = master;
    response.nodes = nodes;

    int len = cdb2__dbinforesponse__get_packed_size(&response);
    size_t sz = sizeof(struct newsqlheader) + len;
    struct iovec v[1];
    struct evbuffer *wrbuf = sql_wrbuf(appdata->writer);
    evbuffer_reserve_space(wrbuf, sz, v, 1);
    uint8_t *b = v[0].iov_base;
    struct newsqlheader *hdr = (struct newsqlheader *)b;
    hdr->type = htonl(RESPONSE_HEADER__DBINFO_RESPONSE);
    hdr->length = htonl(len);
    b += sizeof(struct newsqlheader);
    cdb2__dbinforesponse__pack(&response, b);
    v[0].iov_len = sz;
    evbuffer_commit_space(wrbuf, v, 1);
    event_base_once(appsock_timer_base, appdata->fd, EV_WRITE, write_dbinfo, appdata, NULL);

    if (query) {
        cdb2__query__free_unpacked(query, NULL);
    }
}

static void process_get_effects(struct newsql_appdata_evbuffer *appdata, CDB2QUERY *query)
{
    CDB2EFFECTS effects = CDB2__EFFECTS__INIT;
    CDB2SQLRESPONSE response = CDB2__SQLRESPONSE__INIT;
    newsql_effects(&response, &effects, &appdata->clnt);

    int len = cdb2__sqlresponse__get_packed_size(&response);
    size_t sz = sizeof(struct newsqlheader) + len;
    struct iovec v[1];
    struct evbuffer *wrbuf = sql_wrbuf(appdata->writer);
    evbuffer_reserve_space(wrbuf, sz, v, 1);
    uint8_t *b = v[0].iov_base;
    struct newsqlheader *hdr = (struct newsqlheader *)b;
    hdr->type = htonl(RESPONSE_HEADER__SQL_EFFECTS);
    hdr->length = htonl(len);
    b += sizeof(struct newsqlheader);
    cdb2__sqlresponse__pack(&response, b);
    v[0].iov_len = sz;
    evbuffer_commit_space(wrbuf, v, 1);
    event_base_once(appsock_timer_base, appdata->fd, EV_WRITE, write_dbinfo, appdata, NULL);
    cdb2__query__free_unpacked(query, NULL);
}

static void process_query(struct newsql_appdata_evbuffer *appdata, CDB2QUERY *query)
{
    int do_read = 0;
    int commit_rollback;
    appdata->query = query;
    appdata->sqlquery = query->sqlquery;
    struct sqlclntstate *clnt = &appdata->clnt;
    if (!appdata->active) {
        if (add_appsock_connection_evbuffer(clnt) != 0) {
            add_lru_evbuffer(clnt);
            exhausted_appsock_connections(clnt);
            goto out;
        }
        appdata->active = 1;
    }
    if (appdata->initial) {
        if (newsql_first_run(clnt, query->sqlquery) != 0) {
            goto out;
        }
        appdata->initial = 0;
    }
    if (newsql_loop(clnt, query->sqlquery) != 0) {
        goto out;
    }
    if (newsql_should_dispatch(clnt, &commit_rollback) != 0) {
        do_read = 1;
        goto out;
    }
    sql_reset(appdata->writer);
    if (clnt->query_timeout) {
        sql_enable_timeout(appdata->writer, clnt->query_timeout);
    }
    if (dispatch_sql_query_no_wait(clnt) == 0) {
        sql_enable_heartbeat(appdata->writer);
        return;
    }
out:cdb2__query__free_unpacked(query, NULL);
    event_once(appsock_timer_base, do_read ? newsql_read_again : newsql_cleanup, appdata);
}

static void process_cdb2query(struct newsql_appdata_evbuffer *appdata, CDB2QUERY *query)
{
    rem_lru_evbuffer(&appdata->clnt);
    CDB2DBINFO *dbinfo = query->dbinfo;
    if (!dbinfo) {
        process_query(appdata, query);
    } else if (dbinfo->has_want_effects && dbinfo->want_effects) {
        process_get_effects(appdata, query);
    } else {
        process_dbinfo(appdata, query);
    }
}

static void write_ssl_ability(int fd, short what, void *arg)
{
    check_appsock_timer_thd();
    struct newsql_appdata_evbuffer *appdata = arg;
    struct evbuffer *wrbuf = sql_wrbuf(appdata->writer);
    if (evbuffer_write(wrbuf, fd) <= 0) {
        newsql_cleanup(-1, 0, appdata);
        return;
    }
    if (evbuffer_get_length(wrbuf) != 0) {
        event_base_once(appsock_timer_base, appdata->fd, EV_WRITE, write_ssl_ability, appdata, NULL);
        return;
    }
#if WITH_SSL
    int sslrc = sslio_accept(&appdata->ssl, gbl_ssl_ctx, fd, gbl_client_ssl_mode, gbl_dbname, gbl_nid_dbname, 0);
    if (sslrc == 1) {
        /* Success! Extract the user from the certificate. */
        ssl_set_clnt_user(&appdata->clnt);
        event_once(appsock_rd_base, rd_hdr, appdata);
    } else {
        if (appdata->ssl == NULL) {
            write_response(&appdata->clnt, RESPONSE_ERROR, "Server out of memory", CDB2ERR_CONNECT_ERROR);
            logmsgperror("Could not allocate SSL structure");
        } else {
            write_response(&appdata->clnt, RESPONSE_ERROR, "Client certificate authentication failed.", CDB2ERR_CONNECT_ERROR);
            char err[256];
            sslio_get_error(appdata->ssl, err, sizeof(err));
            logmsg(LOGMSG_ERROR, "%s\n", err);
            sslio_close(appdata->ssl, 1);
            appdata->ssl = NULL;
        }
    }
#else
    /* Do not clean up yet. Client may downgrade to non-SSL. */
#endif
}

static void process_sslconn(struct newsql_appdata_evbuffer *appdata)
{
    char *ssl_ability;
#if WITH_SSL
    if (sslio_has_ssl(appdata->ssl)) {
        logmsg(LOGMSG_WARN, "The connection is already SSL encrypted.\n");
        return;
    }
    ssl_ability = "Y";
#else
    ssl_ability = "N";
#endif
    if (evbuffer_add(sql_wrbuf(appdata->writer), ssl_ability, 1) == 0)
        event_base_once(appsock_timer_base, appdata->fd, EV_WRITE, write_ssl_ability, appdata, NULL);
}

static void process_newsql_payload(struct newsql_appdata_evbuffer *appdata, CDB2QUERY *query)
{
    switch (appdata->hdr.type) {
    case CDB2_REQUEST_TYPE__CDB2QUERY:
        process_cdb2query(appdata, query);
        break;
    case CDB2_REQUEST_TYPE__RESET:
        newsql_reset_evbuffer(appdata);
        rd_hdr(appdata->fd, 0, appdata);
        break;
    case CDB2_REQUEST_TYPE__SSLCONN:
        process_sslconn(appdata);
#if 0
        gbl_libevent_appsock = 0;
        event_once(appsock_timer_base, newsql_cleanup, appdata);
#endif
        break;
    default:
        logmsg(LOGMSG_ERROR, "%s bad type:%d fd:%d\n", __func__, appdata->hdr.type, appdata->fd);
        abort();
    }
}

#if WITH_SSL
static int ssl_evbuffer_read(struct newsql_appdata_evbuffer *appdata, size_t sz)
{
    if (!sslio_has_ssl(appdata->ssl)) {
        if (evbuffer_read(appdata->rd_buf, appdata->fd, -1) <= 0)
            goto error_out;
    } else {
        struct iovec v[1];
        int nremain = sz - evbuffer_get_length(appdata->rd_buf);
        if (evbuffer_reserve_space(appdata->rd_buf, nremain, v, 1) <= 0)
            goto error_out;
        int nr = sslio_read_no_retry(appdata->ssl, v[0].iov_base, nremain);
        if (nr <= 0) {
            char err[256];
            int sslerr = sslio_get_error(appdata->ssl, err, sizeof(err));
            puts(err);
            if (sslerr) /* protocol error. bail out. */
                goto error_out;
        } else {
            printf("nr %d\n", nr);
            v[0].iov_len = nr;
            if (evbuffer_commit_space(appdata->rd_buf, v, 1) < 0)
                goto error_out;
        }
    }
    return 0;
error_out:
    event_once(appsock_timer_base, newsql_cleanup, appdata);
    return -1;
}
#endif

static void rd_payload(int fd, short what, void *arg)
{
    struct newsql_appdata_evbuffer *appdata = arg;
    if (what & EV_READ) {
#if WITH_SSL
        if (ssl_evbuffer_read(appdata, appdata->hdr.length) != 0)
            return;
#else
        if (evbuffer_read(appdata->rd_buf, appdata->fd, -1) <= 0) {
            event_once(appsock_timer_base, newsql_cleanup, appdata);
            return;
        }
#endif
    }
    if (evbuffer_get_length(appdata->rd_buf) < appdata->hdr.length) {
        event_add(appdata->rd_payload_ev, NULL);
        return;
    }
    CDB2QUERY *query = NULL;
    int len = appdata->hdr.length;
    if (len) {
        void *data = evbuffer_pullup(appdata->rd_buf, len);
        if ((query = cdb2__query__unpack(NULL, len, data)) == NULL) {
            event_once(appsock_timer_base, newsql_cleanup, appdata);
            return;
        }
        evbuffer_drain(appdata->rd_buf, len);
    }
    process_newsql_payload(appdata, query);
}

static void rd_hdr(int fd, short what, void *arg)
{
    check_appsock_rd_thd();
    struct newsql_appdata_evbuffer *appdata = arg;
    if (what & EV_READ) {
#if WITH_SSL
        if (ssl_evbuffer_read(appdata, sizeof(struct newsqlheader)) != 0)
            return;
#else
        if (evbuffer_read(appdata->rd_buf, appdata->fd, -1) <= 0) {
            event_once(appsock_timer_base, newsql_cleanup, appdata);
            return;
        }
#endif
    }
    size_t len = evbuffer_get_length(appdata->rd_buf);
    if (len < sizeof(struct newsqlheader)) {
        event_add(appdata->rd_hdr_ev, NULL);
        return;
    }
    evbuffer_remove(appdata->rd_buf, &appdata->hdr, sizeof(struct newsqlheader));
    appdata->hdr.type = ntohl(appdata->hdr.type);
    appdata->hdr.compression = ntohl(appdata->hdr.compression);
    appdata->hdr.state = ntohl(appdata->hdr.state);
    appdata->hdr.length = ntohl(appdata->hdr.length);
    rd_payload(appdata->fd, 0, appdata);
}

static void *newsql_destroy_stmt_evbuffer(struct sqlclntstate *clnt, void *arg)
{
    struct newsql_stmt *stmt = arg;
    cdb2__query__free_unpacked(stmt->query, NULL);
    free(stmt);
    return NULL;
}

static int newsql_close_evbuffer(struct sqlclntstate *clnt)
{
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    return shutdown(appdata->fd, SHUT_RDWR);
}

struct debug_cmd {
    struct event_base *base;
    struct evbuffer *buf;
    int need;
};

static void debug_cmd(int fd, short what, void *arg)
{
    struct debug_cmd *cmd = arg;
    if ((what & EV_READ) == 0 ||
        evbuffer_read(cmd->buf, fd, cmd->need) <= 0 ||
        evbuffer_get_length(cmd->buf) == cmd->need
    ){
        event_base_loopbreak(cmd->base);
    }
}

/* read interactive cmds for debugging a stored procedure */
static int newsql_read_evbuffer(struct sqlclntstate *clnt, void *b, int l, int n)
{
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    struct event_base *wrbase = sql_wrbase(appdata->writer);
    struct debug_cmd cmd;
    cmd.buf = evbuffer_new();
    cmd.need = l * n;
    cmd.base = wrbase;
    struct event *ev = event_new(wrbase, appdata->fd, EV_READ | EV_PERSIST, debug_cmd, &cmd);
    event_add(ev, NULL);
    event_base_dispatch(wrbase);
    int have = evbuffer_get_length(cmd.buf);
    evbuffer_copyout(cmd.buf, b, -1);
    evbuffer_free(cmd.buf);
    event_free(ev);
    return have / l;
}

static int newsql_pack_hb(uint8_t *out, void *arg)
{
    struct sqlclntstate *clnt = arg;
    int state;
    if (is_pingpong(clnt))
        state = 1;
    else {
        state = (clnt->sqltick > clnt->sqltick_last_seen);
        clnt->sqltick_last_seen = clnt->sqltick;
    }
    struct newsqlheader *h = (struct newsqlheader *)out;
    memset(h, 0, sizeof(struct newsqlheader));
    h->type = htonl(RESPONSE_HEADER__SQL_RESPONSE_HEARTBEAT);
    h->state = htonl(state);
    return 0;
}

struct newsql_pack_arg {
    struct newsqlheader *hdr;
    const CDB2SQLRESPONSE *resp;
};

static int newsql_pack(uint8_t *out, void *data)
{
    struct newsql_pack_arg *arg = data;
    if (arg->hdr) {
        memcpy(out, arg->hdr, sizeof(struct newsqlheader));
        out += sizeof(struct newsqlheader);
    }
    if (arg->resp) {
        cdb2__sqlresponse__pack(arg->resp, out);
        if (arg->resp->response_type == RESPONSE_TYPE__LAST_ROW) {
            return 1;
        }
    }
    return 0;
}

static int newsql_write_evbuffer(struct sqlclntstate *clnt, int type, int state,
                                 const CDB2SQLRESPONSE *resp, int flush)
{
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    int hdr_len = type ? sizeof(struct newsqlheader) : 0;
    int response_len = resp ? cdb2__sqlresponse__get_packed_size(resp) : 0;
    int total_len = hdr_len + response_len;
    struct newsql_pack_arg arg = {0};
    struct newsqlheader hdr;
    if (type) {
        hdr.type = htonl(type);
        hdr.compression = 0;
        hdr.state = htonl(state);
        hdr.length = htonl(response_len);
        arg.hdr = &hdr;
    }
    arg.resp = resp;
    return sql_write(appdata->writer, total_len, &arg, flush);

}

static int newsql_write_hdr_evbuffer(struct sqlclntstate *clnt, int h, int state)
{
    return newsql_write_evbuffer(clnt, h, state, 0, 1);
}

static int newsql_write_postponed_evbuffer(struct sqlclntstate *clnt)
{
    struct newsql_appdata_evbuffer *appdata = clnt->appdata;
    struct iovec v[2];

    v[0].iov_base = (char *)&appdata->postponed->hdr;
    v[0].iov_len = sizeof(struct newsqlheader);

    v[1].iov_base = (char *)appdata->postponed->row;
    v[1].iov_len = appdata->postponed->len;

    return sql_writev(appdata->writer, v, 2);
}

static int newsql_write_dbinfo_evbuffer(struct sqlclntstate *clnt)
{
    process_dbinfo(clnt->appdata, NULL);
    return 0;
}

static void newsql_setup_clnt_evbuffer(struct appsock_handler_arg *arg, int admin)
{
    check_appsock_rd_thd();

    int local = 0;
    if (arg->addr.sin_addr.s_addr == gbl_myaddr.s_addr) {
        local = 1;
    } else if (arg->addr.sin_addr.s_addr == htonl(INADDR_LOOPBACK)) {
        local = 1;
    }

    if (thedb->no_more_sql_connections || (admin && !local)) {
        evbuffer_free(arg->rd_buf);
        shutdown(arg->fd, SHUT_RDWR);
        close(arg->fd);
        return;
    }

    struct newsql_appdata_evbuffer *appdata = calloc(1, sizeof(*appdata));
    struct sqlclntstate *clnt = &appdata->clnt;

    reset_clnt(clnt, 1);
    char *origin = get_hostname_by_fileno(arg->fd);
    clnt->origin = origin ? origin : intern("???");
    clnt->appdata = appdata;
    clnt->done_cb = newsql_done_cb;

    newsql_setup_clnt(clnt);
    plugin_set_callbacks_newsql(evbuffer);

    clnt->admin = admin;
    appdata->local = local;

    appdata->initial = 1;
    appdata->fd = arg->fd;
    appdata->rd_buf = arg->rd_buf;
    appdata->rd_hdr_ev = event_new(appsock_rd_base, arg->fd, EV_READ, rd_hdr, appdata);
    appdata->rd_payload_ev = event_new(appsock_rd_base, arg->fd, EV_READ, rd_payload, appdata);

    struct sqlwriter_arg sqlwriter_arg = {
        .fd = arg->fd,
        .clnt = &appdata->clnt,
        .pack = newsql_pack,
        .pack_hb = newsql_pack_hb,
        .hb_sz = sizeof(struct newsqlheader),
    };
    appdata->writer = sqlwriter_new(&sqlwriter_arg);
    newsql_read_hdr(-1, 0, appdata);
}

static void handle_newsql_request_evbuffer(int fd, short what, void *data)
{
    newsql_setup_clnt_evbuffer(data, 0);
    free(data);
}

static void handle_newsql_admin_request_evbuffer(int fd, short what, void *data)
{
    newsql_setup_clnt_evbuffer(data, 1);
    free(data);
}

void setup_newsql_evbuffer_handlers(void)
{
    add_appsock_handler("newsql\n", handle_newsql_request_evbuffer);
    add_appsock_handler("@newsql\n", handle_newsql_admin_request_evbuffer);
}
