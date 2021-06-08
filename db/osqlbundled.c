#include <compile_time_assert.h>

#include "comdb2uuid.h"
#include "osqlcomm.h"
#include "osqlbundled.h"

static int bundle(osql_target_t *target, int usertype, void *data, int datalen,
                  int nodelay, void *tail, int tailen, int done, int unbundled);

int gbl_osql_max_bundled_bytes = 4 * 1024 * 1024 *1;

void init_bplog_bundled(osql_target_t *target)
{
    if (gbl_osql_max_bundled_bytes <= 0)
        return;

    /* Latch the original send routine. Replace it with our adaptor. */
    target->bundled.send = target->send;
    target->send = bundle;
}

struct osql_bundled {
    int nmsgs; /* number of messages in this bundle */
    int offset_done_snap; /* offset of OSQL_DONE_SNAP */
};

enum {
    OSQLCOMM_BUNDLED_TYPE_LEN = 8
};

BB_COMPILE_TIME_ASSERT(osqlcomm_bundled_type_len,
                       sizeof(struct osql_bundled) == OSQLCOMM_BUNDLED_TYPE_LEN);

static uint8_t *osqlcomm_bundled_type_put(const struct osql_bundled *bundled,
                                        uint8_t *p_buf,
                                        const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_BUNDLED_TYPE_LEN > p_buf_end - p_buf)
        return NULL;

    p_buf = buf_put(&(bundled->nmsgs),
                    sizeof(bundled->nmsgs), p_buf, p_buf_end);
    p_buf = buf_put(&(bundled->offset_done_snap),
                    sizeof(bundled->offset_done_snap), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *osqlcomm_bundled_type_get(struct osql_bundled *bundled,
                                              const uint8_t *p_buf,
                                              const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_BUNDLED_TYPE_LEN > p_buf_end - p_buf)
        return NULL;

    p_buf = buf_get(&(bundled->nmsgs),
                    sizeof(bundled->nmsgs), p_buf, p_buf_end);
    p_buf = buf_get(&(bundled->offset_done_snap),
                    sizeof(bundled->offset_done_snap), p_buf, p_buf_end);
    return p_buf;
}

struct osql_bundled_rpl {
    osql_rpl_t hd;
    struct osql_bundled dt;
};

enum {
    OSQLCOMM_BUNDLED_RPL_TYPE_LEN = OSQLCOMM_RPL_TYPE_LEN + OSQLCOMM_BUNDLED_TYPE_LEN
};

BB_COMPILE_TIME_ASSERT(osqlcomm_bundled_rpl_type_len,
                       sizeof(struct osql_bundled_rpl) == OSQLCOMM_BUNDLED_RPL_TYPE_LEN);

static uint8_t *
osqlcomm_bundled_rpl_type_put(const struct osql_bundled_rpl *bundled_rpl,
                            uint8_t *p_buf, uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_BUNDLED_RPL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_rpl_type_put(&(bundled_rpl->hd), p_buf, p_buf_end);
    p_buf = osqlcomm_bundled_type_put(&(bundled_rpl->dt), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *
osqlcomm_bundled_rpl_type_get(struct osql_bundled_rpl *bundled_rpl,
                            const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OSQLCOMM_BUNDLED_RPL_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_rpl_type_get(&(bundled_rpl->hd), p_buf, p_buf_end);
    p_buf = osqlcomm_bundled_type_get(&(bundled_rpl->dt), p_buf, p_buf_end);

    return p_buf;
}

struct osql_bundled_rpl_uuid {
    struct osql_rpl_uuid hd;
    struct osql_bundled dt;
};

enum {
    OSQLCOMM_BUNDLED_RPL_UUID_TYPE_LEN =
        OSQLCOMM_UUID_RPL_TYPE_LEN + OSQLCOMM_BUNDLED_TYPE_LEN
};

BB_COMPILE_TIME_ASSERT(osqlcomm_bundled_rpl_uuid_type_len,
                       sizeof(struct osql_bundled_rpl_uuid) ==
                           OSQLCOMM_BUNDLED_RPL_UUID_TYPE_LEN);

static uint8_t *osqlcomm_bundled_uuid_rpl_type_put(
    const struct osql_bundled_rpl_uuid *bundled_uuid_rpl, uint8_t *p_buf,
    uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        OSQLCOMM_BUNDLED_RPL_UUID_TYPE_LEN > (p_buf_end - p_buf)) {
        return NULL;
    }

    p_buf = osqlcomm_uuid_rpl_type_put(&(bundled_uuid_rpl->hd), p_buf,
                                       p_buf_end);
    p_buf =
        osqlcomm_bundled_type_put(&(bundled_uuid_rpl->dt), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *
osqlcomm_bundled_rpl_uuid_type_get(struct osql_bundled_rpl_uuid *bundled_uuid_rpl,
                                 const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        OSQLCOMM_BUNDLED_RPL_UUID_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = osqlcomm_uuid_rpl_type_get(&(bundled_uuid_rpl->hd), p_buf,
                                       p_buf_end);
    p_buf =
        osqlcomm_bundled_type_get(&(bundled_uuid_rpl->dt), p_buf, p_buf_end);

    return p_buf;
}



/* RIVERSTODO */
/* osql_bundled -> osql_bundle */
/* move osqlcomm_bundled_uuid_rpl_type_put to this file and consider git checkout -- on osqlcomm.c */

static int wrap_up(osql_target_t *target, int done, int nodelay, int offset_done_snap)
{
    int rc, type;
    int unused = 0;
    int hdrlen, bundlehdrlen, buflen;
    uint8_t *buf, *p_buf, *p_buf_end;

    struct osql_target_bundled *bundled = &target->bundled;
    unsigned long long rqid = bundled->rqid;

    enum OSQL_RPL_TYPE hdtype = done ? OSQL_DONE_BUNDLED : OSQL_BUNDLED;

    if (bundled->nmsgs == 0)
        return 0;

    if (rqid == OSQL_RQID_USE_UUID)
        hdrlen = OSQLCOMM_BUNDLED_RPL_UUID_TYPE_LEN;
    else
        hdrlen = OSQLCOMM_BUNDLED_RPL_TYPE_LEN;

    bundlehdrlen = sizeof(int) * bundled->nmsgs;
    buflen = hdrlen + bundlehdrlen;

    buf = malloc(buflen);
    if (buf == NULL) {
        logmsgperror("malloc");
        return errno;
    }

    p_buf = buf;
    p_buf_end = p_buf + buflen;
    type = bundled->send_type;

    memcpy(buf + hdrlen, bundled->hdr, bundlehdrlen);

    if (rqid == OSQL_RQID_USE_UUID) {
        struct osql_bundled_rpl_uuid rpl = {{0}};

        rpl.hd.type = hdtype;
        comdb2uuidcpy(rpl.hd.uuid, bundled->uuid);
        rpl.dt.nmsgs = bundled->nmsgs;
        rpl.dt.offset_done_snap = offset_done_snap;

        type = osql_net_type_to_net_uuid_type(type);
        if (!(p_buf = osqlcomm_bundled_uuid_rpl_type_put(&rpl, p_buf,
                        p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                    "osqlcomm_bundled_uuid_rpl_type_put");
            free(buf);
            return -1;
        }
    } else {
        struct osql_bundled_rpl rpl = {{0}};

        rpl.hd.type = hdtype;
        rpl.hd.sid = rqid;
        rpl.dt.nmsgs = bundled->nmsgs;

        if (!(p_buf = osqlcomm_bundled_rpl_type_put(&rpl, p_buf,
                        p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                    "osqlcomm_bundled_rpl_type_put");
            free(buf);
            return -1;
        }
    }

    if (gbl_enable_osql_logging) {
        uuidstr_t us;
        logmsg(LOGMSG_INFO, "[%llu %s] send %s\n", rqid,
                comdb2uuidstr(bundled->uuid, us), osql_reqtype_str(hdtype));
    }

    rc = bundled->send(target, type, buf, buflen, nodelay, bundled->buf, bundled->bufsz, unused, unused);

    if (rc == 0) {
        bundled->bufsz = 0;
        bundled->nmsgs = 0;
    }

    free(buf);
    return rc;
}

static int bundle(osql_target_t *target, int usertype, void *data, int datalen,
                  int nodelay, void *tail, int taillen,
                  int done, /* end of message */
                  int unbundled /* do not consolidate if 1 */)
{
    struct osql_target_bundled *bundled = &target->bundled;
    int rc = 0;
    int unused = 0;
    int size_new, size_min;
    int size_total = datalen + taillen;
    int offset_done_snap = -1;

    if (unbundled) {
        rc = wrap_up(target, 0, nodelay, offset_done_snap);
        if (rc != 0)
            return rc;
        return bundled->send(target, usertype, data, datalen, nodelay, tail, taillen, unused, unused);
    }

    if (bundled->send_type != usertype) { /* Messages of different user types can't be bundled */
        rc = wrap_up(target, 0, nodelay, offset_done_snap);
        if (rc != 0)
            return rc;
        bundled->send_type = usertype;
    }

    if (bundled->bufsz_alloc - bundled->bufsz < size_total) { /* Not enough space for payload */
        /* Minimal length required to hold all messages */
        size_min = bundled->bufsz + size_total;

        /* Attempt to grow the buffer exponentially. */
        size_new = size_min << 1;
        if (size_new > gbl_osql_max_bundled_bytes)
            size_new = gbl_osql_max_bundled_bytes;

        if (size_min >= size_new) { /* buffer is filled up */
            rc = wrap_up(target, 0, nodelay, offset_done_snap);
            if (rc != 0)
                return rc;
            return bundled->send(target, usertype, data, datalen, nodelay, tail, taillen, unused, unused);
        }

        if ((bundled->buf = realloc(bundled->buf, size_new)) == NULL) {
            logmsgperror("realloc");
            return errno;
        }
        bundled->bufsz_alloc = size_new;
    }

    if (bundled->nmsgs == bundled->nmsgs_alloc) { /* Not enough space for header */
        bundled->nmsgs_alloc = (bundled->nmsgs_alloc + 1) << 1;
        if ((bundled->hdr = realloc(bundled->hdr, sizeof(int) * bundled->nmsgs_alloc)) == NULL) {
            logmsgperror("realloc");
            return errno;
        }
    }

    bundled->hdr[bundled->nmsgs++] = htonl(size_total);
    memcpy(bundled->buf + bundled->bufsz, data, datalen);
    if (done > 1) /* A DONE_SNAP message */
        offset_done_snap = bundled->bufsz;
    bundled->bufsz += datalen;
    if (taillen > 0) {
        memcpy(bundled->buf + bundled->bufsz, tail, taillen);
        bundled->bufsz += taillen;
    }

    if (nodelay || done)
        rc = wrap_up(target, done, 1, offset_done_snap);

    return rc;
}

void osql_extract_snap_info(osql_sess_t *sess, void *rpl, int rpllen, int is_uuid);

void osql_extract_snap_info_from_bundle(osql_sess_t *sess, void *buf, int len, int is_uuid)
{
    const uint8_t *p_buf, *p_buf_end;
    int done_len;
    struct osql_bundled dt = {0};

    p_buf = (uint8_t *)buf;
    p_buf_end = p_buf + len;

    if (is_uuid) {
        osql_uuid_rpl_t rpl;
        p_buf = osqlcomm_uuid_rpl_type_get(&rpl, p_buf, p_buf_end);
    } else {
        osql_rpl_t rpl;
        p_buf = osqlcomm_rpl_type_get(&rpl, p_buf, p_buf_end);
    }

    p_buf_end = p_buf + sizeof(struct osql_bundled);

    (void)osqlcomm_bundled_type_get(&dt, p_buf, p_buf_end);

    if (dt.offset_done_snap >= 0) {
        p_buf = p_buf_end + (sizeof(int) * dt.nmsgs) + dt.offset_done_snap;
        done_len = (uint8_t *)buf + len - p_buf;
        osql_extract_snap_info(sess, (void *)p_buf, done_len, is_uuid);
    }
}

int osql_process_bundled(struct ireq *iq, unsigned long long rqid, uuid_t uuid,
                        void *trans, char *msg, int msglen, int *flags,
                        int **updCols, blob_buffer_t blobs[MAXBLOBS], int step,
                        struct block_err *err, int *receivedrows)
{
    const uint8_t *p_buf, *p_buf_end, *p_msgs_buf;
    int rc, i, nmsgs, *msglens, len, ofs, type = 0;
    void *a_msg;
    struct osql_bundled dt = {0};

    rc = 0;
    p_buf = (uint8_t *)msg;
    p_buf_end = p_buf + sizeof(struct osql_bundled);
    msglens = (int *)osqlcomm_bundled_type_get(&dt, p_buf, p_buf_end);
    nmsgs = dt.nmsgs;
    p_msgs_buf = p_buf_end + (sizeof(int) * nmsgs);

    for (i = 0, ofs = 0; i != nmsgs; ++i) {
        len = ntohl(msglens[i]);
        p_buf = p_msgs_buf + ofs;
        p_buf_end = p_buf + len;

        if (rqid == OSQL_RQID_USE_UUID) {
            osql_uuid_rpl_t rpl;
            osqlcomm_uuid_rpl_type_get(&rpl, p_buf, p_buf_end);
            type = rpl.type;
        } else {
            osql_rpl_t rpl;
            osqlcomm_rpl_type_get(&rpl, p_buf, p_buf_end);
            type = rpl.type;
        }

        if (type != OSQL_QBLOB) {
            a_msg = (void *)p_buf;
        } else {
            a_msg = malloc(len);
            if (a_msg == NULL) {
                logmsgperror("malloc");
                return errno;
            }
            memcpy(a_msg, p_buf, len);
        }

        /* RIVERSTODB : CAN WE PRE-PROCESS OPCODES HERE??? */
        switch (type) {
        case OSQL_USEDB:
        case OSQL_INSREC:
        case OSQL_INSERT:
        case OSQL_INSIDX:
        case OSQL_DELIDX:
        case OSQL_QBLOB:
        case OSQL_STARTGEN:
        case OSQL_BUNDLED:
        case OSQL_DONE_SNAP:
        case OSQL_DONE:
        case OSQL_DONE_WITH_EFFECTS:
        case OSQL_XERR:
            break;
        default:
            iq->sorese->is_delayed = true;
            break;
        }

        rc = osql_process_packet(iq, rqid, uuid, trans, (char **)&a_msg, len,
                flags, updCols, blobs, step, err, receivedrows);
        if (a_msg != p_buf)
            free(a_msg);
        if (rc != OSQL_RC_OK && rc != OSQL_RC_DONE)
            break;
        ofs += len;
    }
    return rc;
}

void copy_rqid(osql_target_t *target, unsigned long long rqid, uuid_t uuid)
{
    if (gbl_osql_max_bundled_bytes <= 0)
        return;
    target->bundled.rqid = rqid;
    comdb2uuidcpy(target->bundled.uuid, uuid);
}
