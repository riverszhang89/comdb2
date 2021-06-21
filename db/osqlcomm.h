/*
   Copyright 2015 Bloomberg Finance L.P.

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

#ifndef _OSQL_COMM_H_
#define _OSQL_COMM_H_

#include "comdb2.h"
#include "sbuf2.h"
#include "osqlsession.h"
#include "sqloffload.h"
#include "block_internal.h"
#include "comdb2uuid.h"

#define OSQL_BLOB_ODH_BIT (1 << 31)
#define IS_ODH_READY(x) (!!(((x)->odhind) & OSQL_BLOB_ODH_BIT))
#define OSQL_SEND_ERROR_WRONGMASTER (-1234)

/* common osql structures */
typedef struct osql_rpl {
    /* keep this header aligned head and tail! */
    enum OSQL_RPL_TYPE type;
    int padding;
    unsigned long long sid; /* concurrent access */
} osql_rpl_t;
enum { OSQLCOMM_RPL_TYPE_LEN = 4 + 4 + 8 };
uint8_t *osqlcomm_rpl_type_put(const osql_rpl_t *p_osql_rpl, uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *osqlcomm_rpl_type_get(osql_rpl_t *p_osql_rpl, const uint8_t *p_buf, const uint8_t *p_buf_end);

typedef struct osql_rpl_uuid {
    enum OSQL_RPL_TYPE type;
    int padding;
    uuid_t uuid;
} osql_uuid_rpl_t;
enum { OSQLCOMM_UUID_RPL_TYPE_LEN = 4 + 4 + 16 };
uint8_t *osqlcomm_uuid_rpl_type_put(const osql_uuid_rpl_t *p_osql_rpl, uint8_t *p_buf, const uint8_t *p_buf_end); 
const uint8_t *osqlcomm_uuid_rpl_type_get(osql_uuid_rpl_t *p_osql_rpl, const uint8_t *p_buf, const uint8_t *p_buf_end);

int osql_net_type_to_net_uuid_type(int type);

enum { OSQL_PROCESS_FLAGS_BLOB_OPTIMIZATION = 0x00000001, };

/**
 * Initializes this node for osql communication
 * Creates the offload net.
 * Returns 0 if success.
 *
 */
int osql_comm_init(struct dbenv *dbenv);

/**
 * Destroy osql endpoint.
 * No communication is possible after this.
 *
 */
void osql_comm_destroy(void);

/**
 * Sends a sosql request to the master
 * Sql is the first update part of this transaction
 *
 */
int osql_comm_send_socksqlreq(osqlstate_t *osql, const char *sql, int sqlen,
                              char *tzname, int type, int flags);

/**
 * Send USEDB op
 * It handles remote/local connectivity
 *
 */
int osql_send_usedb(osqlstate_t *osql, char *tablename, int type, unsigned long long version);

/**
 * Send INDEX op
 * It handles remote/local connectivity
 *
 */
int osql_send_index(osqlstate_t *osql,
                    unsigned long long genid, int isDelete, int ixnum,
                    char *pData, int nData, int type);

/**
 * Send QBLOB op
 * It handles remote/local connectivity
 *
 */
int osql_send_qblob(osqlstate_t *osql,
                    int blobid, unsigned long long seq, int type, char *data,
                    int datalen);

/**
 * Send UPDCOLS op
 * It handles remote/local connectivity
 *
 */
int osql_send_updcols(osqlstate_t *osql,
                      unsigned long long seq, int type, int *colList,
                      int ncols);

/**
 * Send UPDREC op
 * It handles remote/local connectivity
 *
 */
int osql_send_updrec(osqlstate_t *osql,
                     unsigned long long genid, unsigned long long ins_keys,
                     unsigned long long del_keys, char *pData, int nData,
                     int type);

/**
 * Send INSREC op
 * It handles remote/local connectivity
 *
 */
int osql_send_insrec(osqlstate_t *osql,
                     unsigned long long genid, unsigned long long dirty_keys,
                     char *pData, int nData, int type, int upsert_flags);

/**
 * Send DELREC op
 * It handles remote/local connectivity
 *
 */
int osql_send_delrec(osqlstate_t *osql, unsigned long long genid,
                     unsigned long long dirty_keys, int type);

/**
 * Send SCHEMACHANGE op
 * It handles remote/local connectivity
 *
 */
int osql_send_schemachange(osqlstate_t *osql,
                           struct schema_change_type *sc, int type);

/**
 * Send BPFUNC op
 * It handles remote/local connectivity
 *
 */
int osql_send_bpfunc(osqlstate_t *osql,
                     BpfuncArg *msg, int type);

/**
 * Send SERIAL op
 *
 */
int osql_send_serial(osqlstate_t *osql,
                     CurRangeArr *arr, unsigned int file, unsigned int offset,
                     int type);

/**
 * Send DONE or DONE_XERR op
 * It handles remote/local connectivity
 *
 */
int osql_send_commit(osqlstate_t *osql,
                     struct errstat *xerr, int type,
                     struct client_query_stats *query_stats,
                     snap_uid_t *snap_info);

int osql_send_commit_by_uuid(osqlstate_t *osql,
                             struct errstat *xerr, int type,
                             struct client_query_stats *query_stats,
                             snap_uid_t *snap_info);

/**
 * Extra commit info
 *
 */
int osql_send_startgen(osqlstate_t *osql,
                       uint32_t start_gen, int type);

/**
 * Consume
 *
 */
int osql_send_dbq_consume(osqlstate_t *osql,
                          genid_t, int type);

/**
 * Request that a remote sql engine start recording it's query stats to a
 * dbglog file.  This will later be slurped up & returned via an
 * FSQL_GRAB_DBGLOG request.
 *
 */
int osql_send_dbglog(osqlstate_t *osql,
                     unsigned long long dbglog_cookie, int queryid, int type);

/**
 * Send RECGENID
 * It handles remote/local connectivity
 *
 */
int osql_send_recordgenid(osqlstate_t *osql,
                          unsigned long long genid, int type);

/**
 * Update stats
 *
 */
int osql_send_updstat(osqlstate_t *osql,
                      unsigned long long seq, char *pData,
                      int nData, int nStat, int type);

/**
 * Sends the result of block processor transaction commit
 * to the sql thread so that it can return the result to the
 * client
 *
 */
int osql_comm_signal_sqlthr_rc(osql_target_t *target, unsigned long long rqid,
                               uuid_t uuid, int nops, struct errstat *xerr,
                               snap_uid_t *snap, int rc);
/**
 * if anything goes wrong during master bplog processing,
 * let replicant know (wrapper around signal_sqlthr_rc)
 *
 */
void signal_replicant_error(osql_target_t *target, unsigned long long rqid,
                            uuid_t uuid, int rc, const char *msg);

/**
 * If "rpl" is a done packet, set xerr to error if any and return 1
 * If "rpl" is a recognizable packet, returns the length of the data type is
 * recognized,
 * or -1 otherwise
 *
 */
int osql_comm_is_done(osql_sess_t *sess, int type, char *rpl, int rpllen,
                      int hasuuid, struct errstat **xerr,
                      struct query_effects *effects, int *preprocess_only);

/**
 * Handles each packet and calls record.c functions
 * to apply to received row updates
 *
 */
int osql_process_packet(struct ireq *iq, unsigned long long rqid, uuid_t uuid,
                        void *trans, char **pmsg, int msglen, int *flags,
                        int **updCols, blob_buffer_t blobs[MAXBLOBS], int step,
                        struct block_err *err, int *receivedrows);

/**
 * Handles each packet and start schema change
 *
 */
int osql_process_schemachange(struct ireq *iq, unsigned long long rqid,
                              uuid_t uuid, void *trans, char **pmsg, int msglen,
                              int *flags, int **updCols,
                              blob_buffer_t blobs[MAXBLOBS], int step,
                              struct block_err *err, int *receivedrows);


/**
 * Sends a user command to offload net (used by "osqlnet")
 *
 */
void osql_net_cmd(char *line, int lline, int st, int op1);

/**
 * Sets the osql net-poll value.
 *
 */
void osql_set_net_poll(int pval);

/**
 * Report on the traffic noticed
 *
 */
int osql_comm_quick_stat(void);

/**
 * Change the rqid and to allow reusing the request
 *
 */
void osql_remap_request(osql_req_t *req, unsigned long long rqid);

/**
 * Copy the big-endian errstat_t pointed to by p_buf into p_errstat_type
 * Exposed for osql_sess_set_complete
 *
 */
const uint8_t *osqlcomm_errstat_type_get(errstat_t *p_errstat_type,
                                         const uint8_t *p_buf,
                                         const uint8_t *p_buf_end);
/**
 * Copy the little-endian errstat_t pointed to by errstat_type into 
 * p_errstat_type.  Exposed for fstblk.
 *
 */
uint8_t *osqlcomm_errstat_type_put(const errstat_t *p_errstat_type,
                                          uint8_t *p_buf,
                                          const uint8_t *p_buf_end);

const uint8_t *osqlcomm_query_effects_get(struct query_effects *effects,
                                          const uint8_t *p_buf,
                                          const uint8_t *p_buf_end);
uint8_t *osqlcomm_query_effects_put(const struct query_effects *effects,
                                    uint8_t *p_buf, const uint8_t *p_buf_end);

/**
 * Copy and pack the host-ordered client_query_stats type into big-endian
 * format.  This routine only packs up to the path_stats component:  use
 * client_query_path_commponent_put to pack each of the path_stats
 *
 */
uint8_t *client_query_stats_put(const struct client_query_stats *p_stats,
                                uint8_t *p_buf, const uint8_t *p_buf_end);

/**
 * Test the net latency by sending a stream of packets to "tonode"
 * and waiting for them to be returned in the same order back
 * Displays per packet latencies
 *
 */
int osql_comm_echo(char *host, int stream, unsigned long long *sent,
                   unsigned long long *replied, unsigned long long *received);

/**
 * Signal net layer that the db is exiting
 *
 */
void osql_net_exiting(void);

/**
 * Enable a netinfo-test for the osqlcomm netinfo_ptr
 *
 */
int osql_enable_net_test(int testnum);

/**
 * Disable the netinfo-test for the osqlcomm netinfo_ptr
 *
 */
int osql_disable_net_test(void);

/**
 * Check if we need the bdb lock to stop long term sql sessions
 *
 */
int osql_comm_check_bdb_lock(const char *func, int line);

netinfo_type *osql_get_netinfo(void);

/**
 * Dumps diffs since last call to logger (called in statthd)
 *
 **/
int osql_comm_diffstat(struct reqlogger *statlogger, int *have_scon_header);

enum osqlpfrq_type {
    OSQLPFRQ_OLDDATA = 1, /* given a table, genid : fault the dta record */
    OSQLPFRQ_OLDKEY = 5,  /* given a table, key   : fault the ix record  */
    OSQLPFRQ_NEWKEY = 6,  /* given a table, key   : fault the ix record  */

    OSQLPFRQ_OLDDATA_OLDKEYS = 3, /* given a table, genid :
                                         1) fault the dta record.
                                         2) then form all keys, and
                                            enque OSQLPRFQ_KEY for each
                                  */
    OSQLPFRQ_NEWDATA_NEWKEYS = 2, /* given a table, record:
                                         1) fault the dta record.
                                         2) then form all keys, and
                                           enque OSQLPRFQ_KEY for each
                                  */
    OSQLPFRQ_OLDDATA_OLDKEYS_NEWKEYS =
        4, /* given a table,genid :
                  1) fault the dta record.
                  2) then form all keys from found record
                     and enque OSQLPRFQ_KEY for each
                  3) form new record based on found
                     record + input record and
                  4) form all keys from new record and
                     enque OSQLPRFQ_KEY for each
           */

    OSQLPFRQ_EXITTHD = 7,
    OSQLPFRQ_OSQLREQ = 99
};

int osql_page_prefault(char *rpl, int rplen, struct dbtable **last_db,
                       int **iq_step_ix, unsigned long long rqid, uuid_t uuid,
                       unsigned long long seq);

int osql_set_usedb(struct ireq *iq, const char *tablename, int tableversion,
                   int step, struct block_err *err);

int osql_send_del_qdb_logic(struct sqlclntstate *, char *, genid_t);

/**
 * Send a "POKE" message to "tonode" inquering about session "rqid"
 *
 */
int osql_comm_send_poke(const char *tonode, unsigned long long rqid,
                        uuid_t uuid, int type);

/**
 * Send decomission for osql net
 *
 */
int osql_process_message_decom(char *host);

/**
 * Simple ping-pong write on the master; used by:
 *   - forward-to-master block requests over socket
 *   - upgrade records
 *  And wait for reply inline.
 */
int offload_comm_send_blockreq(char *host, void *rqid, void *buf, int buflen);

/* Reply to offload block request. */
int offload_comm_send_blockreply(char *host, unsigned long long rqid, void *buf,
                                 int buflen, int rc);

/* Send a message over net to "host" */
int offload_net_send(const char *host, int usertype, void *data, int datalen,
                     int nodelay, void *tail, int tailen);

/**
 * Copy and pack the host-ordered client_query_stats type into big-endian
 * format.  This routine only packs up to the path_stats component:  use
 * client_query_path_commponent_put to pack each of the path_stats
 *
 */
uint8_t *client_query_stats_put(const struct client_query_stats *p_stats,
                                uint8_t *p_buf, const uint8_t *p_buf_end);

/**
 * Read a commit (DONE/XERR) from a socket, used in bplog over socket
 * Timeoutms limits total amount of waiting for a commit
 *
 */
int osql_recv_commit_rc(SBUF2 *sb, int timeoutms, int timeoutdeltams, int *nops,
                        struct errstat *err);

/**
 * Read the bplog request, coming from a socket
 *
 */
int osqlcomm_req_socket(SBUF2 *sb, char **sql, char tzname[DB_MAX_TZNAMEDB],
                        int *type, uuid_t uuid, int *flags);

/**
 * Read the bplog body, coming from a socket
 *
 */
int osqlcomm_bplog_socket(SBUF2 *sb, osql_sess_t *sess);

#endif
