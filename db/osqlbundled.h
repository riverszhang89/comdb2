#ifndef INCLUDED_OSQLBUNDLED_H
#define INCLUDED_OSQLBUNDLED_H

#include "comdb2.h"
#include "block_internal.h"

extern int gbl_osql_max_bundled_bytes;
void init_bplog_bundled(osql_target_t *target);
void osql_extract_snap_info_from_bundle(osql_sess_t *sess, void *buf, int len, int is_uuid);
int osql_process_bundled(struct ireq *iq, unsigned long long rqid, uuid_t uuid,
                        void *trans, char *msg, int msglen, int *flags,
                        int **updCols, blob_buffer_t blobs[MAXBLOBS], int step,
                        struct block_err *err, int *receivedrows);
void copy_rqid(osql_target_t *target, unsigned long long rqid, uuid_t uuid);
#endif
