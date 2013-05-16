#include <erl_nif.h>    /* for Erlang NIF interface */
#include <string.h>
#include <errno.h>
#include "queue.h"        /* for queue interface */
#include "lmdb.h"        /* for MDB interface */


typedef struct {
    ErlNifTid tid;
    ErlNifThreadOpts* opts;

    MDB_env *env;

    queue *commands;

    int alive;
} emdb_connection;

typedef struct {
    MDB_txn *txn;
    emdb_connection *conn;
} emdb_txn;

typedef struct {
    MDB_dbi dbi;
    emdb_connection *conn;
} emdb_dbi;

typedef struct {
    MDB_cursor *cursor;
    emdb_txn   *txn;
    emdb_dbi   *dbi;
} emdb_cursor;

typedef enum {
    cmd_unknown,
    cmd_env_open,
    cmd_env_close,
    cmd_txn_begin,
    cmd_txn_commit,
    cmd_txn_abort,
    cmd_txn_reset,
    cmd_dbi_open,
    cmd_dbi_close,
    cmd_get,
    cmd_put,
    cmd_del,
    cmd_cursor_open,
    cmd_cursor_close,
    cmd_cursor_get,
    cmd_cursor_put,
    cmd_cursor_del,
    cmd_stop
} emdb_cmd_type;

typedef struct {
    emdb_cmd_type type;

    ErlNifEnv *env;
    ERL_NIF_TERM ref;
    ErlNifPid pid;

    ERL_NIF_TERM arg0;
    ERL_NIF_TERM arg1;
    ERL_NIF_TERM arg2;

    emdb_txn    *txn;
    emdb_dbi    *dbi;
    emdb_cursor *cursor;
} emdb_cmd;


static ErlNifResourceType *emdb_connection_type = NULL;
static ErlNifResourceType *emdb_txn_type        = NULL;
static ErlNifResourceType *emdb_dbi_type        = NULL;
static ErlNifResourceType *emdb_cursor_type     = NULL;


#define MAX_PATHNAME 512 /* unfortunately not in sqlite.h. */

/* emdb errors */
#define ERROR_NOT_FOUND              "not_found"
#define ERROR_INVALID_PARAMETER      "invalid_parameter"
#define ERROR_NO_SPACE               "no_space"
#define ERROR_NO_MEMORY              "no_memory"


static ERL_NIF_TERM
make_atom(ErlNifEnv *env, const char *atom_name)
{
    ERL_NIF_TERM atom;

    if (enif_make_existing_atom(env, atom_name, &atom, ERL_NIF_LATIN1)) {
	    return atom;
    }

    return enif_make_atom(env, atom_name);
}

static ERL_NIF_TERM
make_ok_tuple(ErlNifEnv *env, ERL_NIF_TERM value)
{
    return enif_make_tuple2(env, make_atom(env, "ok"), value);
}

static ERL_NIF_TERM
make_error_tuple(ErlNifEnv *env, const char *reason)
{
    return enif_make_tuple2(env, make_atom(env, "error"), make_atom(env, reason));
}

static ERL_NIF_TERM
make_binary(ErlNifEnv *env, const void *bytes, unsigned int size)
{
    ErlNifBinary blob;
    ERL_NIF_TERM term;

    if (!enif_alloc_binary(size, &blob)) {
	    return make_error_tuple(env, "make_binary");
    }

    memcpy(blob.data, bytes, size);
    term = enif_make_binary(env, &blob);
    enif_release_binary(&blob);

    return term;
}


static void
emdb_cmd_destroy(void *arg)
{
    emdb_cmd *cmd = (emdb_cmd *) arg;

    if (cmd->env != NULL) {
	    enif_free_env(cmd->env);
    }

    enif_free(cmd);
}

static emdb_cmd *
emdb_cmd_create()
{/*{{{*/
    emdb_cmd *cmd = (emdb_cmd *) enif_alloc(sizeof(emdb_cmd));
    if (cmd == NULL) {
	    return NULL;
    }

    cmd->env = enif_alloc_env();
    if (cmd->env == NULL) {
	    emdb_cmd_destroy(cmd);
        return NULL;
    }

    cmd->type   = cmd_unknown;
    cmd->ref    = 0;
    cmd->arg0   = 0;
    cmd->arg1   = 0;
    cmd->arg2   = 0;
    cmd->txn    = NULL;
    cmd->dbi    = NULL;
    cmd->cursor = NULL;

    return cmd;
}/*}}}*/

static void
emdb_connection_destroy(ErlNifEnv *env, void *arg)
{/*{{{*/
    emdb_connection *conn = (emdb_connection *) arg;
    emdb_cmd *cmd = emdb_cmd_create();

    /* Send the stop command */
    cmd->type = cmd_stop;
    queue_push(conn->commands, cmd);
    queue_send(conn->commands, cmd);

    /* Wait for the thread to finish */
    enif_thread_join(conn->tid, NULL);
    enif_thread_opts_destroy(conn->opts);

    /* The thread has finished... now remove the command queue, and close
     * the datbase (if it was still open).
     */
    queue_destroy(conn->commands);

    if (conn->env) {
	    mdb_env_close(conn->env);
    }
}/*}}}*/

static void
emdb_txn_destroy(ErlNifEnv *env, void *arg)
{/*{{{*/
    emdb_txn *txn = (emdb_txn *) arg;

    txn->conn = NULL;
}/*}}}*/

static void
emdb_dbi_destroy(ErlNifEnv *env, void *arg)
{/*{{{*/
    emdb_dbi *dbi = (emdb_dbi *) arg;

    dbi->conn = NULL;
}/*}}}*/

static void
emdb_cursor_destroy(ErlNifEnv *env, void *arg)
{/*{{{*/
    emdb_cursor *cursor = (emdb_cursor *) arg;

    cursor->txn = NULL;
    cursor->dbi = NULL;
}/*}}}*/



static ERL_NIF_TERM
do_env_open(ErlNifEnv *env, emdb_connection *conn,
        const ERL_NIF_TERM arg0, const ERL_NIF_TERM arg1)
{/*{{{*/
    char filename[MAX_PATHNAME];
    unsigned int size;
    ErlNifUInt64 flags;
    /*int mode;*/
    int rc;

    size = enif_get_string(env, arg0, filename, MAX_PATHNAME, ERL_NIF_LATIN1);
    if (size <= 0) {
        return make_error_tuple(env, "invalid_filename");
    }

    if (!enif_get_uint64(env, arg1, &flags)) {
        return make_error_tuple(env, "invalid_flags");
    }

    /*
    if (!enif_get_int(env, arg2, &mode)) {
        return make_error_tuple(env, "invalid_mode");
    }
    */

    rc = mdb_env_open(conn->env, filename, flags, 0644);

    switch (rc) {
        case MDB_SUCCESS:
            return make_atom(env, "ok");
        case MDB_VERSION_MISMATCH:
            return make_error_tuple(env, "version_mismatch");
        case MDB_INVALID:
            return make_error_tuple(env, "invalid_file");
        case ENOENT:
            return make_error_tuple(env, "non_exists_path");
        case EACCES:
            return make_error_tuple(env, "access_denied");
        case EAGAIN:
            return make_error_tuple(env, "locked");
        default:
            return make_error_tuple(env, "unkown");
    }
}/*}}}*/


static ERL_NIF_TERM
do_env_close(ErlNifEnv *env, emdb_connection *conn)
{/*{{{*/
    mdb_env_close(conn->env);
    conn->env = NULL;
    return make_atom(env, "ok");
}/*}}}*/

static ERL_NIF_TERM
do_txn_begin(ErlNifEnv *env, emdb_connection *conn, const ERL_NIF_TERM arg0)
{/*{{{*/
    ErlNifUInt64 flags;
    emdb_txn *txn;
    ERL_NIF_TERM txn_term;
    int rc;

    if (!enif_get_uint64(env, arg0, &flags)) {
        return make_error_tuple(env, "invalid_flags");
    }

    txn = enif_alloc_resource(emdb_txn_type, sizeof(emdb_txn));
    if (!txn) {
	    return make_error_tuple(env, ERROR_NO_MEMORY);
    }

    txn->conn = conn;

    rc = mdb_txn_begin(conn->env, NULL, flags, &txn->txn);

    if (rc == MDB_SUCCESS) {
        txn_term = enif_make_resource(env, txn);
    }
    enif_release_resource(txn);

    switch (rc) {
        case MDB_SUCCESS:
            return make_ok_tuple(env, txn_term);
        case MDB_PANIC:
            return make_error_tuple(env, "panic");
        case MDB_MAP_RESIZED:
            return make_error_tuple(env, "map_resized");
        case MDB_READERS_FULL:
            return make_error_tuple(env, "readers_full");
        case ENOMEM:
            return make_error_tuple(env, ERROR_NO_MEMORY);
        default:
            return make_error_tuple(env, "unkown");
    }
}/*}}}*/


static ERL_NIF_TERM
do_txn_commit(ErlNifEnv *env, emdb_txn *txn)
{/*{{{*/
    int rc;

    rc = mdb_txn_commit(txn->txn);

    switch (rc) {
        case MDB_SUCCESS:
            return make_atom(env, "ok");
        case EINVAL:
            return make_error_tuple(env, ERROR_INVALID_PARAMETER);
        case ENOSPC:
            return make_error_tuple(env, ERROR_NO_SPACE);
        case EIO:
            return make_error_tuple(env, "io_error");
        case ENOMEM:
            return make_error_tuple(env, ERROR_NO_MEMORY);
        default:
            return make_error_tuple(env, "unkown");
    }
}/*}}}*/

static ERL_NIF_TERM
do_txn_reset(ErlNifEnv *env, emdb_txn *txn)
{/*{{{*/
    mdb_txn_reset(txn->txn);
    return make_atom(env, "ok");
}/*}}}*/

static ERL_NIF_TERM
do_txn_abort(ErlNifEnv *env, emdb_txn *txn)
{/*{{{*/
    mdb_txn_abort(txn->txn);
    return make_atom(env, "ok");
}/*}}}*/

static ERL_NIF_TERM
do_dbi_open(ErlNifEnv *env, emdb_txn *txn,
        const ERL_NIF_TERM arg0, const ERL_NIF_TERM arg1)
{/*{{{*/
    char dbname[MAX_PATHNAME];
    unsigned int size;
    ErlNifUInt64 flags;
    emdb_dbi *dbi;
    ERL_NIF_TERM dbi_term;
    int rc;

    size = enif_get_string(env, arg0, dbname, MAX_PATHNAME, ERL_NIF_LATIN1);
    if (size <= 0) {
        return make_error_tuple(env, "invalid_dbname");
    }

    if (!enif_get_uint64(env, arg1, &flags)) {
        return make_error_tuple(env, "invalid_flags");
    }

    dbi = enif_alloc_resource(emdb_dbi_type, sizeof(emdb_dbi));
    if (!dbi) {
	    return make_error_tuple(env, ERROR_NO_MEMORY);
    }

    dbi->conn = txn->conn;

    rc = mdb_dbi_open(txn->txn, dbname, flags, &dbi->dbi);
    if (rc == MDB_SUCCESS) {
        dbi_term = enif_make_resource(env, dbi);
    }
    enif_release_resource(dbi);

    switch (rc) {
        case MDB_SUCCESS:
            return make_ok_tuple(env, dbi_term);
        case MDB_NOTFOUND:
            return make_error_tuple(env, "db_not_found");
        case MDB_DBS_FULL:
            return make_error_tuple(env, "dbs_full");
        default:
            return make_error_tuple(env, "unkown");
    }
}/*}}}*/

static ERL_NIF_TERM
do_dbi_close(ErlNifEnv *env, emdb_dbi *db)
{/*{{{*/
    mdb_dbi_close(db->conn->env, db->dbi);
    return make_atom(env, "ok");
}/*}}}*/


static ERL_NIF_TERM
do_get(ErlNifEnv *env, emdb_txn *txn, emdb_dbi *dbi,
        const ERL_NIF_TERM arg0)
{/*{{{*/
    ErlNifBinary key;

    MDB_val mkey;
    MDB_val mdata;

    int rc;

    if (!enif_inspect_iolist_as_binary(env, arg0, &key)) {
        return enif_make_badarg(env);
    }

    mkey.mv_size  = key.size;
    mkey.mv_data  = key.data;

    rc = mdb_get(txn->txn, dbi->dbi, &mkey, &mdata);

    switch (rc) {
        case MDB_SUCCESS:
            return make_ok_tuple(env,
                    make_binary(env, mdata.mv_data, mdata.mv_size));
        case MDB_NOTFOUND:
            return make_error_tuple(env, ERROR_NOT_FOUND);
        case EINVAL:
            return make_error_tuple(env, ERROR_INVALID_PARAMETER);
        default:
            return make_error_tuple(env, "unkown");

    }
}/*}}}*/

static ERL_NIF_TERM
do_put(ErlNifEnv *env, emdb_txn *txn, emdb_dbi *dbi,
        const ERL_NIF_TERM arg0, const ERL_NIF_TERM arg1,
        const ERL_NIF_TERM arg2)
{/*{{{*/
    ErlNifUInt64 flags;

    ErlNifBinary key;
    ErlNifBinary val;

    MDB_val mkey;
    MDB_val mdata;

    int rc;

    if (!enif_inspect_iolist_as_binary(env, arg0, &key)) {
        return enif_make_badarg(env);
    }

    if (!enif_inspect_iolist_as_binary(env, arg1, &val)) {
        return enif_make_badarg(env);
    }

    if (!enif_get_uint64(env, arg2, &flags)) {
        return make_error_tuple(env, "invalid_flags");
    }

    mkey.mv_size  = key.size;
    mkey.mv_data  = key.data;
    mdata.mv_size = val.size;
    mdata.mv_data = val.data;

    rc = mdb_put(txn->txn, dbi->dbi, &mkey, &mdata, flags);

    switch (rc) {
        case MDB_SUCCESS:
            return make_atom(env, "ok");
        case MDB_MAP_FULL:
            return make_error_tuple(env, "db_full");
        case MDB_TXN_FULL:
            return make_error_tuple(env, "txn_full");
        case EACCES:
            return make_error_tuple(env, "read_only");
        case EINVAL:
            return make_error_tuple(env, ERROR_INVALID_PARAMETER);
        default:
            return make_error_tuple(env, "unkown");
    }
}/*}}}*/

static ERL_NIF_TERM
do_del(ErlNifEnv *env, emdb_txn *txn, emdb_dbi *dbi,
        const ERL_NIF_TERM arg0, const ERL_NIF_TERM arg1)
{/*{{{*/
    ErlNifBinary key;
    ErlNifBinary val;

    MDB_val mkey;
    MDB_val mdata;

    int rc;

    if (!enif_inspect_iolist_as_binary(env, arg0, &key)) {
        return enif_make_badarg(env);
    }

    mkey.mv_size  = key.size;
    mkey.mv_data  = key.data;

    if (!enif_inspect_iolist_as_binary(env, arg1, &val)) {
        rc = mdb_del(txn->txn, dbi->dbi, &mkey, NULL);
    } else {
        mdata.mv_size = val.size;
        mdata.mv_data = val.data;
        rc = mdb_del(txn->txn, dbi->dbi, &mkey, &mdata);
    }

    switch (rc) {
        case MDB_SUCCESS:
            return make_atom(env, "ok");
        case EACCES:
            return make_error_tuple(env, "read_only");
        case EINVAL:
            return make_error_tuple(env, ERROR_INVALID_PARAMETER);
        default:
            return make_error_tuple(env, "unkown");
    }
}/*}}}*/

static ERL_NIF_TERM
do_cursor_open(ErlNifEnv *env, emdb_txn *txn, emdb_dbi *dbi)
{/*{{{*/
    emdb_cursor *cursor;
    ERL_NIF_TERM cursor_term;
    int rc;

    cursor = enif_alloc_resource(emdb_cursor_type, sizeof(emdb_cursor));
    if (!cursor) {
	    return make_error_tuple(env, ERROR_NO_MEMORY);
    }
    cursor->txn = txn;
    cursor->dbi = dbi;

    rc = mdb_cursor_open(txn->txn, dbi->dbi, &cursor->cursor);

    if (rc == MDB_SUCCESS) {
        cursor_term = enif_make_resource(env, cursor);
    }
    enif_release_resource(cursor);

    switch (rc) {
        case MDB_SUCCESS:
            return make_ok_tuple(env, cursor_term);
        case EINVAL:
            return make_error_tuple(env, ERROR_INVALID_PARAMETER);
        default:
            return make_error_tuple(env, "unkown");
    }
}/*}}}*/

static ERL_NIF_TERM
do_cursor_close(ErlNifEnv *env, emdb_cursor *cursor)
{/*{{{*/
    mdb_cursor_close(cursor->cursor);

    return make_atom(env, "ok");
}/*}}}*/

static ERL_NIF_TERM
do_cursor_get(ErlNifEnv *env, emdb_cursor *cursor,
        const ERL_NIF_TERM arg0, const ERL_NIF_TERM arg1, const ERL_NIF_TERM arg2)
{/*{{{*/
    ErlNifBinary key;
    ErlNifBinary val;

    MDB_val mkey;
    MDB_val mdata;

    MDB_cursor_op op;

    int op_code;
    int rc;

    if (enif_inspect_iolist_as_binary(env, arg0, &key)) {
        mkey.mv_size = key.size;
        mkey.mv_data = key.data;
    }

    if (enif_inspect_iolist_as_binary(env, arg1, &val)) {
        mdata.mv_size = val.size;
        mdata.mv_data = val.data;
    }

    if (!enif_get_int(env, arg2, &op_code)) {
        return enif_make_badarg(env);
    }

    switch (op_code) {
        case 0: op = MDB_FIRST; break;
        case 1: op = MDB_FIRST_DUP; break;
        case 2: op = MDB_GET_BOTH; break;
        case 3: op = MDB_GET_BOTH_RANGE; break;
        case 4: op = MDB_GET_CURRENT; break;
        case 5: op = MDB_GET_MULTIPLE; break;
        case 6: op = MDB_LAST; break;
        case 7: op = MDB_LAST_DUP; break;
        case 8: op = MDB_NEXT; break;
        case 9: op = MDB_NEXT_DUP; break;
        case 10: op = MDB_NEXT_MULTIPLE; break;
        case 11: op = MDB_NEXT_NODUP; break;
        case 12: op = MDB_PREV; break;
        case 13: op = MDB_PREV_DUP; break;
        case 14: op = MDB_PREV_NODUP; break;
        case 15: op = MDB_SET; break;
        case 16: op = MDB_SET_KEY; break;
        case 17: op = MDB_SET_RANGE; break;
        default: op = MDB_NEXT;
    }

    rc = mdb_cursor_get(cursor->cursor, &mkey, &mdata, op);

    switch (rc) {
        case MDB_SUCCESS:
            return make_ok_tuple(env, enif_make_tuple2(env,
                        make_binary(env, mkey.mv_data, mkey.mv_size),
                        make_binary(env, mdata.mv_data, mdata.mv_size)));
        case MDB_NOTFOUND:
            return make_error_tuple(env, ERROR_NOT_FOUND);
        case EINVAL:
            return make_error_tuple(env, ERROR_INVALID_PARAMETER);
        default:
            return make_error_tuple(env, "unkown");
    }
}/*}}}*/

static ERL_NIF_TERM
do_cursor_put(ErlNifEnv *env, emdb_cursor *cursor,
        const ERL_NIF_TERM arg0, const ERL_NIF_TERM arg1, const ERL_NIF_TERM arg2)
{/*{{{*/
    ErlNifUInt64 flags;

    ErlNifBinary key;
    ErlNifBinary val;

    MDB_val mkey;
    MDB_val mdata;

    int rc;

    if (!enif_inspect_iolist_as_binary(env, arg0, &key)) {
        return enif_make_badarg(env);
    }

    if (!enif_inspect_iolist_as_binary(env, arg1, &val)) {
        return enif_make_badarg(env);
    }

    if (!enif_get_uint64(env, arg2, &flags)) {
        return make_error_tuple(env, "invalid_flags");
    }

    mkey.mv_size = key.size;
    mkey.mv_data = key.data;
    mdata.mv_size = val.size;
    mdata.mv_data = val.data;

    rc = mdb_cursor_put(cursor->cursor, &mkey, &mdata, flags);

    switch (rc) {
        case MDB_SUCCESS:
            return make_atom(env, "ok");
        case MDB_MAP_FULL:
            return make_error_tuple(env, "db_full");
        case MDB_TXN_FULL:
            return make_error_tuple(env, "txn_full");
        case EACCES:
            return make_error_tuple(env, "read_only");
        case EINVAL:
            return make_error_tuple(env, ERROR_INVALID_PARAMETER);
        default:
            return make_error_tuple(env, "unkown");
    }
}/*}}}*/

static ERL_NIF_TERM
do_cursor_del(ErlNifEnv *env, emdb_cursor *cursor,
        const ERL_NIF_TERM arg0)
{/*{{{*/
    ErlNifUInt64 flags;

    int rc;

    if (!enif_get_uint64(env, arg0, &flags)) {
        return make_error_tuple(env, "invalid_flags");
    }

    rc = mdb_cursor_del(cursor->cursor, flags);

    switch (rc) {
        case MDB_SUCCESS:
            return make_atom(env, "ok");
        case EACCES:
            return make_error_tuple(env, "read_only");
        case EINVAL:
            return make_error_tuple(env, "invalid_parameter");
        default:
            return make_error_tuple(env, "unkown");
    }
}/*}}}*/

static ERL_NIF_TERM
evaluate_command(emdb_cmd *cmd, emdb_connection *conn)
{/*{{{*/
    switch (cmd->type) {
        case cmd_get:
            return do_get(cmd->env, cmd->txn, cmd->dbi, cmd->arg0);
        case cmd_put:
            return do_put(cmd->env, cmd->txn, cmd->dbi, cmd->arg0, cmd->arg1, cmd->arg2);
        case cmd_del:
            return do_del(cmd->env, cmd->txn, cmd->dbi, cmd->arg0, cmd->arg1);
        case cmd_cursor_open:
            return do_cursor_open(cmd->env, cmd->txn, cmd->dbi);
        case cmd_cursor_close:
            return do_cursor_close(cmd->env, cmd->cursor);
        case cmd_cursor_get:
            return do_cursor_get(cmd->env, cmd->cursor, cmd->arg0, cmd->arg1, cmd->arg2);
        case cmd_cursor_put:
            return do_cursor_put(cmd->env, cmd->cursor, cmd->arg0, cmd->arg1, cmd->arg2);
        case cmd_cursor_del:
            return do_cursor_del(cmd->env, cmd->cursor, cmd->arg0);
        case cmd_env_open:
            return do_env_open(cmd->env, conn, cmd->arg0, cmd->arg1);
        case cmd_env_close:
            return do_env_close(cmd->env, conn);
        case cmd_txn_begin:
            return do_txn_begin(cmd->env, conn, cmd->arg0);
        case cmd_txn_commit:
            return do_txn_commit(cmd->env, cmd->txn);
        case cmd_txn_reset:
            return do_txn_reset(cmd->env, cmd->txn);
        case cmd_txn_abort:
            return do_txn_abort(cmd->env, cmd->txn);
        case cmd_dbi_open:
            return do_dbi_open(cmd->env, cmd->txn, cmd->arg0, cmd->arg1);
        case cmd_dbi_close:
            return do_dbi_close(cmd->env, cmd->dbi);
        default:
            return make_error_tuple(cmd->env, "invalid_command");
    }
}/*}}}*/

static ERL_NIF_TERM
push_command(ErlNifEnv *env, emdb_connection *conn, emdb_cmd *cmd) {
    if (!queue_push(conn->commands, cmd)) {
        return make_error_tuple(env, "command_push_failed");
    }

    return make_atom(env, "ok");
}

static ERL_NIF_TERM
make_answer(emdb_cmd *cmd, ERL_NIF_TERM answer)
{
    return enif_make_tuple2(cmd->env, cmd->ref, answer);
}

static void *
emdb_env_run(void *arg)
{/*{{{*/
    emdb_connection *conn = (emdb_connection *) arg;
    emdb_cmd *cmd;
    int continue_running = 1;

    conn->alive = 1;

    while (continue_running) {
        cmd = queue_pop(conn->commands);

        if (cmd->type == cmd_stop) {
	        continue_running = 0;
        } else {
	        enif_send(NULL, &cmd->pid, cmd->env, make_answer(cmd, evaluate_command(cmd, conn)));
        }

	    emdb_cmd_destroy(cmd);
    }

    conn->alive = 0;
    return NULL;
}/*}}}*/

/*
 * Driver callbacks
 */
static ERL_NIF_TERM
emdb_version(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{/*{{{*/
    int major, minor, patch;

    mdb_version(&major, &minor, &patch);

    return enif_make_tuple(env, 3,
                           enif_make_int(env, major),
                           enif_make_int(env, minor),
                           enif_make_int(env, patch));
}/*}}}*/

static ERL_NIF_TERM
emdb_strerror(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{/*{{{*/
    char *msg;
    int err;
    int size;

    ErlNifBinary bin;
    ERL_NIF_TERM term;

    if (!enif_get_int(env, argv[0], &err)) {
        return enif_make_badarg(env);
    }

    msg = mdb_strerror(err);

    size = strlen(msg);

    if (!enif_alloc_binary(size, &bin)) {
        return make_error_tuple(env, "make_binary");
    }

    memcpy(bin.data, msg, size);
    term = enif_make_binary(env, &bin);

    if (!term) {
        return make_error_tuple(env, "make_binary");
    }

    return term;
}/*}}}*/

static ERL_NIF_TERM
emdb_env_create(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{/*{{{*/
    emdb_connection *conn;
    ERL_NIF_TERM db_conn;

    /* Initialize the resource */
    conn = enif_alloc_resource(emdb_connection_type, sizeof(emdb_connection));
    if (!conn) {
	    return make_error_tuple(env, ERROR_NO_MEMORY);
    }

    if (mdb_env_create(&conn->env) != MDB_SUCCESS) {
        mdb_env_close(conn->env);
        conn->env = NULL;
        enif_release_resource(conn);
        return make_error_tuple(env, "failed");
    }

    /* Create command queue */
    conn->commands = queue_create();
    if (!conn->commands) {
	    enif_release_resource(conn);
	    return make_error_tuple(env, "command_queue_create_failed");
    }

    /* Start command processing thread */
    conn->opts = enif_thread_opts_create("emdb_thread_opts");
    if (enif_thread_create("emdb_connection", &conn->tid, emdb_env_run, conn, conn->opts) != 0) {
	    enif_release_resource(conn);
	    return make_error_tuple(env, "thread_create_failed");
    }

    db_conn = enif_make_resource(env, conn);
    enif_release_resource(conn);

    return make_ok_tuple(env, db_conn);
}/*}}}*/

static ERL_NIF_TERM
emdb_env_set_maxdbs(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{/*{{{*/
    emdb_connection *conn;
    int maxdbs;
    int rc;

    if (argc != 2)
	    return enif_make_badarg(env);

    if (!enif_get_resource(env, argv[0], emdb_connection_type, (void **) &conn))
	    return enif_make_badarg(env);

    if (!enif_get_int(env, argv[1], &maxdbs))
        return make_error_tuple(env, "invalid_maxdbs");

    rc = mdb_env_set_maxdbs(conn->env, maxdbs);

    switch (rc) {
        case MDB_SUCCESS:
            return make_atom(env, "ok");
        case EINVAL:
            return make_error_tuple(env, "already_opened");
        default:
            return make_error_tuple(env, "unkown");
    }
}/*}}}*/

static ERL_NIF_TERM
emdb_env_set_flags(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{/*{{{*/
    emdb_connection *conn;
    ErlNifUInt64 flags;
    int rc;

    if (argc != 2) {
	    return enif_make_badarg(env);
    }

    if (!enif_get_resource(env, argv[0], emdb_connection_type, (void **) &conn)) {
	    return enif_make_badarg(env);
    }

    if (!enif_get_uint64(env, argv[1], &flags)) {
        return make_error_tuple(env, "invalid_flags");
    }

    rc = mdb_env_set_flags(conn->env, flags, 0);

    switch (rc) {
        case MDB_SUCCESS:
            return make_atom(env, "ok");
        case EINVAL:
            return make_error_tuple(env, "already_opened");
        default:
            return make_error_tuple(env, "unkown");
    }
}/*}}}*/

static ERL_NIF_TERM
emdb_env_open(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{/*{{{*/
    emdb_connection *conn;
    emdb_cmd *cmd = NULL;
    ErlNifPid pid;

    if (argc != 5)
	    return enif_make_badarg(env);

    if (!enif_is_ref(env, argv[0]))
	    return make_error_tuple(env, "invalid_ref");

    if (!enif_get_local_pid(env, argv[1], &pid))
	    return make_error_tuple(env, "invalid_pid");

    if (!enif_get_resource(env, argv[2], emdb_connection_type, (void **) &conn))
	    return enif_make_badarg(env);

    if (!enif_is_list(env, argv[3]))
        return make_error_tuple(env, "invalid_filename");

    if (!enif_is_number(env, argv[4]))
        return make_error_tuple(env, "invalid_flags");

    /* Note, no check is made for the type of the argument */
    cmd = emdb_cmd_create();
    if (!cmd) {
	    return make_error_tuple(env, "command_create_failed");
    }

    cmd->type = cmd_env_open;
    cmd->ref  = enif_make_copy(cmd->env, argv[0]);
    cmd->pid  = pid;
    cmd->arg0 = enif_make_copy(cmd->env, argv[3]);
    cmd->arg1 = enif_make_copy(cmd->env, argv[4]);

    return push_command(env, conn, cmd);
}/*}}}*/

static ERL_NIF_TERM
emdb_env_close(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{/*{{{*/
    emdb_connection *conn;
    emdb_cmd *cmd = NULL;
    ErlNifPid pid;

    if (argc != 3)
	    return enif_make_badarg(env);

    if (!enif_is_ref(env, argv[0]))
	    return make_error_tuple(env, "invalid_ref");

    if (!enif_get_local_pid(env, argv[1], &pid))
	    return make_error_tuple(env, "invalid_pid");

    if (!enif_get_resource(env, argv[2], emdb_connection_type, (void **) &conn))
	    return enif_make_badarg(env);

    cmd = emdb_cmd_create();
    if (!cmd) {
	    return make_error_tuple(env, "command_create_failed");
    }

    cmd->type = cmd_env_close;
    cmd->ref  = enif_make_copy(cmd->env, argv[0]);
    cmd->pid  = pid;

    return push_command(env, conn, cmd);
}/*}}}*/


static ERL_NIF_TERM
emdb_txn_begin(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{/*{{{*/
    emdb_connection *conn;
    emdb_cmd *cmd = NULL;
    ErlNifPid pid;

    if (argc != 4)
	    return enif_make_badarg(env);

    if (!enif_is_ref(env, argv[0]))
	    return make_error_tuple(env, "invalid_ref");

    if (!enif_get_local_pid(env, argv[1], &pid))
	    return make_error_tuple(env, "invalid_pid");

    if (!enif_get_resource(env, argv[2], emdb_connection_type, (void **) &conn))
	    return enif_make_badarg(env);

    if (!enif_is_number(env, argv[3]))
        return make_error_tuple(env, "invalid_flags");

    cmd = emdb_cmd_create();
    if (!cmd) {
	    return make_error_tuple(env, "command_create_failed");
    }

    cmd->type = cmd_txn_begin;
    cmd->ref  = enif_make_copy(cmd->env, argv[0]);
    cmd->pid  = pid;
    cmd->arg0 = enif_make_copy(cmd->env, argv[3]);

    return push_command(env, conn, cmd);
}/*}}}*/

static ERL_NIF_TERM
emdb_txn_commit(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{/*{{{*/
    emdb_txn *txn;
    emdb_cmd *cmd = NULL;
    ErlNifPid pid;

    if (argc != 3)
	    return enif_make_badarg(env);

    if (!enif_is_ref(env, argv[0]))
	    return make_error_tuple(env, "invalid_ref");

    if (!enif_get_local_pid(env, argv[1], &pid))
	    return make_error_tuple(env, "invalid_pid");

    if (!enif_get_resource(env, argv[2], emdb_txn_type, (void **) &txn))
	    return enif_make_badarg(env);

    cmd = emdb_cmd_create();
    if (!cmd) {
	    return make_error_tuple(env, "command_create_failed");
    }

    cmd->type = cmd_txn_commit;
    cmd->ref  = enif_make_copy(cmd->env, argv[0]);
    cmd->pid  = pid;
    cmd->txn  = txn;

    return push_command(env, txn->conn, cmd);
}/*}}}*/

static ERL_NIF_TERM
emdb_txn_abort(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{/*{{{*/
    emdb_txn *txn;
    emdb_cmd *cmd = NULL;
    ErlNifPid pid;

    if (argc != 3)
	    return enif_make_badarg(env);

    if (!enif_is_ref(env, argv[0]))
	    return make_error_tuple(env, "invalid_ref");

    if (!enif_get_local_pid(env, argv[1], &pid))
	    return make_error_tuple(env, "invalid_pid");

    if (!enif_get_resource(env, argv[2], emdb_txn_type, (void **) &txn))
	    return enif_make_badarg(env);

    cmd = emdb_cmd_create();
    if (!cmd) {
	    return make_error_tuple(env, "command_create_failed");
    }

    cmd->type = cmd_txn_abort;
    cmd->ref  = enif_make_copy(cmd->env, argv[0]);
    cmd->pid  = pid;
    cmd->txn  = txn;

    return push_command(env, txn->conn, cmd);
}/*}}}*/

static ERL_NIF_TERM
emdb_txn_reset(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{/*{{{*/
    emdb_txn *txn;
    emdb_cmd *cmd = NULL;
    ErlNifPid pid;

    if (argc != 3)
	    return enif_make_badarg(env);

    if (!enif_is_ref(env, argv[0]))
	    return make_error_tuple(env, "invalid_ref");

    if (!enif_get_local_pid(env, argv[1], &pid))
	    return make_error_tuple(env, "invalid_pid");

    if (!enif_get_resource(env, argv[2], emdb_txn_type, (void **) &txn))
	    return enif_make_badarg(env);

    cmd = emdb_cmd_create();
    if (!cmd) {
	    return make_error_tuple(env, "command_create_failed");
    }

    cmd->type = cmd_txn_reset;
    cmd->ref  = enif_make_copy(cmd->env, argv[0]);
    cmd->pid  = pid;
    cmd->txn  = txn;

    return push_command(env, txn->conn, cmd);
}/*}}}*/

static ERL_NIF_TERM
emdb_dbi_open(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{/*{{{*/
    emdb_txn *txn;
    emdb_cmd *cmd = NULL;
    ErlNifPid pid;

    if (argc != 5)
	    return enif_make_badarg(env);

    if (!enif_is_ref(env, argv[0]))
	    return make_error_tuple(env, "invalid_ref");

    if (!enif_get_local_pid(env, argv[1], &pid))
	    return make_error_tuple(env, "invalid_pid");

    if (!enif_get_resource(env, argv[2], emdb_txn_type, (void **) &txn))
	    return enif_make_badarg(env);

    if (!enif_is_list(env, argv[3]))
        return make_error_tuple(env, "invalid_dbname");

    if (!enif_is_number(env, argv[4]))
        return make_error_tuple(env, "invalid_flags");

    cmd = emdb_cmd_create();
    if (!cmd) {
	    return make_error_tuple(env, "command_create_failed");
    }

    cmd->type = cmd_dbi_open;
    cmd->ref  = enif_make_copy(cmd->env, argv[0]);
    cmd->pid  = pid;
    cmd->arg0 = enif_make_copy(cmd->env, argv[3]);
    cmd->arg1 = enif_make_copy(cmd->env, argv[4]);
    cmd->txn  = txn;

    return push_command(env, txn->conn, cmd);
}/*}}}*/

static ERL_NIF_TERM
emdb_dbi_close(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{/*{{{*/
    emdb_dbi *dbi;
    emdb_cmd *cmd = NULL;
    ErlNifPid pid;

    if (argc != 3)
	    return enif_make_badarg(env);

    if (!enif_is_ref(env, argv[0]))
	    return make_error_tuple(env, "invalid_ref");

    if (!enif_get_local_pid(env, argv[1], &pid))
	    return make_error_tuple(env, "invalid_pid");

    if (!enif_get_resource(env, argv[2], emdb_dbi_type, (void **) &dbi))
	    return enif_make_badarg(env);

    cmd = emdb_cmd_create();
    if (!cmd) {
	    return make_error_tuple(env, "command_create_failed");
    }

    cmd->type = cmd_dbi_close;
    cmd->ref  = enif_make_copy(cmd->env, argv[0]);
    cmd->pid  = pid;
    cmd->dbi  = dbi;

    return push_command(env, dbi->conn, cmd);
}/*}}}*/

static ERL_NIF_TERM
emdb_get(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{/*{{{*/
    emdb_txn *txn;
    emdb_dbi *dbi;
    emdb_cmd *cmd = NULL;
    ErlNifPid pid;

    if (argc != 5)
	    return enif_make_badarg(env);

    if (!enif_is_ref(env, argv[0]))
	    return make_error_tuple(env, "invalid_ref");

    if (!enif_get_local_pid(env, argv[1], &pid))
	    return make_error_tuple(env, "invalid_pid");

    if (!enif_get_resource(env, argv[2], emdb_txn_type, (void **) &txn))
	    return enif_make_badarg(env);

    if (!enif_get_resource(env, argv[3], emdb_dbi_type, (void **) &dbi))
	    return enif_make_badarg(env);

    /*
    if (!enif_is_binary(env, argv[4]))
        return make_error_tuple(env, "invalid_key");
    */

    cmd = emdb_cmd_create();
    if (!cmd) {
	    return make_error_tuple(env, "command_create_failed");
    }

    cmd->type = cmd_get;
    cmd->ref  = enif_make_copy(cmd->env, argv[0]);
    cmd->pid  = pid;
    cmd->txn  = txn;
    cmd->dbi  = dbi;
    cmd->arg0 = enif_make_copy(cmd->env, argv[4]);

    return push_command(env, txn->conn, cmd);
}/*}}}*/

static ERL_NIF_TERM
emdb_put(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{/*{{{*/
    emdb_txn *txn;
    emdb_dbi *dbi;
    emdb_cmd *cmd = NULL;
    ErlNifPid pid;

    if (argc != 7)
	    return enif_make_badarg(env);

    if (!enif_is_ref(env, argv[0]))
	    return make_error_tuple(env, "invalid_ref");

    if (!enif_get_local_pid(env, argv[1], &pid))
	    return make_error_tuple(env, "invalid_pid");

    if (!enif_get_resource(env, argv[2], emdb_txn_type, (void **) &txn))
	    return enif_make_badarg(env);

    if (!enif_get_resource(env, argv[3], emdb_dbi_type, (void **) &dbi))
	    return enif_make_badarg(env);

    /*
    if (!enif_is_binary(env, argv[4]))
        return make_error_tuple(env, "invalid_key");

    if (!enif_is_binary(env, argv[5]))
        return make_error_tuple(env, "invalid_value");
    */

    if (!enif_is_number(env, argv[6]))
        return make_error_tuple(env, "invalid_flags");

    cmd = emdb_cmd_create();
    if (!cmd) {
	    return make_error_tuple(env, "command_create_failed");
    }

    cmd->type = cmd_put;
    cmd->ref  = enif_make_copy(cmd->env, argv[0]);
    cmd->pid  = pid;
    cmd->txn  = txn;
    cmd->dbi  = dbi;
    cmd->arg0 = enif_make_copy(cmd->env, argv[4]);
    cmd->arg1 = enif_make_copy(cmd->env, argv[5]);
    cmd->arg2 = enif_make_copy(cmd->env, argv[6]);

    return push_command(env, txn->conn, cmd);
}/*}}}*/

static ERL_NIF_TERM
emdb_del(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{/*{{{*/
    emdb_txn *txn;
    emdb_dbi *dbi;
    emdb_cmd *cmd = NULL;
    ErlNifPid pid;

    if (argc != 6)
	    return enif_make_badarg(env);

    if (!enif_is_ref(env, argv[0]))
	    return make_error_tuple(env, "invalid_ref");

    if (!enif_get_local_pid(env, argv[1], &pid))
	    return make_error_tuple(env, "invalid_pid");

    if (!enif_get_resource(env, argv[2], emdb_txn_type, (void **) &txn))
	    return enif_make_badarg(env);

    if (!enif_get_resource(env, argv[3], emdb_dbi_type, (void **) &dbi))
	    return enif_make_badarg(env);

    cmd = emdb_cmd_create();
    if (!cmd) {
	    return make_error_tuple(env, "command_create_failed");
    }

    cmd->type = cmd_del;
    cmd->ref  = enif_make_copy(cmd->env, argv[0]);
    cmd->pid  = pid;
    cmd->txn  = txn;
    cmd->dbi  = dbi;
    cmd->arg0 = enif_make_copy(cmd->env, argv[4]);
    cmd->arg1 = enif_make_copy(cmd->env, argv[5]);

    return push_command(env, txn->conn, cmd);
}/*}}}*/


static ERL_NIF_TERM
emdb_cursor_open(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{/*{{{*/
    emdb_txn *txn;
    emdb_dbi *dbi;
    emdb_cmd *cmd = NULL;
    ErlNifPid pid;

    if (argc != 4)
	    return enif_make_badarg(env);

    if (!enif_is_ref(env, argv[0]))
	    return make_error_tuple(env, "invalid_ref");

    if (!enif_get_local_pid(env, argv[1], &pid))
	    return make_error_tuple(env, "invalid_pid");

    if (!enif_get_resource(env, argv[2], emdb_txn_type, (void **) &txn))
	    return enif_make_badarg(env);

    if (!enif_get_resource(env, argv[3], emdb_dbi_type, (void **) &dbi))
	    return enif_make_badarg(env);

    cmd = emdb_cmd_create();
    if (!cmd) {
	    return make_error_tuple(env, "command_create_failed");
    }

    cmd->type = cmd_cursor_open;
    cmd->ref  = enif_make_copy(cmd->env, argv[0]);
    cmd->pid  = pid;
    cmd->txn  = txn;
    cmd->dbi  = dbi;

    return push_command(env, txn->conn, cmd);
}/*}}}*/

static ERL_NIF_TERM
emdb_cursor_close(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{/*{{{*/
    emdb_cursor *cursor;
    emdb_cmd *cmd = NULL;
    ErlNifPid pid;

    if (argc != 3)
	    return enif_make_badarg(env);

    if (!enif_is_ref(env, argv[0]))
	    return make_error_tuple(env, "invalid_ref");

    if (!enif_get_local_pid(env, argv[1], &pid))
	    return make_error_tuple(env, "invalid_pid");

    if (!enif_get_resource(env, argv[2], emdb_cursor_type, (void **) &cursor))
	    return enif_make_badarg(env);

    cmd = emdb_cmd_create();
    if (!cmd) {
	    return make_error_tuple(env, "command_create_failed");
    }

    cmd->type   = cmd_cursor_close;
    cmd->ref    = enif_make_copy(cmd->env, argv[0]);
    cmd->pid    = pid;
    cmd->cursor = cursor;

    return push_command(env, cursor->txn->conn, cmd);
}/*}}}*/

static ERL_NIF_TERM
emdb_cursor_get(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{/*{{{*/
    emdb_cursor *cursor;
    emdb_cmd *cmd = NULL;
    ErlNifPid pid;

    if (argc != 6)
	    return enif_make_badarg(env);

    if (!enif_is_ref(env, argv[0]))
	    return make_error_tuple(env, "invalid_ref");

    if (!enif_get_local_pid(env, argv[1], &pid))
	    return make_error_tuple(env, "invalid_pid");

    if (!enif_get_resource(env, argv[2], emdb_cursor_type, (void **) &cursor))
	    return enif_make_badarg(env);

    if (!enif_is_number(env, argv[5]))
        return enif_make_badarg(env);

    cmd = emdb_cmd_create();
    if (!cmd) {
	    return make_error_tuple(env, "command_create_failed");
    }

    cmd->type   = cmd_cursor_get;
    cmd->ref    = enif_make_copy(cmd->env, argv[0]);
    cmd->pid    = pid;
    cmd->cursor = cursor;
    cmd->arg0   = enif_make_copy(cmd->env, argv[3]);
    cmd->arg1   = enif_make_copy(cmd->env, argv[4]);
    cmd->arg2   = enif_make_copy(cmd->env, argv[5]);

    return push_command(env, cursor->txn->conn, cmd);
}/*}}}*/

static ERL_NIF_TERM
emdb_cursor_put(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{/*{{{*/
    emdb_cursor *cursor;
    emdb_cmd *cmd = NULL;
    ErlNifPid pid;

    if (argc != 6)
	    return enif_make_badarg(env);

    if (!enif_is_ref(env, argv[0]))
	    return make_error_tuple(env, "invalid_ref");

    if (!enif_get_local_pid(env, argv[1], &pid))
	    return make_error_tuple(env, "invalid_pid");

    if (!enif_get_resource(env, argv[2], emdb_cursor_type, (void **) &cursor))
	    return enif_make_badarg(env);

    cmd = emdb_cmd_create();
    if (!cmd) {
	    return make_error_tuple(env, "command_create_failed");
    }

    cmd->type   = cmd_cursor_put;
    cmd->ref    = enif_make_copy(cmd->env, argv[0]);
    cmd->pid    = pid;
    cmd->cursor = cursor;
    cmd->arg0   = enif_make_copy(cmd->env, argv[3]);
    cmd->arg1   = enif_make_copy(cmd->env, argv[4]);
    cmd->arg2   = enif_make_copy(cmd->env, argv[5]);

    return push_command(env, cursor->txn->conn, cmd);
}/*}}}*/

static ERL_NIF_TERM
emdb_cursor_del(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{/*{{{*/
    emdb_cursor *cursor;
    emdb_cmd *cmd = NULL;
    ErlNifPid pid;

    if (argc != 4)
	    return enif_make_badarg(env);

    if (!enif_is_ref(env, argv[0]))
	    return make_error_tuple(env, "invalid_ref");

    if (!enif_get_local_pid(env, argv[1], &pid))
	    return make_error_tuple(env, "invalid_pid");

    if (!enif_get_resource(env, argv[2], emdb_cursor_type, (void **) &cursor))
	    return enif_make_badarg(env);

    cmd = emdb_cmd_create();
    if (!cmd) {
	    return make_error_tuple(env, "command_create_failed");
    }

    cmd->type   = cmd_cursor_del;
    cmd->ref    = enif_make_copy(cmd->env, argv[0]);
    cmd->pid    = pid;
    cmd->cursor = cursor;
    cmd->arg0   = enif_make_copy(cmd->env, argv[3]);

    return push_command(env, cursor->txn->conn, cmd);
}/*}}}*/

static int emdb_load(ErlNifEnv *env, void **priv, ERL_NIF_TERM load_info)
{/*{{{*/
    ErlNifResourceType *rt;

    rt = enif_open_resource_type(env, "emdb_drv", "emdb_connection_type",
				emdb_connection_destroy, ERL_NIF_RT_CREATE, NULL);
    if (!rt) {
	    return -1;
    }
    emdb_connection_type = rt;

    rt =  enif_open_resource_type(env, "emdb_drv", "emdb_txn_type",
				   emdb_txn_destroy, ERL_NIF_RT_CREATE, NULL);
    if (!rt) {
	    return -1;
    }
    emdb_txn_type = rt;

    rt =  enif_open_resource_type(env, "emdb_drv", "emdb_dbi_type",
				   emdb_dbi_destroy, ERL_NIF_RT_CREATE, NULL);
    if (!rt) {
	    return -1;
    }
    emdb_dbi_type = rt;

    rt =  enif_open_resource_type(env, "emdb_drv", "emdb_cursor_type",
				   emdb_cursor_destroy, ERL_NIF_RT_CREATE, NULL);
    if (!rt) {
	    return -1;
    }
    emdb_cursor_type = rt;

    /* Initialize common atoms */

    return 0;
}/*}}}*/

static int emdb_reload(ErlNifEnv *env, void **priv, ERL_NIF_TERM info)
{
    return 0;
}


static int emdb_upgrade(ErlNifEnv *env, void **priv, void **old_priv, ERL_NIF_TERM load_info)
{
    return 0;
}


static void emdb_unload(ErlNifEnv *env, void *priv)
{
    return;
}



static ErlNifFunc nif_funcs[] =
{
    {"version",        0, emdb_version},
    {"strerror",       1, emdb_strerror},

    {"env_create",     0, emdb_env_create},
    {"env_open",       5, emdb_env_open},
    {"env_close",      3, emdb_env_close},
    {"env_set_maxdbs", 2, emdb_env_set_maxdbs},
    {"env_set_flags",  2, emdb_env_set_flags},

    {"txn_begin",      4, emdb_txn_begin},
    {"txn_commit",     3, emdb_txn_commit},
    {"txn_abort",      3, emdb_txn_abort},
    {"txn_reset",      3, emdb_txn_reset},

    {"open",           5, emdb_dbi_open},
    {"close",          3, emdb_dbi_close},

    {"get",            5, emdb_get},
    {"put",            7, emdb_put},
    {"del",            6, emdb_del},

    {"cursor_open",    4, emdb_cursor_open},
    {"cursor_close",   3, emdb_cursor_close},
    {"cursor_get",     6, emdb_cursor_get},
    {"cursor_put",     6, emdb_cursor_put},
    {"cursor_del",     4, emdb_cursor_del},
    /*
    {"env_copy",    2, emdb_env_copy},
    {"env_stat",    1, emdb_env_stat},
    {"env_info",    1, emdb_env_info},
    {"env_sync",    2, emdb_env_sync},

    {"txn_renew",   1, emdb_txn_renew},

    {"drop",        2, emdb_drop},


    {"cursor_renew", 2, emdb_cursor_renew},

    {"cursor_count", 2, emdb_cursor_count},
    */
};

/* driver entry point */
ERL_NIF_INIT(emdb_drv,
             nif_funcs,
             & emdb_load,
             & emdb_reload,
             & emdb_upgrade,
             & emdb_unload)
