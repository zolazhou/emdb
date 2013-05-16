-module(emdb).


%%====================================================================
%% EXPORTS
%%====================================================================
-export([version/0,
         strerror/1]).

-export([env_create/0,
         env_open/2,
         env_open/3,
         env_close/1,
         env_set_maxdbs/2,
         env_set_flags/2]).

-export([txn_begin/1,
         txn_begin/2,
         txn_commit/1,
         txn_abort/1,
         txn_reset/1]).

-export([open/2,
         open/3,
         close/1]).

-export([get/3,
         put/4,
         put/5,
         del/3,
         del/4]).

-export([cursor_open/2,
         cursor_close/1,
         cursor_get/2,
         cursor_get/4,
         cursor_put/3,
         cursor_put/4,
         cursor_del/1,
         cursor_del/2]).

%%====================================================================
%% Includes
%%====================================================================
-include("emdb.hrl").

%%====================================================================
%% PUBLIC API
%%====================================================================

-spec version() -> {integer(), integer(), integer()}.
version() ->
    emdb_drv:version().

-spec strerror(integer()) -> binary().
strerror(Error) ->
    emdb_drv:strerror(Error).

-spec env_create() -> {ok, env_ref()} | {error, any()}.
env_create() ->
    emdb_drv:env_create().

-spec env_open(env_ref(), string()) -> ok | {error, any()}.
env_open(Env, Path) ->
    env_open(Env, Path, []).

-spec env_open(env_ref(), string(), env_options()) -> ok | {error, any()}.
env_open(Env, Path, Opts) ->
    ok = filelib:ensure_dir(Path ++ "/"),
    Flags = parse_env_flags(Opts),
    apply_with_receivor(env_open, [Env, Path, Flags]).

-spec env_close(env_ref()) -> ok | {error, any()}.
env_close(Env) ->
    apply_with_receivor(env_close, [Env]).

-spec env_set_maxdbs(env_ref(), integer()) -> ok | {error, any()}.
env_set_maxdbs(Env, Max) ->
    emdb_drv:env_set_maxdbs(Env, Max).

-spec env_set_flags(env_ref(), env_options()) -> ok | {error, any()}.
env_set_flags(Env, Opts) ->
    Flags = parse_env_flags(Opts),
    emdb_drv:env_set_flags(Env, Flags).

-spec txn_begin(env_ref()) -> {ok, txn_ref()} | {error, any()}.
txn_begin(Env) ->
    txn_begin(Env, []).

-spec txn_begin(env_ref(), txn_options()) -> {ok, txn_ref()} | {error, any()}.
txn_begin(Env, Opts) ->
    Flags = parse_txn_flags(Opts),
    apply_with_receivor(txn_begin, [Env, Flags]).

-spec txn_commit(txn_ref()) -> ok | {error, any()}.
txn_commit(Txn) ->
    apply_with_receivor(txn_commit, [Txn]).

-spec txn_abort(txn_ref()) -> ok | {error, any()}.
txn_abort(Txn) ->
    apply_with_receivor(txn_abort, [Txn]).

-spec txn_reset(txn_ref()) -> ok | {error, any()}.
txn_reset(Txn) ->
    apply_with_receivor(txn_reset, [Txn]).

-spec open(txn_ref(), string()) -> {ok, db_ref()} | {error, any()}.
open(Txn, Name) ->
    open(Txn, Name, []).

-spec open(txn_ref(), string(), open_options()) -> {ok, db_ref()} | {error, any()}.
open(Txn, Name, Opts) ->
    Flags = parse_open_db_flags(Opts),
    apply_with_receivor(open, [Txn, Name, Flags]).

-spec close(db_ref()) -> ok | {error, any()}.
close(DB) ->
    apply_with_receivor(close, [DB]).


-spec get(txn_ref(), db_ref(), binary()) -> {ok, any()} | {error, any()}.
get(Txn, DB, Key) ->
    apply_with_receivor(get, [Txn, DB, Key]).

-spec put(txn_ref(), db_ref(), binary(), binary()) -> ok | {error, any()}.
put(Txn, DB, Key, Value) ->
    ?MODULE:put(Txn, DB, Key, Value, []).

-spec put(txn_ref(), db_ref(), binary(), binary(), put_options()) -> ok | {error, any()}.
put(Txn, DB, Key, Value, Opts) ->
    Flags = parse_put_flags(Opts),
    apply_with_receivor(put, [Txn, DB, Key, Value, Flags]).

-spec del(txn_ref(), db_ref(), binary()) -> ok | {error, any()}.
del(Txn, DB, Key) ->
    del(Txn, DB, Key, undefined).

-spec del(txn_ref(), db_ref(), binary(), binary()) -> ok | {error, any()}.
del(Txn, DB, Key, Value) ->
    apply_with_receivor(del, [Txn, DB, Key, Value]).


cursor_open(Txn, DB) ->
    apply_with_receivor(cursor_open, [Txn, DB]).

cursor_close(Cursor) ->
    apply_with_receivor(cursor_close, [Cursor]).

cursor_get(Cursor, Op) when is_atom(Op) ->
    cursor_get(Cursor, undefined, undefined, Op).

cursor_get(Cursor, Key, Val, Op) when is_atom(Op) ->
    OpCode = atom_to_opcode(Op),
    apply_with_receivor(cursor_get, [Cursor, Key, Val, OpCode]).

cursor_put(Cursor, Key, Val) ->
    cursor_put(Cursor, Key, Val, []).

cursor_put(Cursor, Key, Val, Opts) ->
    Flags = parse_put_flags(Opts),
    apply_with_receivor(cursor_put, [Cursor, Key, Val, Flags]).

cursor_del(Cursor) ->
    cursor_del(Cursor, []).

cursor_del(Cursor, Opts) ->
    Flags = parse_cursor_del_flags(Opts),
    apply_with_receivor(cursor_del, [Cursor, Flags]).

%%====================================================================
%% PRIVATE API
%%====================================================================

apply_with_receivor(Method, Args) ->
    apply_with_receivor(Method, Args, 1000).

apply_with_receivor(Method, Args, Timeout) ->
    Ref = make_ref(),
    Args1 = [Ref, self()|Args],
    case erlang:apply(emdb_drv, Method, Args1) of
        ok    -> receive_answer(Ref, Timeout);
        Error -> Error
    end.

receive_answer(Ref, Timeout) ->
    receive
        {Ref, Resp} -> Resp;
        Other -> throw(Other)
    after Timeout ->
        throw({error, timeout, Ref})
    end.


parse_env_flags(Opts) ->
    lists:foldl(fun
            (fixed_map,    Acc) -> Acc bor ?MDB_FIXEDMAP;
            (no_sub_dir,   Acc) -> Acc bor ?MDB_NOSUBDIR;
            (no_sync,      Acc) -> Acc bor ?MDB_NOSYNC;
            (readonly,     Acc) -> Acc bor ?MDB_RDONLY;
            (no_meta_sync, Acc) -> Acc bor ?MDB_NOMETASYNC;
            (write_map,    Acc) -> Acc bor ?MDB_WRITEMAP;
            (map_async,    Acc) -> Acc bor ?MDB_MAPASYNC;
            (no_tls,       Acc) -> Acc bor ?MDB_NOTLS;
            (_,            Acc) -> Acc
        end, 0, Opts).

parse_txn_flags(Opts) ->
    lists:foldl(fun
            (readonly, Acc) -> Acc bor ?MDB_RDONLY;
            (_,        Acc) -> Acc
        end, 0, Opts).

parse_open_db_flags(Opts) ->
    lists:foldl(fun
            (reverse_key, Acc) -> Acc bor ?MDB_REVERSEKEY;
            (dup_sort,    Acc) -> Acc bor ?MDB_DUPSORT;
            (integer_key, Acc) -> Acc bor ?MDB_INTEGERKEY;
            (dup_fixed,   Acc) -> Acc bor ?MDB_DUPFIXED;
            (integer_dup, Acc) -> Acc bor ?MDB_INTEGERDUP;
            (reverse_dup, Acc) -> Acc bor ?MDB_REVERSEDUP;
            (create,      Acc) -> Acc bor ?MDB_CREATE;
            (_,           Acc) -> Acc
        end, 0, Opts).

parse_put_flags(Opts) ->
    lists:foldl(fun
            (no_overwrite, Acc) -> Acc bor ?MDB_NOOVERWRITE;
            (no_dup_data,  Acc) -> Acc bor ?MDB_NODUPDATA;
            (current,      Acc) -> Acc bor ?MDB_CURRENT;
            (reverse,      Acc) -> Acc bor ?MDB_RESERVE;
            (append,       Acc) -> Acc bor ?MDB_APPEND;
            (append_dup,   Acc) -> Acc bor ?MDB_APPENDDUP;
            (multiple,     Acc) -> Acc bor ?MDB_MULTIPLE;
            (_,            Acc) -> Acc
        end, 0, Opts).

parse_cursor_del_flags(Opts) ->
    lists:foldl(fun
            (no_dup_data,  Acc) -> Acc bor ?MDB_NODUPDATA;
            (_,            Acc) -> Acc
        end, 0, Opts).

atom_to_opcode(first)          -> 0;
atom_to_opcode(first_dup)      -> 1;
atom_to_opcode(get_both)       -> 2;
atom_to_opcode(get_both_range) -> 3;
atom_to_opcode(get_current)    -> 4;
atom_to_opcode(get_multiple)   -> 5;
atom_to_opcode(last)           -> 6;
atom_to_opcode(last_dup)       -> 7;
atom_to_opcode(next)           -> 8;
atom_to_opcode(next_dup)       -> 9;
atom_to_opcode(next_multiple)  -> 10;
atom_to_opcode(next_no_dup)    -> 11;
atom_to_opcode(prev)           -> 12;
atom_to_opcode(prev_dup)       -> 13;
atom_to_opcode(prev_no_dup)    -> 14;
atom_to_opcode(set)            -> 15;
atom_to_opcode(set_key)        -> 16;
atom_to_opcode(set_range)      -> 17;
atom_to_opcode(_)              -> 8.

