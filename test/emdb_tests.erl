-module(emdb_tests).

-include("emdb.hrl").
-include_lib("eunit/include/eunit.hrl").

simple_test() ->
    os:cmd("mkdir ./testdb"),
    {ok, Env} = emdb:env_create(),
    try
        ok = emdb:env_set_maxdbs(Env, 4),
        ok = emdb:env_open(Env, "./testdb"),
        {ok, Txn} = emdb:txn_begin(Env),
        {ok, DB} = emdb:open(Txn, "db1", [create]),

        ok = emdb:put(Txn, DB, <<"key1">>, <<"data1">>),
        ok = emdb:put(Txn, DB, <<"key2">>, <<"data2">>),
        ok = emdb:put(Txn, DB, <<"key3">>, <<"data3">>),
        ok = emdb:txn_commit(Txn),

        {ok, Txn1} = emdb:txn_begin(Env),
        {ok, <<"data1">>} = emdb:get(Txn1, DB, <<"key1">>),
        {ok, <<"data2">>} = emdb:get(Txn1, DB, <<"key2">>),
        {ok, <<"data3">>} = emdb:get(Txn1, DB, <<"key3">>),
        ok = emdb:del(Txn1, DB, <<"key1">>, undefined),
        ok = emdb:txn_commit(Txn1),

        ok = emdb:close(DB)
    after
        ok = emdb:env_close(Env),
        os:cmd("rm -rf ./testdb")
    end.


cursor_test() ->
    os:cmd("mkdir ./testdb"),
    {ok, Env} = emdb:env_create(),
    try
        ok = emdb:env_set_maxdbs(Env, 4),
        ok = emdb:env_open(Env, "./testdb"),
        {ok, Txn} = emdb:txn_begin(Env),
        {ok, DB}  = emdb:open(Txn, "db1", [create]),

        {ok, Cursor} = emdb:cursor_open(Txn, DB),
        ok = emdb:cursor_put(Cursor, <<"key1">>, <<"data1">>, [no_overwrite]),
        ok = emdb:cursor_put(Cursor, <<"key2">>, <<"data2">>, [no_overwrite]),
        ok = emdb:cursor_put(Cursor, <<"key3">>, <<"data3">>, [no_overwrite]),
        ok = emdb:cursor_close(Cursor),
        ok = emdb:txn_commit(Txn),

        {ok, Txn1} = emdb:txn_begin(Env),
        {ok, Cursor1} = emdb:cursor_open(Txn1, DB),
        {ok, {<<"key1">>, <<"data1">>}} = emdb:cursor_get(Cursor1, next),
        {ok, {<<"key2">>, <<"data2">>}} = emdb:cursor_get(Cursor1, next),
        {ok, {<<"key3">>, <<"data3">>}} = emdb:cursor_get(Cursor1, next),
        ok = emdb:cursor_close(Cursor1),
        ok = emdb:txn_commit(Txn1),

        {ok, Txn2} = emdb:txn_begin(Env),
        {ok, Cursor2} = emdb:cursor_open(Txn2, DB),
        {ok, {<<"key1">>, <<"data1">>}} = emdb:cursor_get(Cursor2, next),
        ok = emdb:cursor_del(Cursor2),
        ok = emdb:cursor_close(Cursor2),
        ok = emdb:txn_commit(Txn2),

        {ok, Txn3} = emdb:txn_begin(Env),
        {error, not_found} = emdb:get(Txn3, DB, <<"key1">>),
        {ok, <<"data2">>} = emdb:get(Txn3, DB, <<"key2">>),
        ok = emdb:txn_commit(Txn3),

        ok = emdb:close(DB)
    after
        ok = emdb:env_close(Env),
        os:cmd("rm -rf ./testdb")
    end.

