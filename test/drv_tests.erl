-module(drv_tests).

-include("emdb.hrl").
-include_lib("eunit/include/eunit.hrl").


version_test() ->
    {X, Y, Z} = emdb_drv:version(),
    ?debugFmt("MDB version: {~p, ~p, ~p}", [X, Y, Z]),
    ok.


strerror_test() ->
    <<"MDB_KEYEXIST: Key/data pair already exists">> = emdb_drv:strerror(-30799),
    <<"MDB_NOTFOUND: No matching key/data pair found">> = emdb_drv:strerror(-30798),
    <<"MDB_BAD_RSLOT: Invalid reuse of reader locktable slot">> = emdb_drv:strerror(-30783).

simple_test() ->
    os:cmd("mkdir ./testdb"),
    Pid = self(),
    {ok, Env} = emdb_drv:env_create(),
    ?debugFmt("Env created: ~p", [Env]),
    try
        ok = emdb_drv:env_set_maxdbs(Env, 4),

        Ref = make_ref(),
        ok = emdb_drv:env_open(Ref, Pid, Env, "./testdb", 0),
        ok = receive_answer(Ref),

        Ref1 = make_ref(),
        ok = emdb_drv:txn_begin(Ref1, Pid, Env, 0),
        {ok, Txn} = receive_answer(Ref1),
        ?debugFmt("Txn created: ~p", [Txn]),

        Ref2 = make_ref(),
        ok = emdb_drv:open(Ref2, Pid, Txn, "db1", ?MDB_CREATE),
        {ok, DB} = receive_answer(Ref2),

        Ref3 = make_ref(),
        ok = emdb_drv:put(Ref3, Pid, Txn, DB, <<"key1">>, <<"data1">>, 0),
        ok = receive_answer(Ref3),

        Ref4 = make_ref(),
        ok = emdb_drv:txn_commit(Ref4, Pid, Txn),
        ok = receive_answer(Ref4),

        Ref5 = make_ref(),
        ok = emdb_drv:txn_begin(Ref5, Pid, Env, 0),
        {ok, Txn1} = receive_answer(Ref5),

        Ref6 = make_ref(),
        ok = emdb_drv:get(Ref6, Pid, Txn1, DB, <<"key1">>),
        {ok, <<"data1">>} = receive_answer(Ref6),

        Ref7 = make_ref(),
        ok = emdb_drv:del(Ref7, Pid, Txn1, DB, <<"key1">>, undefined),
        ok = receive_answer(Ref7),

        Ref8 = make_ref(),
        ok = emdb_drv:txn_commit(Ref8, Pid, Txn1),
        ok = receive_answer(Ref8),

        Ref9 = make_ref(),
        ok = emdb_drv:close(Ref9, Pid, DB),
        ok = receive_answer(Ref9)
    after
        Ref10 = make_ref(),
        ok = emdb_drv:env_close(Ref10, Pid, Env),
        ok = receive_answer(Ref10),
        os:cmd("rm -rf ./testdb")
    end.

receive_answer(Ref) ->
    receive_answer(Ref, 1000).

receive_answer(Ref, Timeout) ->
    receive
        {Ref, Resp} -> Resp;
        Other -> throw(Other)
    after Timeout ->
        throw({error, timeout, Ref})
    end.
