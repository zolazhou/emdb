-module(emdb_drv).

%%====================================================================
%% EXPORTS
%%====================================================================
-export([version/0,
         strerror/1]).

-export([env_create/0,
         env_open/5,
         env_close/3,
         env_set_maxdbs/2,
         env_set_flags/2]).

-export([txn_begin/4,
         txn_commit/3,
         txn_abort/3,
         txn_reset/3]).

-export([open/5,
         close/3]).

-export([get/5,
         put/7,
         del/6]).

-export([cursor_open/4,
         cursor_close/3,
         cursor_get/6,
         cursor_put/6,
         cursor_del/4]).

%% internal export (ex. spawn, apply)
-on_load(init/0).

%%====================================================================
%% MACROS
%%====================================================================
-define(EMDB_DRIVER_NAME, "emdb_drv").
-define(NOT_LOADED, not_loaded(?LINE)).


%%====================================================================
%% PUBLIC API
%%====================================================================

-spec version() -> {integer(), integer(), integer()}.
version() ->
    ?NOT_LOADED.

strerror(_Error) when is_integer(_Error) ->
    ?NOT_LOADED.

env_create() ->
    ?NOT_LOADED.

env_open(_Ref, _Pid, _Env, _Path, _Flags) ->
    ?NOT_LOADED.

env_close(_Ref, _Pid, _Env) ->
    ?NOT_LOADED.

env_set_maxdbs(_Env, _MaxDbs) ->
    ?NOT_LOADED.

env_set_flags(_Env, _Flags) ->
    ?NOT_LOADED.


txn_begin(_Ref, _Pid, _Env, _Flags) ->
    ?NOT_LOADED.

txn_commit(_Ref, _Pid, _Txn) ->
    ?NOT_LOADED.

txn_abort(_Ref, _Pid, _Txn) ->
    ?NOT_LOADED.

txn_reset(_Ref, _Pid, _Txn) ->
    ?NOT_LOADED.

open(_Ref, _Pid, _Txn, _Name, _Flags) ->
    ?NOT_LOADED.

close(_Ref, _Pid, _DB) ->
    ?NOT_LOADED.

get(_Ref, _Pid, _Txn, _DB, _Key) ->
    ?NOT_LOADED.

put(_Ref, _Pid, _Txn, _DB, _Key, _Val, _Flags) ->
    ?NOT_LOADED.

del(_Ref, _Pid, _Txn, _DB, _Key, _Val) ->
    ?NOT_LOADED.

cursor_open(_Ref, _Pid, _Txn, _DB) ->
    ?NOT_LOADED.

cursor_close(_Ref, _Pid, _Cursor) ->
    ?NOT_LOADED.

cursor_get(_Ref, _Pid, _Cursor, _Key, _Val, _Op) ->
    ?NOT_LOADED.

cursor_put(_Ref, _Pid, _Cursor, _Key, _Val, _Flags) ->
    ?NOT_LOADED.

cursor_del(_Ref, _Pid, _Cursor, _Flags) ->
    ?NOT_LOADED.

%%====================================================================
%% PRIVATE API
%%====================================================================

%%--------------------------------------------------------------------
%% @doc 
%% @end
%%--------------------------------------------------------------------
init() ->
    PrivDir = case code:priv_dir(?MODULE) of
        {error, _} ->
            EbinDir = filename:dirname(code:which(?MODULE)),
            AppPath = filename:dirname(EbinDir),
            filename:join(AppPath, "priv");
        Path ->
            Path
    end,
    erlang:load_nif(filename:join(PrivDir, ?EMDB_DRIVER_NAME), 0).


%%--------------------------------------------------------------------
%% @doc 
%% @end
%%--------------------------------------------------------------------
not_loaded(Line) ->
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, Line}]}).
