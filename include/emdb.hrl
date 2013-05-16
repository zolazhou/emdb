%%-------------------------------------------------------------------
%% This file is part of EMDB - Erlang MDB API
%%
%% Copyright (c) 2012 by Aleph Archives. All rights reserved.
%%
%%-------------------------------------------------------------------
%% Redistribution and use in source and binary forms, with or without
%% modification, are permitted only as authorized by the OpenLDAP
%% Public License.
%%
%% A copy of this license is available in the file LICENSE in the
%% top-level directory of the distribution or, alternatively, at
%% <http://www.OpenLDAP.org/license.html>.
%%
%% Permission to use, copy, modify, and distribute this software for any
%% purpose with or without fee is hereby granted, provided that the above
%% copyright notice and this permission notice appear in all copies.
%%
%% THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
%% WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
%% MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
%% ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
%% WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
%% ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
%% OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
%%-------------------------------------------------------------------


%% Environment Flags
-define(MDB_FIXEDMAP,   16#01).
-define(MDB_NOSUBDIR,   16#4000).
-define(MDB_NOSYNC,     16#10000).
-define(MDB_RDONLY,     16#20000).
-define(MDB_NOMETASYNC, 16#40000).
-define(MDB_WRITEMAP,   16#80000).
-define(MDB_MAPASYNC,   16#100000).
-define(MDB_NOTLS,      16#200000).

-define(MDB_REVERSEKEY, 16#02).  %% use reverse string keys
-define(MDB_DUPSORT,    16#04).  %% use sorted duplicates
-define(MDB_INTEGERKEY, 16#08).  %% numeric keys in native byte order. The keys must all be of the same size.
-define(MDB_DUPFIXED,   16#10).  %% with #MDB_DUPSORT, sorted dup items have fixed size
-define(MDB_INTEGERDUP, 16#20).  %% with #MDB_DUPSORT, dups are numeric in native byte order
-define(MDB_REVERSEDUP, 16#40).  %% with #MDB_DUPSORT, use reverse string dups
-define(MDB_CREATE,     16#40000).  %% create DB if not already existing

-define(MDB_NOOVERWRITE, 16#10).
-define(MDB_NODUPDATA,   16#20).
-define(MDB_CURRENT,     16#40).
-define(MDB_RESERVE,     16#10000).
-define(MDB_APPEND,      16#20000).
-define(MDB_APPENDDUP,   16#40000).
-define(MDB_MULTIPLE,    16#80000).


-opaque env_ref() :: binary().
-opaque txn_ref() :: binary().
-opaque db_ref() :: binary().
-opaque cursor_ref() :: binary().

-type cursor_op() :: first | first_dup |
                     last | last_dup |
                     get_both | get_both_range | get_current | get_multiple |
                     next | next_dup | next_multiple | next_nodup |
                     prev | prev_dup | prev_nodup |
                     set | set_key | set_range.

-type env_options() :: [fixed_map |
                        no_sub_dir |
                        no_sync |
                        readonly |
                        no_meta_sync |
                        write_map |
                        map_async |
                        no_tls].

-type txn_options() :: [readonly].

-type open_options() :: [reverse_key |
                         dup_sort |
                         integer_key |
                         dup_fixed |
                         integer_dup |
                         reverse_dup |
                         create].

-type put_options() :: [no_overwrite |
                        no_dup_data |
                        current |
                        reverse |
                        append |
                        append_dup |
                        multiple].

-type del_options() :: [no_dup_data].
