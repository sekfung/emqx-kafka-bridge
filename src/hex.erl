%%%-------------------------------------------------------------------
%%% @author sekfung
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 11. 10月 2020 8:56 下午
%%%-------------------------------------------------------------------
-module(hex).
-author("sekfung").

-export([bin_to_hexstr/2, bin_to_hexstr/1, hexstr_to_bin/1]).

bin_to_hexstr(Bin, Lowercase) ->
  case Lowercase of
    true ->
      lists:flatten([io_lib:format("~2.16.0b", [X]) ||
        X <- binary_to_list(Bin)]);
    false ->
      lists:flatten([io_lib:format("~2.16.0B", [X]) ||
        X <- binary_to_list(Bin)])
  end.

bin_to_hexstr(Bin) ->
  bin_to_hexstr(Bin, false).



hexstr_to_bin(S) ->
  hexstr_to_bin(S, []).
hexstr_to_bin([], Acc) ->
  list_to_binary(lists:reverse(Acc));
hexstr_to_bin([X, Y | T], Acc) ->
  {ok, [V], []} = io_lib:fread("~16u", [X, Y]),
  hexstr_to_bin(T, [V | Acc]);
hexstr_to_bin([X | T], Acc) ->
  {ok, [V], []} = io_lib:fread("~16u", lists:flatten([X, "0"])),
  hexstr_to_bin(T, [V | Acc]).
