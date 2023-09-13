%%%
%%% @doc The module defines Chivesweave hard forks' heights.
%%%

-module(ar_fork).

-export([height_1_6/0, height_1_7/0, height_1_8/0, height_1_9/0, height_2_0/0, height_2_2/0,
		height_2_3/0, height_2_4/0, height_2_5/0, height_2_6/0, height_2_6_8/0]).

-include_lib("chivesweave/include/ar.hrl").
-include_lib("chivesweave/include/ar_consensus.hrl").

-ifdef(FORKS_RESET).
height_1_6() ->
	0.
-else.
height_1_6() ->
	0.
-endif.

-ifdef(FORKS_RESET).
height_1_7() ->
	0.
-else.
height_1_7() ->
	0. % Targeting 2019-07-08 UTC
-endif.

-ifdef(FORKS_RESET).
height_1_8() ->
	0.
-else.
height_1_8() ->
	0. % Targeting 2019-08-29 UTC
-endif.

-ifdef(FORKS_RESET).
height_1_9() ->
	0.
-else.
height_1_9() ->
	0. % Targeting 2019-11-04 UTC
-endif.

-ifdef(FORKS_RESET).
height_2_0() ->
	0.
-else.
height_2_0() ->
	0. % Targeting 2020-04-09 10:00 UTC
-endif.

-ifdef(FORKS_RESET).
height_2_2() ->
	0.
-else.
height_2_2() ->
	0. % Targeting 2020-10-21 13:00 UTC
-endif.

-ifdef(FORKS_RESET).
height_2_3() ->
	0.
-else.
height_2_3() ->
	0. % Targeting 2020-12-21 11:00 UTC
-endif.

-ifdef(FORKS_RESET).
height_2_4() ->
	0.
-else.
height_2_4() ->
	0. % Targeting 2021-02-24 11:50 UTC
-endif.

-ifdef(FORKS_RESET).
height_2_5() ->
	0.
-else.
height_2_5() ->
	0.
-endif.

-ifdef(FORKS_RESET).
height_2_6() ->
	0.
-else.
	-ifdef(TESTNET).
		height_2_6() ->
			0.
	-else.
		height_2_6() ->
			0. % Targeting 2023-03-06 14:00 UTC
	-endif.
-endif.

-ifdef(FORKS_RESET).
height_2_6_8() ->
	0.
-else.
	-ifdef(TESTNET).
		height_2_6_8() ->
			1189000.
	-else.
		height_2_6_8() ->
			1189560. % Targeting 2023-05-30 16:00 UTC
	-endif.
-endif.
