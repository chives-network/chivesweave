%%% @doc Module responsible for managing and testing the inflation schedule of 
%%% the Chivesweave main network.
-module(ar_inflation).

-export([calculate/1, calculate_post_15_y1_extra/0]).

-include_lib("chivesweave/include/ar_inflation.hrl").

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Calculate the static reward received for mining a given block.
%% This reward portion depends only on block height, not the number of transactions.
-ifdef(DEBUG).
calculate(_Height) ->
	10.
-else.
calculate(Height) ->
	calculate2(Height).
-endif.

calculate2(Height) ->
	case Height >= ar_fork:height_2_7_0() of
		true ->
			calculate_base_fork_2_7_0(Height);
		false ->
			calculate_base(Height)
	end.

%% @doc Calculate the value used in the ?POST_15_Y1_EXTRA macro.
%% The value is memoized to avoid frequent large computational load.
calculate_post_15_y1_extra() ->
    Pre15 = erlang:trunc(sum_rewards(fun calculate/1, 0, ?FORK_15_HEIGHT)),
    Base = erlang:trunc(sum_rewards(fun calculate_base/1, 0, ?FORK_15_HEIGHT)),
    Post15Diff = Base - Pre15,
    erlang:trunc(Post15Diff / (30 * 24 * 365 - ?FORK_15_HEIGHT)).

%%%===================================================================
%%% Private functions.
%%%===================================================================

calculate_base(Height) ->
	{Ln2Dividend, Ln2Divisor} = ?LN2,
	Dividend = Height * Ln2Dividend,
	Divisor = 30 * 24 * 365 * Ln2Divisor,
	Precision = ?INFLATION_NATURAL_EXPONENT_DECIMAL_FRACTION_PRECISION,
	{EXDividend, EXDivisor} = ar_fraction:natural_exponent({Dividend, Divisor}, Precision),
	?GENESIS_TOKENS
		* ?WINSTON_PER_AR
		* EXDivisor
		* 1
		* Ln2Dividend
		div (
			10
			* 30 * 24 * 365
			* Ln2Divisor
			* EXDividend
		).

calculate_base_fork_2_7_0(Height) ->
	{Ln2Dividend, Ln2Divisor} = ?LN2,
	Dividend = Height * Ln2Dividend,
	Divisor = 120 * 24 * 365 * Ln2Divisor,
	Precision = ?INFLATION_NATURAL_EXPONENT_DECIMAL_FRACTION_PRECISION,
	{EXDividend, EXDivisor} = ar_fraction:natural_exponent({Dividend, Divisor}, Precision),
	?GENESIS_TOKENS
		* ?WINSTON_PER_AR
		* EXDivisor
		* Ln2Dividend
		* 10
		div (
			10
			* 10
			* 120 * 24 * 365
			* Ln2Divisor
			* EXDividend
		).

calculate_base_pre_fork_2_5(Height) ->
	?WINSTON_PER_AR
		* (
			0.2
			* ?GENESIS_TOKENS
			* math:pow(2, -(Height) / ?BLOCK_PER_YEAR)
			* math:log(2)
		)
		/ ?BLOCK_PER_YEAR.

%%%===================================================================
%%% Tests.
%%%===================================================================

%% Test that the within tolerance helper function works as anticipated.
is_in_tolerance_test() ->
    true = is_in_tolerance(100, 100.5, 1),
    false = is_in_tolerance(100, 101.5, 1),
    true = is_in_tolerance(100.9, 100, 1),
    false = is_in_tolerance(101.1, 100, 1),
    true = is_in_tolerance(100.0001, 100, 0.01),
    false = is_in_tolerance(100.0001, 100, 0.00009),
    true = is_in_tolerance(?XWE(100 * 1000000), ?XWE(100 * 1000000) + 10, 0.01).

%%% Calculate and verify per-year expected and actual inflation.

year_1_test_() ->
	{timeout, 60, fun test_year_1/0}.

test_year_1() ->
    true = is_in_tolerance(year_sum_rewards(0), ?XWE(5500000)).

year_2_test_() ->
	{timeout, 60, fun test_year_2/0}.

test_year_2() ->
    true = is_in_tolerance(year_sum_rewards(1), ?XWE(2750000)).

year_3_test() ->
	{timeout, 60, fun test_year_3/0}.

test_year_3() ->
    true = is_in_tolerance(year_sum_rewards(2), ?XWE(1375000)).

year_4_test() ->
	{timeout, 60, fun test_year_4/0}.

test_year_4() ->
    true = is_in_tolerance(year_sum_rewards(3), ?XWE(687500)).

year_5_test() ->
	{timeout, 60, fun test_year_5/0}.

test_year_5() ->
    true = is_in_tolerance(year_sum_rewards(4), ?XWE(343750)).

year_6_test() ->
	{timeout, 60, fun test_year_6/0}.

test_year_6() ->
    true = is_in_tolerance(year_sum_rewards(5), ?XWE(171875)).

year_7_test() ->
	{timeout, 60, fun test_year_7/0}.

test_year_7() ->
    true = is_in_tolerance(year_sum_rewards(6), ?XWE(85937.5)).

year_8_test() ->
	{timeout, 60, fun test_year_8/0}.

test_year_8() ->
    true = is_in_tolerance(year_sum_rewards(7), ?XWE(42968.75)).

year_9_test() ->
	{timeout, 60, fun test_year_9/0}.

test_year_9() ->
    true = is_in_tolerance(year_sum_rewards(8), ?XWE(21484.375)).

year_10_test() ->
	{timeout, 60, fun test_year_10/0}.

test_year_10() ->
    true = is_in_tolerance(year_sum_rewards(9), ?XWE(10742.1875)).

%% @doc Is the value X within TolerancePercent of Y.
is_in_tolerance(X, Y) ->
    is_in_tolerance(X, Y, ?DEFAULT_TOLERANCE_PERCENT).
is_in_tolerance(X, Y, TolerancePercent) ->
    Tolerance = TolerancePercent / 100,
    ( X >= ( Y * (1 - Tolerance ) ) ) and
    ( X =< ( Y + (Y * Tolerance ) ) ).

%% @doc Count the total inflation rewards for a given year.
year_sum_rewards(YearNum) ->
    year_sum_rewards(YearNum, fun calculate2/1).
year_sum_rewards(YearNum, Fun) ->
    sum_rewards(
        Fun,
        (YearNum * ?BLOCKS_PER_YEAR),
        ((YearNum + 1) * ?BLOCKS_PER_YEAR)
    ).

%% @doc Calculate the reward sum between two blocks.
sum_rewards(Fun, Start, End) ->
    lists:sum(lists:map(Fun, lists:seq(Start, End))).
