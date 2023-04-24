-module(ar_post_block_tests).

-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [start/1, start/2, slave_start/1,
		slave_start/2, connect_to_slave/0, get_tx_anchor/0, disconnect_from_slave/0,
		wait_until_height/1, sign_tx/2, sign_tx/3,
		read_block_when_stored/1, read_block_when_stored/2,
		master_peer/0, slave_peer/0, slave_mine/0, assert_slave_wait_until_height/1,
		slave_call/3, assert_post_tx_to_master/1, assert_post_tx_to_slave/1,
		test_with_mocked_functions/2]).

start_node() ->
	[B0] = ar_weave:init([], 0), %% Set difficulty to 0 to speed up tests
	start(B0),
	slave_start(B0),
	connect_to_slave().

reset_node() ->
	connect_to_slave().

setup_all_post_2_6() ->
	{Setup, Cleanup} = ar_test_node:mock_functions([
		{ar_fork, height_2_6, fun() -> 0 end}
		]),
	Functions = Setup(),
	start_node(),
	{Cleanup, Functions}.

cleanup_all_post_2_6({Cleanup, Functions}) ->
	Cleanup(Functions).

setup_one_post_2_6() ->
	reset_node(),
	Height = slave_height(),
	[{PrevH, _, _} | _] = wait_until_height(Height),
	disconnect_from_slave(),
	slave_mine(),
	[{H, _, _} | _] = ar_test_node:assert_slave_wait_until_height(Height + 1),
	B = slave_call(ar_block_cache, get, [block_cache, H]),
	PrevB = slave_call(ar_block_cache, get, [block_cache, PrevH]),
	{ok, Config} = slave_call(application, get_env, [arweave, config]),
	Key = element(1, slave_call(ar_wallet, load_key, [Config#config.mining_addr])),
	{Key, B, PrevB}.

instantiator(TestFun) ->
	fun (Fixture) -> {timeout, 60, {with, Fixture, [TestFun]}} end.

post_2_6_test_() ->
	{setup, fun setup_all_post_2_6/0, fun cleanup_all_post_2_6/1,
		{foreach, fun setup_one_post_2_6/0, [
			instantiator(fun test_reject_block_invalid_miner_reward/1),
			instantiator(fun test_reject_block_invalid_denomination/1),
			instantiator(fun test_reject_block_invalid_kryder_plus_rate_multiplier/1),
			instantiator(fun test_reject_block_invalid_kryder_plus_rate_multiplier_latch/1),
			instantiator(fun test_reject_block_invalid_endowment_pool/1),
			instantiator(fun test_reject_block_invalid_debt_supply/1),
			instantiator(fun test_reject_block_invalid_wallet_list/1),
			instantiator(fun test_add_external_block_with_invalid_timestamp/1)
		]}
	}.

rejects_invalid_blocks_test_() ->
	ar_test_node:test_with_mocked_functions([{ar_fork, height_2_6, fun() -> 0 end}],
		fun test_rejects_invalid_blocks/0).

test_rejects_invalid_blocks() ->
	[B0] = ar_weave:init([], ar_retarget:switch_to_linear_diff(2)),
	start(B0),
	{_Slave, _} = slave_start(B0),
	disconnect_from_slave(),
	slave_mine(),
	BI = ar_test_node:assert_slave_wait_until_height(1),
	B1 = slave_call(ar_storage, read_block, [hd(BI)]),
	%% Try to post an invalid block.
	InvalidH = crypto:strong_rand_bytes(48),
	ok = ar_events:subscribe(block),
	post_block(B1#block{ indep_hash = InvalidH }, invalid_hash),
	%% Verify the IP address of self is NOT banned in ar_blacklist_middleware.
	InvalidH2 = crypto:strong_rand_bytes(48),
	post_block(B1#block{ indep_hash = InvalidH2 }, invalid_hash),
	%% The valid block with the ID from the failed attempt can still go through.
	post_block(B1, valid),
	%% Try to post the same block again.
	Peer = master_peer(),
	?assertMatch({ok, {{<<"208">>, _}, _, _, _, _}}, send_new_block(Peer, B1)),
	%% Correct hash, but invalid signature.
	B2Preimage = B1#block{ signature = <<>> },
	B2 = B2Preimage#block{ indep_hash = ar_block:indep_hash(B2Preimage) },
	post_block(B2, invalid_signature),
	%% Nonce limiter output too far in the future.
	Info1 = B1#block.nonce_limiter_info,
	{ok, Config} = slave_call(application, get_env, [arweave, config]),
	Key = element(1, slave_call(ar_wallet, load_key, [Config#config.mining_addr])),
	B3 = sign_block(B1#block{
			%% Change the solution hash so that the validator does not go down
			%% the comparing the resigned solution with the cached solution path.
			hash = crypto:strong_rand_bytes(32),
			nonce_limiter_info = Info1#nonce_limiter_info{
				global_step_number = 100000 } }, B0, Key),
	post_block(B3, invalid_nonce_limiter_global_step_number),
	%% Nonce limiter output lower than that of the previous block.
	B4 = sign_block(B1#block{ previous_block = B1#block.indep_hash,
			previous_cumulative_diff = B1#block.cumulative_diff,
			%% Change the solution hash so that the validator does not go down
			%% the comparing the resigned solution with the cached solution path.
			hash = crypto:strong_rand_bytes(32),
			height = B1#block.height + 1,
			nonce_limiter_info = Info1#nonce_limiter_info{ global_step_number = 1 } },
			B1, Key),
	post_block(B4, invalid_nonce_limiter_global_step_number),
	B1SolutionH = B1#block.hash,
	B1SolutionNum = binary:decode_unsigned(B1SolutionH),
	B5 = sign_block(B1#block{ previous_block = B1#block.indep_hash,
			previous_cumulative_diff = B1#block.cumulative_diff,
			height = B1#block.height + 1,
			hash = binary:encode_unsigned(B1SolutionNum - 1) }, B1, Key),
	post_block(B5, invalid_nonce_limiter_global_step_number),
	%% Correct hash, but invalid PoW.
	InvalidKey = ar_wallet:new(),
	InvalidAddr = ar_wallet:to_address(InvalidKey),
	B6 = sign_block(B1#block{ reward_addr = InvalidAddr,
			%% Change the solution hash so that the validator does not go down
			%% the comparing the resigned solution with the cached solution path.
			hash = crypto:strong_rand_bytes(32),
			reward_key = element(2, InvalidKey) }, B0, element(1, InvalidKey)),
	timer:sleep(100 * 2), % ?THROTTLE_BY_IP_INTERVAL_MS * 2
	post_block(B6, [invalid_hash_preimage, invalid_pow]),
	?assertMatch({ok, {{<<"403">>, _}, _,
			<<"IP address blocked due to previous request.">>, _, _}},
			send_new_block(Peer, B1#block{ indep_hash = crypto:strong_rand_bytes(48) })),
	ar_blacklist_middleware:reset(),
	B7 = sign_block(B1#block{
			%% Change the solution hash so that the validator does not go down
			%% the comparing the resigned solution with the cached solution path.
			%% Also, here it changes the block hash (the previous one would be ignored),
			%% because the poa field does not explicitly go in there (the motivation is to have
			%% a "quick pow" step which is quick to validate and somewhat expensive to
			%% forge).
			hash = crypto:strong_rand_bytes(32),
			poa = (B1#block.poa)#poa{ chunk = <<"a">> } }, B0, Key),
	post_block(B7, invalid_pow),
	?assertMatch({ok, {{<<"403">>, _}, _,
			<<"IP address blocked due to previous request.">>, _, _}},
			send_new_block(Peer, B1#block{ indep_hash = crypto:strong_rand_bytes(48) })),
	ar_blacklist_middleware:reset(),
	B8 = sign_block(B1#block{ last_retarget = 100000 }, B0, Key),
	post_block(B8, invalid_last_retarget),
	?assertMatch({ok, {{<<"403">>, _}, _,
			<<"IP address blocked due to previous request.">>, _, _}},
			send_new_block(Peer, B1#block{ indep_hash = crypto:strong_rand_bytes(48) })),
	ar_blacklist_middleware:reset(),
	B9 = sign_block(B1#block{ diff = 100000 }, B0, Key),
	post_block(B9, invalid_difficulty),
	?assertMatch({ok, {{<<"403">>, _}, _,
			<<"IP address blocked due to previous request.">>, _, _}},
			send_new_block(Peer, B1#block{ indep_hash = crypto:strong_rand_bytes(48) })),
	ar_blacklist_middleware:reset(),
	B10 = sign_block(B1#block{
			%% Change the solution hash so that the validator does not go down
			%% the comparing the resigned solution with the cached solution path.
			hash = crypto:strong_rand_bytes(32),
			nonce = 100 }, B0, Key),
	post_block(B10, invalid_nonce),
	?assertMatch({ok, {{<<"403">>, _}, _,
			<<"IP address blocked due to previous request.">>, _, _}},
			send_new_block(Peer, B1#block{ indep_hash = crypto:strong_rand_bytes(48) })),
	ar_blacklist_middleware:reset(),
	B11_1 = sign_block(B1#block{ partition_number = 1 }, B0, Key),
	%% We might get invalid_hash_preimage occasionally, because the partition number
	%% changes H0 which changes the solution hash which may happen to be lower than
	%% the difficulty.
	post_block(B11_1, [invalid_resigned_solution_hash, invalid_hash_preimage]),
	B11 = sign_block(B1#block{
			%% Change the solution hash so that the validator does not go down
			%% the comparing the resigned solution with the cached solution path.
			hash = crypto:strong_rand_bytes(32),
			partition_number = 1 }, B0, Key),
	post_block(B11, [invalid_partition_number, invalid_hash_preimage]),
	?assertMatch({ok, {{<<"403">>, _}, _,
			<<"IP address blocked due to previous request.">>, _, _}},
			send_new_block(Peer, B1#block{ indep_hash = crypto:strong_rand_bytes(48) })),
	ar_blacklist_middleware:reset(),
	B12 = sign_block(B1#block{
			nonce_limiter_info = (B1#block.nonce_limiter_info)#nonce_limiter_info{
					last_step_checkpoints = [crypto:strong_rand_bytes(32)] } }, B0, Key),
	%% Reset the node to the genesis block.
	start(B0),
	ok = ar_events:subscribe(block),
	post_block(B12, invalid_nonce_limiter),
	?assertMatch({ok, {{<<"403">>, _}, _,
			<<"IP address blocked due to previous request.">>, _, _}},
			send_new_block(Peer, B1#block{ indep_hash = crypto:strong_rand_bytes(48) })),
	ar_blacklist_middleware:reset(),
	B13 = sign_block(B1#block{ poa = (B1#block.poa)#poa{ data_path = <<>> } }, B0, Key),
	post_block(B13, invalid_poa),
	?assertMatch({ok, {{<<"403">>, _}, _,
			<<"IP address blocked due to previous request.">>, _, _}},
			send_new_block(Peer, B1#block{ indep_hash = crypto:strong_rand_bytes(48) })),
	ar_blacklist_middleware:reset(),
	B14 = sign_block(B1#block{
			%% Change the solution hash so that the validator does not go down
			%% the comparing the resigned solution with the cached solution path.
			hash = crypto:strong_rand_bytes(32),
			nonce_limiter_info = (B1#block.nonce_limiter_info)#nonce_limiter_info{
					next_seed = crypto:strong_rand_bytes(48) } }, B0, Key),
	post_block(B14, invalid_nonce_limiter_seed_data),
	?assertMatch({ok, {{<<"403">>, _}, _,
			<<"IP address blocked due to previous request.">>, _, _}},
			send_new_block(Peer, B1#block{ indep_hash = crypto:strong_rand_bytes(48) })),
	ar_blacklist_middleware:reset(),
	B15 = sign_block(B1#block{
			%% Change the solution hash so that the validator does not go down
			%% the comparing the resigned solution with the cached solution path.
			hash = crypto:strong_rand_bytes(32),
			nonce_limiter_info = (B1#block.nonce_limiter_info)#nonce_limiter_info{
					partition_upper_bound = 10000000 } }, B0, Key),
	post_block(B15, invalid_nonce_limiter_seed_data),
	?assertMatch({ok, {{<<"403">>, _}, _,
			<<"IP address blocked due to previous request.">>, _, _}},
			send_new_block(Peer, B1#block{ indep_hash = crypto:strong_rand_bytes(48) })),
	ar_blacklist_middleware:reset(),
	B16 = sign_block(B1#block{
			%% Change the solution hash so that the validator does not go down
			%% the comparing the resigned solution with the cached solution path.
			hash = crypto:strong_rand_bytes(32),
			nonce_limiter_info = (B1#block.nonce_limiter_info)#nonce_limiter_info{
					next_partition_upper_bound = 10000000 } }, B0, Key),
	post_block(B16, invalid_nonce_limiter_seed_data),
	?assertMatch({ok, {{<<"403">>, _}, _,
			<<"IP address blocked due to previous request.">>, _, _}},
			send_new_block(Peer, B1#block{ indep_hash = crypto:strong_rand_bytes(48) })),
	ar_blacklist_middleware:reset().

test_reject_block_invalid_denomination({Key, B, PrevB}) ->
	ok = ar_events:subscribe(block),
	B2 = sign_block(B#block{ denomination = 0 }, PrevB, Key),
	post_block(B2, invalid_denomination).

rejects_blocks_with_invalid_double_signing_proof_test_() ->
	ar_test_node:test_with_mocked_functions([{ar_fork, height_2_6, fun() -> 0 end}],
		fun test_reject_block_invalid_double_signing_proof/0).

test_reject_block_invalid_double_signing_proof() ->
	Key0 = ar_wallet:new(),
	Addr0 = ar_wallet:to_address(Key0),
	[B0] = ar_weave:init([{Addr0, ?AR(1000), <<>>}], ar_retarget:switch_to_linear_diff(2)),
	start(B0),
	slave_start(B0),
	disconnect_from_slave(),
	{ok, Config} = slave_call(application, get_env, [arweave, config]),
	ok = ar_events:subscribe(block),
	{Key, _} = FullKey = slave_call(ar_wallet, load_key, [Config#config.mining_addr]),
	TX0 = sign_tx(Key0, #{ target => ar_wallet:to_address(Key), quantity => ?AR(10) }),
	assert_post_tx_to_slave(TX0),
	assert_post_tx_to_master(TX0),
	slave_mine(),
	BI = ar_test_node:assert_slave_wait_until_height(1),
	B1 = slave_call(ar_storage, read_block, [hd(BI)]),
	Random512 = crypto:strong_rand_bytes(512),
	Random64 = crypto:strong_rand_bytes(64),
	InvalidProof = {Random512, Random512, 2, 1, Random64, Random512, 3, 2, Random64},
	B2 = sign_block(B1#block{ double_signing_proof = InvalidProof }, B0, Key),
	post_block(B2, invalid_double_signing_proof_same_signature),
	Random512_2 = crypto:strong_rand_bytes(512),
	InvalidProof_2 = {Random512, Random512, 2, 1, Random64, Random512_2, 3, 2, Random64},
	B2_2 = sign_block(B1#block{ double_signing_proof = InvalidProof_2 }, B0, Key),
	post_block(B2_2, invalid_double_signing_proof_cdiff),
	CDiff = B1#block.cumulative_diff,
	PrevCDiff = B0#block.cumulative_diff,
	SignedH = ar_block:generate_signed_hash(B1),
	Preimage1 = << (B0#block.hash)/binary, SignedH/binary >>,
	Preimage2 = << (B0#block.hash)/binary, (crypto:strong_rand_bytes(32))/binary >>,
	SignaturePreimage = << (ar_serialize:encode_int(CDiff, 16))/binary,
					(ar_serialize:encode_int(PrevCDiff, 16))/binary, Preimage2/binary >>,
	Signature2 = ar_wallet:sign(Key, SignaturePreimage),
	%% We cannot ban ourselves.
	InvalidProof2 = {element(3, Key), B1#block.signature, CDiff, PrevCDiff, Preimage1,
			Signature2, CDiff, PrevCDiff, Preimage2},
	B3 = sign_block(B1#block{ double_signing_proof = InvalidProof2 }, B0, Key),
	post_block(B3, invalid_double_signing_proof_same_address),
	slave_mine(),
	BI2 = ar_test_node:assert_slave_wait_until_height(2),
	{ok, MasterConfig} = application:get_env(arweave, config),
	Key2 = element(1, ar_wallet:load_key(MasterConfig#config.mining_addr)),
	Preimage3 = << (B0#block.hash)/binary, (crypto:strong_rand_bytes(32))/binary >>,
	Preimage4 = << (B0#block.hash)/binary, (crypto:strong_rand_bytes(32))/binary >>,
	SignaturePreimage3 = << (ar_serialize:encode_int(CDiff, 16))/binary,
					(ar_serialize:encode_int(PrevCDiff, 16))/binary, Preimage3/binary >>,
	SignaturePreimage4 = << (ar_serialize:encode_int(CDiff, 16))/binary,
					(ar_serialize:encode_int(PrevCDiff, 16))/binary, Preimage4/binary >>,
	Signature3 = ar_wallet:sign(Key, SignaturePreimage3),
	Signature4 = ar_wallet:sign(Key, SignaturePreimage4),
	%% The account address is not in the reward history.
	InvalidProof3 = {element(3, Key2), Signature3, CDiff, PrevCDiff, Preimage3,
			Signature4, CDiff, PrevCDiff, Preimage4},
	B5 = sign_block(B1#block{ double_signing_proof = InvalidProof3 }, B0, Key),
	post_block(B5, invalid_double_signing_proof_not_in_reward_history),
	connect_to_slave(),
	wait_until_height(2),
	B6 = slave_call(ar_storage, read_block, [hd(BI2)]),
	B7 = sign_block(B6, B1, Key),
	post_block(B7, valid),
	ar_node:mine(),
	BI3 = assert_slave_wait_until_height(3),
	B8 = slave_call(ar_storage, read_block, [hd(BI3)]),
	?assertNotEqual(undefined, B8#block.double_signing_proof),
	RewardAddr = B8#block.reward_addr,
	BannedAddr = ar_wallet:to_address(Key),
	Accounts = ar_wallets:get(B8#block.wallet_list, [BannedAddr, RewardAddr]),
	?assertMatch(#{ BannedAddr := {_, _, 1, false}, RewardAddr := {_, _} }, Accounts),
	%% The banned address may still use their accounts for transfers/uploads.
	Key3 = ar_wallet:new(),
	Target = ar_wallet:to_address(Key3),
	TX1 = sign_tx(FullKey, #{ last_tx => <<>>, quantity => 1, target => Target }),
	TX2 = sign_tx(FullKey, #{ last_tx => get_tx_anchor(), data => <<"a">> }),
	lists:foreach(fun(TX) -> assert_post_tx_to_master(TX) end, [TX1, TX2]),
	ar_node:mine(),
	BI4 = assert_slave_wait_until_height(4),
	B9 = slave_call(ar_storage, read_block, [hd(BI4)]),
	Accounts2 = ar_wallets:get(B9#block.wallet_list, [BannedAddr, Target]),
	TXID = TX2#tx.id,
	?assertEqual(2, length(B9#block.txs)),
	?assertMatch(#{ Target := {1, <<>>}, BannedAddr := {_, TXID, 1, false} }, Accounts2).

test_reject_block_invalid_kryder_plus_rate_multiplier({Key, B, PrevB}) ->
	ok = ar_events:subscribe(block),
	B2 = sign_block(B#block{ kryder_plus_rate_multiplier = 0 }, PrevB, Key),
	post_block(B2, invalid_kryder_plus_rate_multiplier).

test_reject_block_invalid_kryder_plus_rate_multiplier_latch({Key, B, PrevB}) ->
	ok = ar_events:subscribe(block),
	B2 = sign_block(B#block{ kryder_plus_rate_multiplier_latch = 2 }, PrevB, Key),
	post_block(B2, invalid_kryder_plus_rate_multiplier_latch).

test_reject_block_invalid_endowment_pool({Key, B, PrevB}) ->
	ok = ar_events:subscribe(block),
	B2 = sign_block(B#block{ reward_pool = 2 }, PrevB, Key),
	post_block(B2, invalid_reward_pool).

test_reject_block_invalid_debt_supply({Key, B, PrevB}) ->
	ok = ar_events:subscribe(block),
	B2 = sign_block(B#block{ debt_supply = 100000000 }, PrevB, Key),
	post_block(B2, invalid_debt_supply).

test_reject_block_invalid_miner_reward({Key, B, PrevB}) ->
	ok = ar_events:subscribe(block),
	B2 = sign_block(B#block{ reward = 0 }, PrevB, Key),
	post_block(B2, invalid_reward_history_hash),
	HashRate = ar_difficulty:get_hash_rate(B2#block.diff),
	RewardHistory = tl(B2#block.reward_history),
	Addr = B2#block.reward_addr,
	B3 = sign_block(B2#block{
			reward_history_hash = ar_block:reward_history_hash([{Addr, HashRate, 0, 1}
					| RewardHistory]) }, PrevB, Key),
	post_block(B3, invalid_miner_reward).

test_reject_block_invalid_wallet_list({Key, B, PrevB}) ->
	ok = ar_events:subscribe(block),
	B2 = sign_block(B#block{ wallet_list = crypto:strong_rand_bytes(32) }, PrevB, Key),
	post_block(B2, invalid_wallet_list).

post_block(B, ExpectedResult) when not is_list(ExpectedResult) ->
	post_block(B, [ExpectedResult], ar_test_node:master_peer());
post_block(B, ExpectedResults) ->
	post_block(B, ExpectedResults, ar_test_node:master_peer()).

post_block(B, ExpectedResults, Peer) ->
	?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}}, send_new_block(Peer, B)),
	await_post_block(B, ExpectedResults, Peer).

await_post_block(B, ExpectedResults) ->
	await_post_block(B, ExpectedResults, ar_test_node:master_peer()).

await_post_block(#block{ indep_hash = H } = B, ExpectedResults, Peer) ->
	PostGossipFailureCodes = [invalid_denomination,
			invalid_double_signing_proof_same_signature, invalid_double_signing_proof_cdiff,
			invalid_double_signing_proof_same_address,
			invalid_double_signing_proof_not_in_reward_history,
			invalid_double_signing_proof_already_banned,
			invalid_double_signing_proof_invalid_signature,
			mining_address_banned, invalid_account_anchors, invalid_reward_pool,
			invalid_miner_reward, invalid_debt_supply, invalid_reward_history_hash,
			invalid_kryder_plus_rate_multiplier_latch, invalid_kryder_plus_rate_multiplier,
			invalid_wallet_list],
	receive
		{event, block, {rejected, Reason, H, Peer2}} ->
			case lists:member(Reason, PostGossipFailureCodes) of
				true ->
					?assertEqual(no_peer, Peer2);
				false ->
					?assertEqual(Peer, Peer2)
			end,
			case lists:member(Reason, ExpectedResults) of
				true ->
					ok;
				_ ->
					?assert(false, iolist_to_binary(io_lib:format("Unexpected "
							"validation failure: ~p. Expected: ~p.",
							[Reason, ExpectedResults])))
			end;
		{event, block, {new, #block{ indep_hash = H }, #{ source := {peer, Peer} }}} ->
			case ExpectedResults of
				[valid] ->
					ok;
				_ ->
					case lists:any(fun(FailureCode) -> not lists:member(FailureCode,
							PostGossipFailureCodes) end, ExpectedResults) of
						true ->
							?assert(false, iolist_to_binary(io_lib:format("Unexpected "
									"validation success. Expected: ~p.", [ExpectedResults])));
						false ->
							await_post_block(B, ExpectedResults)
					end
			end
	after 5000 ->
			?assert(false, iolist_to_binary(io_lib:format("Timed out. Expected: ~p.",
					[ExpectedResults])))
	end.

sign_block(#block{ cumulative_diff = CDiff } = B, PrevB, Key) ->
	SignedH = ar_block:generate_signed_hash(B),
	PrevCDiff = PrevB#block.cumulative_diff,
	Signature = ar_wallet:sign(Key, << (ar_serialize:encode_int(CDiff, 16))/binary,
		(ar_serialize:encode_int(PrevCDiff, 16))/binary,
		(B#block.previous_solution_hash)/binary, SignedH/binary >>),
	H = ar_block:indep_hash2(SignedH, Signature),
	B#block{ indep_hash = H, signature = Signature }.

test_add_external_block_with_invalid_timestamp({Key, B, PrevB}) ->
	Peer = master_peer(),
	%% Expect the timestamp too far from the future to be rejected.
	FutureTimestampTolerance = ?JOIN_CLOCK_TOLERANCE * 2 + ?CLOCK_DRIFT_MAX,
	TooFarFutureTimestamp = os:system_time(second) + FutureTimestampTolerance + 3,
	B2 = update_block_timestamp(B, PrevB, TooFarFutureTimestamp, Key),
	ok = ar_events:subscribe(block),
	?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}}, send_new_block(Peer, B2)),
	H = B2#block.indep_hash,
	receive
		{event, block, {rejected, invalid_timestamp, H, Peer}} ->
			ok
		after 500 ->
			?assert(false, "Did not receive the rejected block event (invalid_timestamp)")
	end,
	%% Expect the timestamp from the future within the tolerance interval to be accepted.
	OkFutureTimestamp = os:system_time(second) + FutureTimestampTolerance - 3,
	B3 = update_block_timestamp(B, PrevB, OkFutureTimestamp, Key),
	?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}}, send_new_block(Peer, B3)),
	%% Expect the timestamp too far behind the previous timestamp to be rejected.
	PastTimestampTolerance = lists:sum([?JOIN_CLOCK_TOLERANCE * 2, ?CLOCK_DRIFT_MAX]),
	TooFarPastTimestamp = PrevB#block.timestamp - PastTimestampTolerance - 1,
	B4 = update_block_timestamp(B, PrevB, TooFarPastTimestamp, Key),
	?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}}, send_new_block(Peer, B4)),
	H2 = B4#block.indep_hash,
	receive
		{event, block, {rejected, invalid_timestamp, H2, Peer}} ->
			ok
		after 500 ->
			?assert(false, "Did not receive the rejected block event "
					"(invalid_timestamp).")
	end,
	OkPastTimestamp = PrevB#block.timestamp - PastTimestampTolerance + 1,
	B5 = update_block_timestamp(B, PrevB, OkPastTimestamp, Key),
	?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}}, send_new_block(Peer, B5)).

update_block_timestamp(B, PrevB, Timestamp, Key) ->
	sign_block(B#block{ timestamp = Timestamp }, PrevB, Key).

send_block2_test_() ->
	test_with_mocked_functions([{ar_fork, height_2_6, fun() -> 0 end}],
		fun() -> test_send_block2() end).

test_send_block2() ->
	{_, Pub} = Wallet = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(100), <<>>}]),
	MasterWallet = ar_wallet:new_keyfile(),
	MasterAddress = ar_wallet:to_address(MasterWallet),
	SlaveWallet = slave_call(ar_wallet, new_keyfile, []),
	SlaveAddress = ar_wallet:to_address(SlaveWallet),
	start(B0, MasterAddress),
	slave_start(B0, SlaveAddress),
	disconnect_from_slave(),
	TXs = [sign_tx(Wallet, #{ last_tx => get_tx_anchor() }) || _ <- lists:seq(1, 10)],
	lists:foreach(fun(TX) -> assert_post_tx_to_master(TX) end, TXs),
	ar_node:mine(),
	[{H, _, _}, _] = wait_until_height(1),
	B = ar_storage:read_block(H),
	TXs2 = sort_txs_by_block_order(TXs, B),
	EverySecondTX = element(2, lists:foldl(fun(TX, {N, Acc}) when N rem 2 /= 0 ->
			{N + 1, [TX | Acc]}; (_TX, {N, Acc}) -> {N + 1, Acc} end, {0, []}, TXs2)),
	lists:foreach(fun(TX) -> assert_post_tx_to_slave(TX) end, EverySecondTX),
	Announcement = #block_announcement{ indep_hash = B#block.indep_hash,
			previous_block = B0#block.indep_hash,
			tx_prefixes = [binary:part(TX#tx.id, 0, 8) || TX <- TXs2] },
	{ok, {{<<"200">>, _}, _, Body, _, _}} = ar_http:req(#{ method => post,
			peer => slave_peer(), path => "/block_announcement",
			body => ar_serialize:block_announcement_to_binary(Announcement) }),
	Response = ar_serialize:binary_to_block_announcement_response(Body),
	?assertEqual({ok, #block_announcement_response{ missing_chunk = true,
			missing_tx_indices = [0, 2, 4, 6, 8] }}, Response),
	Announcement2 = Announcement#block_announcement{ recall_byte = 0 },
	{ok, {{<<"200">>, _}, _, Body2, _, _}} = ar_http:req(#{ method => post,
			peer => slave_peer(), path => "/block_announcement",
			body => ar_serialize:block_announcement_to_binary(Announcement2) }),
	Response2 = ar_serialize:binary_to_block_announcement_response(Body2),
	?assertEqual({ok, #block_announcement_response{ missing_chunk = false,
			missing_tx_indices = [0, 2, 4, 6, 8] }}, Response2),
	Announcement3 = Announcement#block_announcement{ recall_byte = 100000000000000 },
	{ok, {{<<"200">>, _}, _, Body, _, _}} = ar_http:req(#{ method => post,
			peer => slave_peer(), path => "/block_announcement",
			body => ar_serialize:block_announcement_to_binary(Announcement3) }),
	{ok, {{<<"418">>, _}, _, Body3, _, _}} = ar_http:req(#{ method => post,
			peer => slave_peer(), path => "/block2",
			body => ar_serialize:block_to_binary(B) }),
	?assertEqual(iolist_to_binary(lists:foldl(fun(#tx{ id = TXID }, Acc) -> [TXID | Acc] end,
			[], TXs2 -- EverySecondTX)), Body3),
	B2 = B#block{ txs = [lists:nth(1, TXs2) | tl(B#block.txs)] },
	{ok, {{<<"418">>, _}, _, Body4, _, _}} = ar_http:req(#{ method => post,
			peer => slave_peer(), path => "/block2",
			body => ar_serialize:block_to_binary(B2) }),
	?assertEqual(iolist_to_binary(lists:foldl(fun(#tx{ id = TXID }, Acc) -> [TXID | Acc] end,
			[], (TXs2 -- EverySecondTX) -- [lists:nth(1, TXs2)])), Body4),
	TXs3 = [sign_tx(master, Wallet, #{ last_tx => get_tx_anchor(),
			data => crypto:strong_rand_bytes(10 * 1024) }) || _ <- lists:seq(1, 10)],
	lists:foreach(fun(TX) -> assert_post_tx_to_master(TX) end, TXs3),
	ar_node:mine(),
	[{H2, _, _}, _, _] = wait_until_height(2),
	{ok, {{<<"412">>, _}, _, <<>>, _, _}} = ar_http:req(#{ method => post,
			peer => slave_peer(), path => "/block_announcement",
			body => ar_serialize:block_announcement_to_binary(#block_announcement{
					indep_hash = H2, previous_block = B#block.indep_hash }) }),
	BTXs = ar_storage:read_tx(B#block.txs),
	B3 = B#block{ txs = BTXs },
	{ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} = ar_http:req(#{ method => post,
			peer => slave_peer(), path => "/block2",
			body => ar_serialize:block_to_binary(B3) }),
	{ok, {{<<"200">>, _}, _, SerializedB, _, _}} = ar_http:req(#{ method => get,
			peer => master_peer(), path => "/block2/height/1" }),
	?assertEqual({ok, B}, ar_serialize:binary_to_block(SerializedB)),
	Map = element(2, lists:foldl(fun(TX, {N, M}) -> {N + 1, maps:put(TX#tx.id, N, M)} end,
			{0, #{}}, TXs2)),
	{ok, {{<<"200">>, _}, _, Serialized2B, _, _}} = ar_http:req(#{ method => get,
			peer => master_peer(), path => "/block2/height/1",
			body => << 1:1, 0:(8 * 125 - 1) >> }),
	?assertEqual({ok, B#block{ txs = [case maps:get(TX#tx.id, Map) == 0 of true -> TX;
			_ -> TX#tx.id end || TX <- BTXs] }}, ar_serialize:binary_to_block(Serialized2B)),
	{ok, {{<<"200">>, _}, _, Serialized2B, _, _}} = ar_http:req(#{ method => get,
			peer => master_peer(), path => "/block2/height/1",
			body => << 1:1, 0:7 >> }),
	{ok, {{<<"200">>, _}, _, Serialized3B, _, _}} = ar_http:req(#{ method => get,
			peer => master_peer(), path => "/block2/height/1",
			body => << 0:1, 1:1, 0:1, 1:1, 0:4 >> }),
	?assertEqual({ok, B#block{ txs = [case lists:member(maps:get(TX#tx.id, Map), [1, 3]) of
			true -> TX; _ -> TX#tx.id end || TX <- BTXs] }},
					ar_serialize:binary_to_block(Serialized3B)),
	B4 = read_block_when_stored(H2, true),
	timer:sleep(500),
	{ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} = ar_http:req(#{ method => post,
			peer => slave_peer(), path => "/block2",
			body => ar_serialize:block_to_binary(B4) }),
	connect_to_slave(),
	lists:foreach(
		fun(Height) ->
			ar_node:mine(),
			assert_slave_wait_until_height(Height)
		end,
		lists:seq(3, 3 + ?SEARCH_SPACE_UPPER_BOUND_DEPTH)
	),
	B5 = ar_storage:read_block(ar_node:get_current_block_hash()),
	{ok, {{<<"208">>, _}, _, _, _, _}} = ar_http:req(#{ method => post,
			peer => slave_peer(), path => "/block_announcement",
			body => ar_serialize:block_announcement_to_binary(#block_announcement{
					indep_hash = B5#block.indep_hash,
					previous_block = B5#block.previous_block }) }),
	disconnect_from_slave(),
	ar_node:mine(),
	[_ | _] = wait_until_height(3 + ?SEARCH_SPACE_UPPER_BOUND_DEPTH + 1),
	B6 = ar_storage:read_block(ar_node:get_current_block_hash()),
	{ok, {{<<"200">>, _}, _, Body5, _, _}} = ar_http:req(#{ method => post,
			peer => slave_peer(), path => "/block_announcement",
			body => ar_serialize:block_announcement_to_binary(#block_announcement{
					indep_hash = B6#block.indep_hash,
					previous_block = B6#block.previous_block,
					recall_byte = 0 }) }),
	?assertEqual({ok, #block_announcement_response{ missing_chunk = false,
			missing_tx_indices = [] }},
			ar_serialize:binary_to_block_announcement_response(Body5)),
	{ok, {{<<"200">>, _}, _, Body6, _, _}} = ar_http:req(#{ method => post,
			peer => slave_peer(), path => "/block_announcement",
			body => ar_serialize:block_announcement_to_binary(#block_announcement{
					indep_hash = B6#block.indep_hash,
					previous_block = B6#block.previous_block,
					recall_byte = 1024 }) }),
	?assertEqual({ok, #block_announcement_response{ missing_chunk = false,
			missing_tx_indices = [] }},
			ar_serialize:binary_to_block_announcement_response(Body6)).

resigned_solution_test_() ->
	test_with_mocked_functions([{ar_fork, height_2_6, fun() -> 0 end}],
		fun() -> test_resigned_solution() end).

test_resigned_solution() ->
	[B0] = ar_weave:init(),
	start(B0),
	slave_start(B0),
	connect_to_slave(),
	slave_mine(),
	wait_until_height(1),
	disconnect_from_slave(),
	slave_mine(),
	B = ar_node:get_current_block(),
	{ok, Config} = slave_call(application, get_env, [arweave, config]),
	Key = element(1, slave_call(ar_wallet, load_key, [Config#config.mining_addr])),
	ok = ar_events:subscribe(block),
	B2 = sign_block(B#block{ tags = [<<"tag1">>] }, B0, Key),
	post_block(B2, [valid]),
	B3 = sign_block(B#block{ tags = [<<"tag2">>] }, B0, Key),
	post_block(B3, [valid]),
	assert_slave_wait_until_height(2),
	B4 = slave_call(ar_node, get_current_block, []),
	?assertEqual(B#block.indep_hash, B4#block.previous_block),
	B2H = B2#block.indep_hash,
	?assertNotEqual(B2#block.indep_hash, B4#block.previous_block),
	PrevStepNumber = (B#block.nonce_limiter_info)#nonce_limiter_info.global_step_number,
	PrevInterval = PrevStepNumber div ?NONCE_LIMITER_RESET_FREQUENCY,
	Info4 = B4#block.nonce_limiter_info,
	StepNumber = Info4#nonce_limiter_info.global_step_number,
	Interval = StepNumber div ?NONCE_LIMITER_RESET_FREQUENCY,
	B5 =
		case Interval == PrevInterval of
			true ->
				sign_block(B4#block{
						hash_list_merkle = ar_block:compute_hash_list_merkle(B2),
						previous_block = B2H }, B2, Key);
			false ->
				sign_block(B4#block{ previous_block = B2H,
						hash_list_merkle = ar_block:compute_hash_list_merkle(B2),
						nonce_limiter_info = Info4#nonce_limiter_info{ next_seed = B2H } },
						B2, Key)
		end,
	B5H = B5#block.indep_hash,
	post_block(B5, [valid]),
	[{B5H, _, _}, {B2H, _, _}, _] = wait_until_height(2),
	ar_node:mine(),
	[{B6H, _, _}, _, _, _] = wait_until_height(3),
	connect_to_slave(),
	[{B6H, _, _}, {B5H, _, _}, {B2H, _, _}, _] = assert_slave_wait_until_height(3).

sort_txs_by_block_order(TXs, B) ->
	TXByID = lists:foldl(fun(TX, Acc) -> maps:put(tx_id(TX), TX, Acc) end, #{}, TXs),
	lists:foldr(fun(TX, Acc) -> [maps:get(tx_id(TX), TXByID) | Acc] end, [], B#block.txs).

tx_id(#tx{ id = ID }) ->
	ID;
tx_id(ID) ->
	ID.

send_new_block(Peer, B) ->
	ar_http_iface_client:send_block_binary(Peer, B#block.indep_hash,
			ar_serialize:block_to_binary(B)).

slave_height() ->
	slave_call(ar_node, get_height, []).