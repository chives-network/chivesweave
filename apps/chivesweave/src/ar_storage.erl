-module(ar_storage).

-behaviour(gen_server).

-export([start_link/0, read_block_index/0, read_reward_history/1, read_block_time_history/2,
		store_block_index/1, update_block_index/3,
		store_reward_history_part/1, store_reward_history_part2/1,
		store_block_time_history_part/2, store_block_time_history_part2/1,
		write_full_block/2, read_block/1, read_block/2, write_tx/1,
		read_tx/1, read_tx_data/1, update_confirmation_index/1, get_tx_confirmation_data/1,
		read_wallet_list/1, write_wallet_list/2,
		delete_blacklisted_tx/1, lookup_tx_filename/1,
		wallet_list_filepath/1, tx_filepath/1, tx_data_filepath/1, read_tx_file/1,
		read_migrated_v1_tx_file/1, ensure_directories/1, write_file_atomic/2,
		write_term/2, write_term/3, read_term/1, read_term/2, delete_term/1, is_file/1,
		migrate_tx_record/1, migrate_block_record/1, read_account/2,
		read_txsrecord_function/1, read_txs_by_addr/3, read_txrecord_by_txid/1, read_txsrecord_by_addr/3, 
		read_data_by_addr/3, read_datarecord_by_addr/3, read_txsrecord_by_addr_deposits/3, read_txsrecord_by_addr_send/3, 
		take_first_n_chars/2, read_block_from_height_by_number/3, read_statistics_network/0, read_statistics_data/0, read_statistics_block/0, 	
		read_statistics_address/0, read_statistics_transaction/0, read_datarecord_function/1, 
		get_mempool_tx_data_records/1, get_mempool_tx_send_records/1, get_mempool_tx_deposits_records/1, 
		get_mempool_tx_txs_records/1, get_mempool_tx_txs_records/0, 
		image_thumbnail_compress_to_storage/3, 
		pdf_office_thumbnail_png_to_storage/3, 
		video_thumbnail_png_to_storage/3,
		contentTypeToFileType/1,
		file_to_thumbnail_data/4, file_to_pdf_data/4,
		find_value_in_tags/2, get_address_txs/1,
		parse_bundle_data/5, read_txs_and_into_parse_bundle_list/1, parse_bundle_tx_from_list/1,
		office_format_convert_to_docx_storage/3, office_format_convert_to_xlsx_storage/3, office_format_convert_to_pdf_storage/3
	]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("chivesweave/include/ar.hrl").
-include_lib("chivesweave/include/ar_config.hrl").
-include_lib("chivesweave/include/ar_wallets.hrl").
-include_lib("chivesweave/include/ar_pricing.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").

-record(state, {}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Read the entire stored block index.
read_block_index() ->
	%% Use a key that is bigger than any << Height:256 >> (<<"a">> > << Height:256 >>)
	%% to retrieve the largest stored Height.
	case ar_kv:get_prev(block_index_db, <<"a">>) of
		none ->
			not_found;
		{ok, << Height:256 >>, V} ->
			{ok, Map} = ar_kv:get_range(block_index_db, << 0:256 >>, << (Height - 1):256 >>),
			read_block_index_from_map(maps:put(<< Height:256 >>, V, Map), 0, Height, <<>>, [])
	end.

read_block_index_from_map(_Map, Height, End, _PrevH, BI) when Height > End ->
	BI;
read_block_index_from_map(Map, Height, End, PrevH, BI) ->
	V = maps:get(<< Height:256 >>, Map, not_found),
	case V of
		not_found ->
			ar:console("The stored block index is invalid. Height ~B not found.~n", [Height]),
			not_found;
		_ ->
			case binary_to_term(V) of
				{H, WeaveSize, TXRoot, PrevH} ->
					read_block_index_from_map(Map, Height + 1, End, H, [{H, WeaveSize, TXRoot} | BI]);
				{_, _, _, PrevH2} ->
					ar:console("The stored block index is invalid. Height: ~B, "
							"stored previous hash: ~s, expected previous hash: ~s.~n",
							[Height, ar_util:encode(PrevH2), ar_util:encode(PrevH)]),
					not_found
			end
	end.

%% @doc Return the reward history for the given block index part or not_found.
read_reward_history([]) ->
	[];
read_reward_history([{H, _WeaveSize, _TXRoot} | BI]) ->
	case read_reward_history(BI) of
		not_found ->
			not_found;
		History ->
			case ar_kv:get(reward_history_db, H) of
				not_found ->
					not_found;
				{ok, Bin} ->
					Element = binary_to_term(Bin),
					[Element | History]
			end
	end.

%% @doc Return the block time history for the given block index part or not_found.
read_block_time_history(_Height, []) ->
	[];
read_block_time_history(Height, [{H, _WeaveSize, _TXRoot} | BI]) ->
	case Height < ar_fork:height_2_7() of
		true ->
			[];
		false ->
			case read_block_time_history(Height - 1, BI) of
				not_found ->
					not_found;
				History ->
					case ar_kv:get(block_time_history_db, H) of
						not_found ->
							not_found;
						{ok, Bin} ->
							Element = binary_to_term(Bin),
							[Element | History]
					end
			end
	end.

%% @doc Record the entire block index on disk.
%% Return {error, block_index_no_recent_intersection} if the local state forks away
%% at more than ?STORE_BLOCKS_BEHIND_CURRENT blocks ago.
store_block_index(BI) ->
	%% Use a key that is bigger than any << Height:256 >> (<<"a">> > << Height:256 >>)
	%% to retrieve the largest stored Height.
	case ar_kv:get_prev(block_index_db, <<"a">>) of
		none ->
			update_block_index(0, 0, lists:reverse(BI));
		{ok, << Height:256 >>, _V} ->
			Height2 = length(BI) - 1,
			Height3 = max(0, min(Height, Height2) - ?STORE_BLOCKS_BEHIND_CURRENT),
			{ok, V} = ar_kv:get(block_index_db, << Height3:256 >>),
			{H, WeaveSize, TXRoot} = lists:nth(Height2 - Height3 + 1, BI),
			case binary_to_term(V) of
				{H, WeaveSize, TXRoot, _PrevH} ->
					BI2 = lists:reverse(lists:sublist(BI, Height2 - Height3)),
					update_block_index(Height, Height - Height3, BI2);
				{H2, _, _, _} ->
					?LOG_ERROR([{event, failed_to_store_block_index},
							{reason, no_intersection},
							{height, Height3},
							{stored_hash, ar_util:encode(H2)},
							{expected_hash, ar_util:encode(H)}]),
					{error, block_index_no_recent_intersection}
			end;
		Error ->
			Error
	end.

%% @doc Record the block index update on disk. Remove the orphans, if any.
update_block_index(TipHeight, OrphanCount, BI) ->
	%% Height of the earliest orphaned block. This height and higher will be deleted from
	%% the block index. If OrphanCount is 0, then no blocks will be deleted from the block index.
	OrphanHeight = TipHeight - OrphanCount + 1,
	%% Record the contents of BI starting at this height. Whether there are 1 or 0 orphans
	%% we update the index starting at the same height (the tip). Only when OrphanCount is > 1 do
	%% need to rewrite the index starting at a lower height.
	IndexHeight = TipHeight - max(0, OrphanCount-1),
	%% 1. Delete all the orphaned blocks from the block index
	case ar_kv:delete_range(block_index_db,
			<< OrphanHeight:256 >>, << (TipHeight + 1):256 >>) of
		ok ->
			case IndexHeight of
				0 ->
					update_block_index2(0, <<>>, BI);
				_ ->
					%% 2. Add all the entries in BI to the block index
					%% BI will include the new tip block at the current height, as well as any new
					%% history blocks if the tip is on a new branch.
					case ar_kv:get(block_index_db, << (IndexHeight - 1):256 >>) of
						not_found ->
							?LOG_ERROR([{event, failed_to_update_block_index},
									{reason, prev_element_not_found},
									{prev_height, IndexHeight - 1}]),
							{error, not_found};
						{ok, Bin} ->
							{PrevH, _, _, _} = binary_to_term(Bin),
							update_block_index2(IndexHeight, PrevH, BI)
					end
			end;
		{error, Error} ->
			?LOG_ERROR([{event, failed_to_update_block_index},
					{reason, failed_to_remove_orphaned_range},
					{range_start, OrphanHeight},
					{range_end, TipHeight + 1},
					{reason, io_lib:format("~p", [Error])}]),
			{error, Error}
	end.

update_block_index2(_Height, _PrevH, []) ->
	ok;
update_block_index2(Height, PrevH, [{H, WeaveSize, TXRoot} | BI]) ->
	Bin = term_to_binary({H, WeaveSize, TXRoot, PrevH}),
	case ar_kv:put(block_index_db, << Height:256 >>, Bin) of
		ok ->
			update_block_index2(Height + 1, H, BI);
		Error ->
			?LOG_ERROR([{event, failed_to_update_block_index},
					{height, Height},
					{reason, io_lib:format("~p", [Error])}]),
			{error, Error}
	end.

store_reward_history_part([]) ->
	ok;
store_reward_history_part(Blocks) ->
	store_reward_history_part2([{B#block.indep_hash, {B#block.reward_addr,
			ar_difficulty:get_hash_rate(B#block.diff), ar_pricing:reset_block_reward_by_height(B#block.height, B#block.reward),
			B#block.denomination}} || B <- Blocks]).

store_reward_history_part2([]) ->
	ok;
store_reward_history_part2([{H, El} | History]) ->
	Bin = term_to_binary(El),
	case ar_kv:put(reward_history_db, H, Bin) of
		ok ->
			store_reward_history_part2(History);
		Error ->
			?LOG_ERROR([{event, failed_to_update_reward_history},
					{reason, io_lib:format("~p", [Error])},
					{block, ar_util:encode(H)}]),
			{error, not_found}
	end.

store_block_time_history_part([], _PrevB) ->
	ok;
store_block_time_history_part(Blocks, PrevB) ->
	History = get_block_time_history_from_blocks(Blocks, PrevB),
	store_block_time_history_part2(History).

get_block_time_history_from_blocks([], _PrevB) ->
	[];
get_block_time_history_from_blocks([B | Blocks], PrevB) ->
	case B#block.height >= ar_fork:height_2_7() of
		false ->
			get_block_time_history_from_blocks(Blocks, B);
		true ->
			[{B#block.indep_hash, ar_block:get_block_time_history_element(B, PrevB)}
					| get_block_time_history_from_blocks(Blocks, B)]
	end.

store_block_time_history_part2([]) ->
	ok;
store_block_time_history_part2([{H, El} | History]) ->
	Bin = term_to_binary(El),
	case ar_kv:put(block_time_history_db, H, Bin) of
		ok ->
			store_block_time_history_part2(History);
		Error ->
			?LOG_ERROR([{event, failed_to_update_block_time_history},
					{reason, io_lib:format("~p", [Error])},
					{block, ar_util:encode(H)}]),
			{error, not_found}
	end.

-if(?NETWORK_NAME == "chivesweave.mainnet.1").
write_full_block(BShadow, TXs) ->
	case update_confirmation_index(BShadow#block{ txs = TXs }) of
		ok ->
			case write_tx([TX || TX <- TXs, not is_blacklisted(TX)]) of
				ok ->
					write_full_block2(BShadow, TXs);
				Error ->
					Error
			end;
		Error ->
			Error
	end.
-else.
write_full_block(BShadow, TXs) ->
	case update_confirmation_index(BShadow#block{ txs = TXs }) of
		ok ->
			case write_tx([TX || TX <- TXs, not is_blacklisted(TX)]) of
				ok ->
					write_full_block2(BShadow, TXs);
				Error ->
					Error
			end;
		Error ->
			Error
	end.
-endif.

is_blacklisted(#tx{ format = 2 }) ->
	false;
is_blacklisted(#tx{ id = TXID }) ->
	xwe_tx_blacklist:is_tx_blacklisted(TXID).

update_confirmation_index(B) ->
	{ok, Config} = application:get_env(chivesweave, config),
	put_tx_confirmation_data(B),
	case lists:member(arql_tags_index, Config#config.enable) of
		true ->
			ar_arql_db:insert_full_block(B, store_tags);
		false ->
			ok
	end.

put_tx_confirmation_data(B) ->
	Data = term_to_binary({B#block.height, B#block.indep_hash}),
	lists:foldl(
		fun	(TX, ok) ->
				ar_kv:put(tx_confirmation_db, TX#tx.id, Data);
			(_TX, Acc) ->
				Acc
		end,
		ok,
		B#block.txs
	).

%% @doc Return {BlockHeight, BlockHash} belonging to the block where
%% the given transaction was included.
get_tx_confirmation_data(TXID) ->
	case ar_kv:get(tx_confirmation_db, TXID) of
		{ok, Binary} ->
			{ok, binary_to_term(Binary)};
		not_found ->
			{ok, Config} = application:get_env(chivesweave, config),
			case lists:member(arql, Config#config.disable) of
				true ->
					not_found;
				_ ->
					case catch ar_arql_db:select_block_by_tx_id(ar_util:encode(TXID)) of
						{ok, #{
							height := Height,
							indep_hash := EncodedIndepHash
						}} ->
							{ok, {Height, ar_util:decode(EncodedIndepHash)}};
						not_found ->
							not_found;
						{'EXIT', {timeout, {gen_server, call, [ar_arql_db, _]}}} ->
							{error, timeout}
					end
			end
	end.

%% @doc Read a block from disk, given a height
%% and a block index (used to determine the hash by height).
read_block(Height, BI) when is_integer(Height) ->
	case Height of
		_ when Height < 0 ->
			unavailable;
		_ when Height > length(BI) - 1 ->
			unavailable;
		_ ->
			{H, _, _} = lists:nth(length(BI) - Height, BI),
			read_block(H)
	end;
read_block(H, _BI) ->
	read_block(H).

%% @doc Read a block from disk, given a hash, a height, or a block index entry.
read_block(unavailable) ->
	unavailable;
read_block(B) when is_record(B, block) ->
	B;
read_block(Blocks) when is_list(Blocks) ->
	lists:map(fun(B) -> read_block(B) end, Blocks);
read_block({H, _, _}) ->
	read_block(H);
read_block(BH) ->
	case ar_disk_cache:lookup_block_filename(BH) of
		{ok, {Filename, Encoding}} ->
			%% The cache keeps a rotated number of recent headers when the
			%% node is out of disk space.
			read_block_from_file(Filename, Encoding);
		_ ->
			case ar_kv:get(block_db, BH) of
				not_found ->
					case lookup_block_filename(BH) of
						unavailable ->
							unavailable;
						{Filename, Encoding} ->
							read_block_from_file(Filename, Encoding)
					end;
				{ok, V} ->
					parse_block_kv_binary(V);
				{error, Reason} ->
					?LOG_WARNING([{event, error_reading_block_from_kv_storage},
							{block, ar_util:encode(BH)},
							{error, io_lib:format("~p", [Reason])}])
			end
	end.

%% @doc Read the account information for the given address and
%% root hash of the account tree. Return {0, <<>>} if the given address does not belong
%% to the tree. The balance may be also 0 when the address exists in the tree. Return
%% not_found if some of the files with the account data are missing.
read_account(Addr, Key) ->
	read_account(Addr, Key, <<>>).

read_account(Addr, Key, Prefix) ->
	case get_account_tree_value(Key, Prefix) of
		{ok, << Key:48/binary, _/binary >>, V} ->
			case binary_to_term(V) of
				{K, Val} when K == Addr ->
					Val;
				{_, _} ->
					{0, <<>>};
				[_ | _] = SubTrees ->
					case find_key_by_matching_longest_prefix(Addr, SubTrees) of
						not_found ->
							{0, <<>>};
						{H, Prefix2} ->
							read_account(Addr, H, Prefix2)
					end
			end;
		_ ->
			read_account2(Addr, Key)
	end.

find_key_by_matching_longest_prefix(Addr, Keys) ->
	find_key_by_matching_longest_prefix(Addr, Keys, {<<>>, -1}).

find_key_by_matching_longest_prefix(_Addr, [], {Key, Prefix}) ->
	case Key of
		<<>> ->
			not_found;
		_ ->
			{Key, Prefix}
	end;
find_key_by_matching_longest_prefix(Addr, [{_, Prefix} | Keys], {Key, KeyPrefix})
		when Prefix == <<>> orelse byte_size(Prefix) =< byte_size(KeyPrefix) ->
	find_key_by_matching_longest_prefix(Addr, Keys, {Key, KeyPrefix});
find_key_by_matching_longest_prefix(Addr, [{H, Prefix} | Keys], {Key, KeyPrefix}) ->
	case binary:match(Addr, Prefix) of
		{0, _} ->
			find_key_by_matching_longest_prefix(Addr, Keys, {H, Prefix});
		_ ->
			find_key_by_matching_longest_prefix(Addr, Keys, {Key, KeyPrefix})
	end.

read_account2(Addr, RootHash) ->
	%% Unfortunately, we do not have an easy access to the information about how many
	%% accounts there were in the given tree so we perform the binary search starting
	%% from the number in the latest block.
	Size = ar_wallets:get_size(),
	MaxFileCount = Size div ?WALLET_LIST_CHUNK_SIZE + 1,
	{ok, Config} = application:get_env(chivesweave, config),
	read_account(Addr, RootHash, 0, MaxFileCount, Config#config.data_dir, false).

read_account(_Addr, _RootHash, Left, Right, _DataDir, _RightFileFound) when Left == Right ->
	not_found;
read_account(Addr, RootHash, Left, Right, DataDir, RightFileFound) ->
	Pos = Left + (Right - Left) div 2,
	Filepath = wallet_list_chunk_relative_filepath(Pos * ?WALLET_LIST_CHUNK_SIZE, RootHash),
	case filelib:is_file(filename:join(DataDir, Filepath)) of
		false ->
			read_account(Addr, RootHash, Left, Pos, DataDir, false);
		true ->
			{ok, L} = ar_storage:read_term(Filepath),
			read_account2(Addr, RootHash, Pos, Left, Right, DataDir, L, RightFileFound)
	end.

wallet_list_chunk_relative_filepath(Position, RootHash) ->
	binary_to_list(iolist_to_binary([
		?WALLET_LIST_DIR,
		"/",
		ar_util:encode(RootHash),
		"-",
		integer_to_binary(Position),
		"-",
		integer_to_binary(?WALLET_LIST_CHUNK_SIZE)
	])).

read_account2(Addr, _RootHash, _Pos, _Left, _Right, _DataDir, [last, {LargestAddr, _} | _L],
		_RightFileFound) when Addr > LargestAddr ->
	{0, <<>>};
read_account2(Addr, RootHash, Pos, Left, Right, DataDir, [last | L], RightFileFound) ->
	read_account2(Addr, RootHash, Pos, Left, Right, DataDir, L, RightFileFound);
read_account2(Addr, RootHash, Pos, _Left, Right, DataDir, [{LargestAddr, _} | _L],
		RightFileFound) when Addr > LargestAddr ->
	case Pos + 1 == Right of
		true ->
			case RightFileFound of
				true ->
					{0, <<>>};
				false ->
					not_found
			end;
		false ->
			read_account(Addr, RootHash, Pos, Right, DataDir, RightFileFound)
	end;
read_account2(Addr, RootHash, Pos, Left, _Right, DataDir, L, _RightFileFound) ->
	case Addr < element(1, lists:last(L)) of
		true ->
			case Pos == Left of
				true ->
					{0, <<>>};
				false ->
					read_account(Addr, RootHash, Left, Pos, DataDir, true)
			end;
		false ->
			case lists:search(fun({Addr2, _}) -> Addr2 == Addr end, L) of
				{value, {Addr, Data}} ->
					Data;
				false ->
					{0, <<>>}
			end
	end.

lookup_block_filename(H) ->
	{ok, Config} = application:get_env(chivesweave, config),
	Name = filename:join([Config#config.data_dir, ?BLOCK_DIR,
			binary_to_list(ar_util:encode(H))]),
	NameJSON = iolist_to_binary([Name, ".json"]),
	case is_file(NameJSON) of
		true ->
			{NameJSON, json};
		false ->
			NameBin = iolist_to_binary([Name, ".bin"]),
			case is_file(NameBin) of
				true ->
					{NameBin, binary};
				false ->
					unavailable
			end
	end.

%% @doc Delete the blacklisted tx with the given hash from disk. Return {ok, BytesRemoved} if
%% the removal is successful or the file does not exist. The reported number of removed
%% bytes does not include the migrated v1 data. The removal of migrated v1 data is requested
%% from ar_data_sync asynchronously. The v2 headers are not removed.
delete_blacklisted_tx(Hash) ->
	case ar_kv:get(tx_db, Hash) of
		{ok, V} ->
			TX = parse_tx_kv_binary(V),
			case TX#tx.format == 1 andalso TX#tx.data_size > 0 of
				true ->
					case ar_kv:delete(tx_db, Hash) of
						ok ->
							{ok, byte_size(V)};
						Error ->
							Error
					end;
				_ ->
					{ok, 0}
			end;
		{error, _} = DBError ->
			DBError;
		not_found ->
			case lookup_tx_filename(Hash) of
				{Status, Filename} ->
					case Status of
						migrated_v1 ->
							case file:read_file_info(Filename) of
								{ok, FileInfo} ->
									case file:delete(Filename) of
										ok ->
											{ok, FileInfo#file_info.size};
										Error ->
											Error
									end;
								Error ->
									Error
							end;
						_ ->
							{ok, 0}
					end;
				unavailable ->
					{ok, 0}
			end
	end.

parse_tx_kv_binary(Bin) ->
	case catch ar_serialize:binary_to_tx(Bin) of
		{ok, TX} ->
			TX;
		_ ->
			migrate_tx_record(binary_to_term(Bin))
	end.

%% Convert the stored tx record to its latest state in the code
%% (assign the default values to all missing fields). Since the version introducing
%% the fork 2.6, the transactions are serialized via ar_serialize:tx_to_binary/1, which
%% is maintained compatible with all past versions, so this code is only used
%% on the nodes synced before the corresponding release.
migrate_tx_record(#tx{} = TX) ->
	TX;
migrate_tx_record({tx, Format, ID, LastTX, Owner, Tags, Target, Quantity, Data,
		DataSize, DataTree, DataRoot, Signature, Reward}) ->
	#tx{ format = Format, id = ID, last_tx = LastTX,
			owner = Owner, tags = Tags, target = Target, quantity = Quantity,
			data = Data, data_size = DataSize, data_root = DataRoot,
			signature = Signature, signature_type = ?DEFAULT_KEY_TYPE,
			reward = Reward, data_tree = DataTree }.

parse_block_kv_binary(Bin) ->
	case catch ar_serialize:binary_to_block(Bin) of
		{ok, B} ->
			B;
		_ ->
			migrate_block_record(binary_to_term(Bin))
	end.

%% Convert the stored block record to its latest state in the code
%% (assign the default values to all missing fields). Since the version introducing
%% the fork 2.6, the blocks are serialized via ar_serialize:block_to_binary/1, which
%% is maintained compatible with all past block versions, so this code is only used
%% on the nodes synced before the corresponding release.
migrate_block_record(#block{} = B) ->
	B;
migrate_block_record({block, Nonce, PrevH, TS, Last, Diff, Height, Hash, H,
		TXs, TXRoot, TXTree, HL, HLMerkle, WL, RewardAddr, Tags, RewardPool,
		WeaveSize, BlockSize, CDiff, SizeTaggedTXs, PoA, Rate, ScheduledRate,
		Packing_2_5_Threshold, StrictDataSplitThreshold}) ->
	#block{ nonce = Nonce, previous_block = PrevH, timestamp = TS,
			last_retarget = Last, diff = Diff, height = Height, hash = Hash,
			indep_hash = H, txs = TXs, tx_root = TXRoot, tx_tree = TXTree,
			hash_list = HL, hash_list_merkle = HLMerkle, wallet_list = WL,
			reward_addr = RewardAddr, tags = Tags, reward_pool = RewardPool,
			weave_size = WeaveSize, block_size = BlockSize, cumulative_diff = CDiff,
			size_tagged_txs = SizeTaggedTXs, poa = PoA, usd_to_ar_rate = Rate,
			scheduled_usd_to_ar_rate = ScheduledRate,
			packing_2_5_threshold = Packing_2_5_Threshold,
			strict_data_split_threshold = StrictDataSplitThreshold }.

write_tx(TXs) when is_list(TXs) ->
	lists:foldl(
		fun (TX, ok) ->
				write_tx(TX);
			(_TX, Acc) ->
				Acc
		end,
		ok,
		TXs
	);
write_tx(#tx{ format = Format, id = TXID } = TX) ->
	case write_tx_header(TX) of
		ok ->
			DataSize = byte_size(TX#tx.data),
			case DataSize > 0 of
				true ->
					case {DataSize == TX#tx.data_size, Format} of
						{false, 2} ->
							?LOG_ERROR([{event, failed_to_store_tx_data},
									{reason, size_mismatch}, {tx, ar_util:encode(TX#tx.id)}]),
							ok;
						{true, 1} ->
							case write_tx_data(no_expected_data_root, TX#tx.data, TXID) of
								ok ->
									ok;
								{error, Reason} ->
									?LOG_WARNING([{event, failed_to_store_tx_data},
											{reason, Reason}, {tx, ar_util:encode(TX#tx.id)}]),
									%% We have stored the data in the tx_db table
									%% so we return ok here.
									ok
							end;
						{true, 2} ->
							case xwe_tx_blacklist:is_tx_blacklisted(TX#tx.id) of
								true ->
									ok;
								false ->
									case write_tx_data(TX#tx.data_root, TX#tx.data, TXID) of
										ok ->
											ok;
										{error, Reason} ->
											%% v2 data is not part of the header. We have to
											%% report success here even if we failed to store
											%% the attached data.
											?LOG_WARNING([{event, failed_to_store_tx_data},
													{reason, Reason},
													{tx, ar_util:encode(TX#tx.id)}]),
											ok
									end
							end
					end;
				false ->
					ok
			end;
		NotOk ->
			NotOk
	end.

write_tx_header(TX) ->
	TX2 =
		case TX#tx.format of
			1 ->
				TX;
			_ ->
				TX#tx{ data = <<>> }
		end,
	ar_kv:put(tx_db, TX#tx.id, ar_serialize:tx_to_binary(TX2)).

write_tx_data(ExpectedDataRoot, Data, TXID) ->
	Chunks = ar_tx:chunk_binary(?DATA_CHUNK_SIZE, Data),
	SizeTaggedChunks = ar_tx:chunks_to_size_tagged_chunks(Chunks),
	SizeTaggedChunkIDs = ar_tx:sized_chunks_to_sized_chunk_ids(SizeTaggedChunks),
	case {ExpectedDataRoot, ar_merkle:generate_tree(SizeTaggedChunkIDs)} of
		{no_expected_data_root, {DataRoot, DataTree}} ->
			write_tx_data(DataRoot, DataTree, Data, SizeTaggedChunks, TXID);
		{_, {ExpectedDataRoot, DataTree}} ->
			write_tx_data(ExpectedDataRoot, DataTree, Data, SizeTaggedChunks, TXID);
		_ ->
			{error, [invalid_data_root]}
	end.

write_tx_data(DataRoot, DataTree, Data, SizeTaggedChunks, TXID) ->
	Errors = lists:foldl(
		fun
			({<<>>, _}, Acc) ->
				%% Empty chunks are produced by ar_tx:chunk_binary/2, when
				%% the data is evenly split by the given chunk size. They are
				%% the last chunks of the corresponding transactions and have
				%% the same end offsets as their preceding chunks. They are never
				%% picked as recall chunks because recall byte has to be strictly
				%% smaller than the end offset. They are an artifact of the original
				%% chunking implementation. There is no value in storing them.
				Acc;
			({Chunk, Offset}, Acc) ->
				DataPath = ar_merkle:generate_path(DataRoot, Offset - 1, DataTree),
				TXSize = byte_size(Data),
				case ar_data_sync:add_chunk(DataRoot, DataPath, Chunk, Offset - 1, TXSize) of
					ok ->
						Acc;
					{error, Reason} ->
						?LOG_WARNING([{event, failed_to_write_tx_chunk},
								{tx, ar_util:encode(TXID)},
								{reason, io_lib:format("~p", [Reason])}]),
						[Reason | Acc]
				end
		end,
		[],
		SizeTaggedChunks
	),
	case Errors of
		[] ->
			ok;
		_ ->
			{error, Errors}
	end.

%% @doc Read a tx from disk, given a hash.
read_tx(unavailable) ->
	unavailable;
read_tx(TX) when is_record(TX, tx) ->
	TX;
read_tx(TXs) when is_list(TXs) ->
	lists:map(fun read_tx/1, TXs);
read_tx(ID) ->
	case read_tx_from_disk_cache(ID) of
		unavailable ->
			read_tx2(ID);
		TX ->
			TX
	end.

read_tx2(ID) ->
	case ar_kv:get(tx_db, ID) of
		not_found ->
			read_tx_from_file(ID);
		{ok, Binary} ->
			TX = parse_tx_kv_binary(Binary),
			case TX#tx.format == 1 andalso TX#tx.data_size > 0
					andalso byte_size(TX#tx.data) == 0 of
				true ->
					case read_tx_data_from_kv_storage(TX#tx.id) of
						{ok, Data} ->
							TX#tx{ data = Data };
						Error ->
							?LOG_WARNING([{event, error_reading_tx_from_kv_storage},
									{tx, ar_util:encode(ID)},
									{error, io_lib:format("~p", [Error])}]),
							unavailable
					end;
				_ ->
					TX
			end
	end.

read_tx_from_disk_cache(ID) ->
	case ar_disk_cache:lookup_tx_filename(ID) of
		unavailable ->
			unavailable;
		{ok, Filename} ->
			case read_tx_file(Filename) of
				{ok, TX} ->
					TX;
				_Error ->
					unavailable
			end
	end.

read_tx_from_file(ID) ->
	case lookup_tx_filename(ID) of
		{ok, Filename} ->
			case read_tx_file(Filename) of
				{ok, TX} ->
					TX;
				_Error ->
					unavailable
			end;
		{migrated_v1, Filename} ->
			case read_migrated_v1_tx_file(Filename) of
				{ok, TX} ->
					TX;
				_Error ->
					unavailable
			end;
		unavailable ->
			unavailable
	end.

read_tx_file(Filename) ->
	case read_file_raw(Filename) of
		{ok, <<>>} ->
			file:delete(Filename),
			?LOG_WARNING([{event, empty_tx_file},
					{filename, Filename}]),
			{error, tx_file_empty};
		{ok, Binary} ->
			case catch ar_serialize:json_struct_to_tx(Binary) of
				TX when is_record(TX, tx) ->
					{ok, TX};
				_ ->
					file:delete(Filename),
					?LOG_WARNING([{event, failed_to_parse_tx},
							{filename, Filename}]),
					{error, failed_to_parse_tx}
			end;
		Error ->
			Error
	end.

read_file_raw(Filename) ->
	case file:open(Filename, [read, raw, binary]) of
		{ok, File} ->
			case file:read(File, 20000000) of
				{ok, Bin} ->
					file:close(File),
					{ok, Bin};
				Error ->
					Error
			end;
		Error ->
			Error
	end.

read_migrated_v1_tx_file(Filename) ->
	case read_file_raw(Filename) of
		{ok, Binary} ->
			case catch ar_serialize:json_struct_to_v1_tx(Binary) of
				#tx{ id = ID } = TX ->
					case read_tx_data_from_kv_storage(ID) of
						{ok, Data} ->
							{ok, TX#tx{ data = Data }};
						Error ->
							Error
					end
			end;
		Error ->
			Error
	end.

read_tx_data_from_kv_storage(ID) ->
	case ar_data_sync:get_tx_data(ID) of
		{ok, Data} ->
			{ok, Data};
		{error, not_found} ->
			{error, data_unavailable};
		{error, timeout} ->
			{error, data_fetch_timeout};
		Error ->
			Error
	end.

read_tx_data(TX) ->
	case read_file_raw(tx_data_filepath(TX)) of
		{ok, Data} ->
			{ok, ar_util:decode(Data)};
		Error ->
			Error
	end.

write_wallet_list(Height, Tree) ->
	{RootHash, _UpdatedTree, UpdateMap} = ar_block:hash_wallet_list(Tree),
	store_account_tree_update(Height, RootHash, UpdateMap),
	RootHash.

%% @doc Read a given wallet list (by hash) from the disk.
read_wallet_list(<<>>) ->
	{ok, ar_patricia_tree:new()};
read_wallet_list(WalletListHash) when is_binary(WalletListHash) ->
	Key = WalletListHash,
	read_wallet_list(get_account_tree_value(Key, <<>>), ar_patricia_tree:new(), [],
			WalletListHash, WalletListHash).

read_wallet_list({ok, << K:48/binary, _/binary >>, Bin}, Tree, Keys, RootHash, K) ->
	case binary_to_term(Bin) of
		{Key, Value} ->
			Tree2 = ar_patricia_tree:insert(Key, Value, Tree),
			case Keys of
				[] ->
					{ok, Tree2};
				[{H, Prefix} | Keys2] ->
					read_wallet_list(get_account_tree_value(H, Prefix), Tree2, Keys2,
							RootHash, H)
			end;
		[{H, Prefix} | Hs] ->
			read_wallet_list(get_account_tree_value(H, Prefix), Tree, Hs ++ Keys, RootHash,
					H)
	end;
read_wallet_list({ok, _, _}, _Tree, _Keys, RootHash, _K) ->
	read_wallet_list_from_chunk_files(RootHash);
read_wallet_list(none, _Tree, _Keys, RootHash, _K) ->
	read_wallet_list_from_chunk_files(RootHash);
read_wallet_list(Error, _Tree, _Keys, _RootHash, _K) ->
	Error.

read_wallet_list_from_chunk_files(WalletListHash) when is_binary(WalletListHash) ->
	case read_wallet_list_chunk(WalletListHash) of
		not_found ->
			Filename = wallet_list_filepath(WalletListHash),
			case file:read_file(Filename) of
				{ok, JSON} ->
					parse_wallet_list_json(JSON);
				{error, enoent} ->
					not_found;
				Error ->
					Error
			end;
		{ok, Tree} ->
			{ok, Tree};
		{error, _Reason} = Error ->
			Error
	end;
read_wallet_list_from_chunk_files(WL) when is_list(WL) ->
	{ok, ar_patricia_tree:from_proplist([{get_wallet_key(T), get_wallet_value(T)}
			|| T <- WL])}.

get_wallet_key(T) ->
	element(1, T).

get_wallet_value({_, Balance, LastTX}) ->
	{Balance, LastTX};
get_wallet_value({_, Balance, LastTX, Denomination, MiningPermission}) ->
	{Balance, LastTX, Denomination, MiningPermission}.

read_wallet_list_chunk(RootHash) ->
	read_wallet_list_chunk(RootHash, 0, ar_patricia_tree:new()).

read_wallet_list_chunk(RootHash, Position, Tree) ->
	{ok, Config} = application:get_env(chivesweave, config),
	Filename =
		binary_to_list(iolist_to_binary([
			Config#config.data_dir,
			"/",
			?WALLET_LIST_DIR,
			"/",
			ar_util:encode(RootHash),
			"-",
			integer_to_binary(Position),
			"-",
			integer_to_binary(?WALLET_LIST_CHUNK_SIZE)
		])),
	case read_term(".", Filename) of
		{ok, Chunk} ->
			{NextPosition, Wallets} =
				case Chunk of
					[last | Tail] ->
						{last, Tail};
					_ ->
						{Position + ?WALLET_LIST_CHUNK_SIZE, Chunk}
				end,
			Tree2 =
				lists:foldl(
					fun({K, V}, Acc) -> ar_patricia_tree:insert(K, V, Acc) end,
					Tree,
					Wallets
				),
			case NextPosition of
				last ->
					{ok, Tree2};
				_ ->
					read_wallet_list_chunk(RootHash, NextPosition, Tree2)
			end;
		{error, Reason} = Error ->
			?LOG_ERROR([
				{event, failed_to_read_wallet_list_chunk},
				{reason, Reason}
			]),
			Error;
		not_found ->
			not_found
	end.

parse_wallet_list_json(JSON) ->
	case ar_serialize:json_decode(JSON) of
		{ok, JiffyStruct} ->
			{ok, ar_serialize:json_struct_to_wallet_list(JiffyStruct)};
		{error, Reason} ->
			{error, {invalid_json, Reason}}
	end.

lookup_tx_filename(ID) ->
	Filepath = tx_filepath(ID),
	case is_file(Filepath) of
		true ->
			{ok, Filepath};
		false ->
			MigratedV1Path = filepath([?TX_DIR, "migrated_v1", tx_filename(ID)]),
			case is_file(MigratedV1Path) of
				true ->
					{migrated_v1, MigratedV1Path};
				false ->
					unavailable
			end
	end.

%% @doc A quick way to lookup the file without using the Erlang file server.
%% Helps take off some IO load during the busy times.
is_file(Filepath) ->
	case file:read_file_info(Filepath, [raw]) of
		{ok, #file_info{ type = Type }} when Type == regular orelse Type == symlink ->
			true;
		_ ->
			false
	end.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	{ok, Config} = application:get_env(chivesweave, config),
	ensure_directories(Config#config.data_dir),
	%% Copy genesis transactions (snapshotted in the repo) into data_dir/txs
	ar_weave:add_mainnet_v1_genesis_txs(),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "block_time_history_db"), block_time_history_db),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "block_index_db"), block_index_db),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "xwe_storage_tx_confirmation_db"), tx_confirmation_db),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "xwe_storage_tx_db"), tx_db),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "xwe_storage_address_tx_deposits_db"), address_tx_deposits_db),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "xwe_storage_address_tx_send_db"), address_tx_send_db),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "xwe_storage_address_tx_db"), address_tx_db),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "xwe_storage_address_data_db"), address_data_db),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "xwe_storage_txid_block_db"), xwe_storage_txid_block_db),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "xwe_storage_txid_in_bundle"), xwe_storage_txid_in_bundle),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "xwe_storage_parse_bundle_txid_list"), xwe_storage_parse_bundle_txid_list),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "xwe_storage_parse_bundle_address_status"), xwe_storage_parse_bundle_address_status),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "xwe_storage_block_db"), block_db),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "reward_history_db"), reward_history_db),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "account_tree_db"), account_tree_db),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "explorer_block"), explorer_block),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "explorer_tx"), explorer_tx),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "explorer_address_richlist"), explorer_address_richlist),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "explorer_token"), explorer_token),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "explorer_contract"), explorer_contract),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "statistics_transaction"), statistics_transaction),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "statistics_network"), statistics_network),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "statistics_data"), statistics_data),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "statistics_block"), statistics_block),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "statistics_address"), statistics_address),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "statistics_contract"), statistics_contract),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "statistics_token"), statistics_token),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "statistics_summary"), statistics_summary),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "statistics_ipaddress"), statistics_ipaddress),

	ets:insert(?MODULE, [{same_disk_storage_modules_total_size,
			get_same_disk_storage_modules_total_size()}]),
	{ok, #state{}}.

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_cast({store_account_tree_update, Height, RootHash, Map}, State) ->
	store_account_tree_update(Height, RootHash, Map),
	{noreply, State};

handle_cast(Cast, State) ->
	?LOG_WARNING("event: unhandled_cast, cast: ~p", [Cast]),
	{noreply, State}.

handle_info(Message, State) ->
	?LOG_WARNING("event: unhandled_info, message: ~p", [Message]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================


write_block(B) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(disk_logging, Config#config.enable) of
		true ->
			?LOG_INFO([{event, writing_block_to_disk},
					{block, ar_util:encode(B#block.indep_hash)}]);
		_ ->
			do_nothing
	end,
	TXIDs = lists:map(fun(TXID) when is_binary(TXID) -> TXID;
			(#tx{ id = TXID }) -> TXID end, B#block.txs),
	case ar_kv:put(block_db, B#block.indep_hash, ar_serialize:block_to_binary(B#block{
			txs = TXIDs })) of
		ok ->
			update_reward_history(B);
		Error ->
			Error
	end,

	%%% Make block data for explorer
	BlockBin = term_to_binary([B#block.height, ar_util:encode(B#block.indep_hash), ar_util:encode(B#block.reward_addr), ar_pricing:reset_block_reward_by_height(B#block.height, B#block.reward), B#block.timestamp, length(B#block.txs), B#block.weave_size, B#block.block_size]),
	ar_kv:put(explorer_block, list_to_binary(integer_to_list(B#block.height)), BlockBin),

	% ?LOG_INFO([{write_block______________________________________________________________________________explorer_block, list_to_binary(integer_to_list(B#block.height)) }]),
	
	TotalTxReward = 0,
	lists:foreach(
        fun(TX) ->
			FromAddress = ar_util:encode(ar_wallet:to_address(TX#tx.owner, TX#tx.signature_type)),
            TargetAddress = ar_util:encode(TX#tx.target),
            TxId = ar_util:encode(TX#tx.id),
            Reward = TX#tx.reward,
            Quantity = TX#tx.quantity,			
			ar_kv:put(xwe_storage_txid_block_db, TxId, term_to_binary([B#block.height,ar_util:encode(B#block.indep_hash),B#block.timestamp])),
			case byte_size(TargetAddress) == 0 of
				true ->
					%%% address_data_db
					case ar_kv:get(address_data_db, FromAddress) of
						not_found ->
							TxIdArrayFrom = [TxId],
							TxIdDataFrom = term_to_binary(TxIdArrayFrom);
						{ok, TxIdBinaryFrom} ->
							TxIdArrayFrom = binary_to_term(TxIdBinaryFrom),
							TxIdDataFrom = term_to_binary([TxId | TxIdArrayFrom])					
					end,			
					ar_kv:put(address_data_db, FromAddress, TxIdDataFrom),
					%%% address_tx_db
					case ar_kv:get(address_tx_db, FromAddress) of
						not_found ->
							TxIdArrayFrom2 = [TxId],
							TxIdDataFrom2 = term_to_binary(TxIdArrayFrom2);
						{ok, TxIdBinaryFrom2} ->
							TxIdArrayFrom2 = binary_to_term(TxIdBinaryFrom2),
							TxIdDataFrom2 = term_to_binary([TxId | TxIdArrayFrom2])					
					end,			
					ar_kv:put(address_tx_db, FromAddress, TxIdDataFrom2),
					%%% explorer_address_richlist
					case ar_kv:get(explorer_address_richlist, FromAddress) of
						not_found ->
							AddressRichListElement = [0-Reward,1,Reward,0],
							AddressRichListElementBin = term_to_binary(AddressRichListElement),
							ar_kv:put(explorer_address_richlist, FromAddress, AddressRichListElementBin);
						{ok, AddressRichListElementResult} ->
							[Balance, TxNumber, SendAmount, ReceiveAmount] = binary_to_term(AddressRichListElementResult),
							AddressRichListElementNew = [Balance-Reward, TxNumber+1, SendAmount+Reward, ReceiveAmount],
							AddressRichListElementNewBin = term_to_binary(AddressRichListElementNew),	
							ar_kv:put(explorer_address_richlist, FromAddress, AddressRichListElementNewBin)				
					end;
				false ->
					case TX#tx.data_size > 0 of
						true ->
							% ?LOG_INFO([{txId_data_size_more_than_0__________________________________________________, TX#tx.data_size}]),
							%%% address_data_db
							case ar_kv:get(address_data_db, TargetAddress) of
								not_found ->
									TxIdArrayFrom3 = [TxId],
									TxIdDataFrom3 = term_to_binary(TxIdArrayFrom3);
								{ok, TxIdBinaryFrom3} ->
									TxIdArrayFrom3 = binary_to_term(TxIdBinaryFrom3),
									TxIdDataFrom3 = term_to_binary([TxId | TxIdArrayFrom3])					
							end,
							ar_kv:put(address_data_db, TargetAddress, TxIdDataFrom3);
						false ->
							% ?LOG_INFO([{txId_data_size_more_than_0__________________________________________________, TX#tx.data_size}]),
							ok
					end,
					%%% address_tx_db
					case ar_kv:get(address_tx_db, FromAddress) of
						not_found ->
							TxIdArray = [TxId],
							TxIdData = term_to_binary(TxIdArray);
						{ok, TxIdBinary} ->
							TxIdArray = binary_to_term(TxIdBinary),
							TxIdData = term_to_binary([TxId | TxIdArray])					
					end,			
					ar_kv:put(address_tx_db, FromAddress, TxIdData),
					
					%%% address_tx_db
					case ar_kv:get(address_tx_db, TargetAddress) of
						not_found ->
							TxIdArray2 = [TxId],
							TxIdData2 = term_to_binary(TxIdArray2);
						{ok, TxIdBinary2} ->
							TxIdArray2 = binary_to_term(TxIdBinary2),
							TxIdData2 = term_to_binary([TxId | TxIdArray2])					
					end,			
					ar_kv:put(address_tx_db, TargetAddress, TxIdData2),
					
					%%% address_tx_deposits_db
					case ar_kv:get(address_tx_deposits_db, TargetAddress) of
						not_found ->
							TxIdArray3 = [TxId],
							TxIdData3 = term_to_binary(TxIdArray3);
						{ok, TxIdBinary3} ->
							TxIdArray3 = binary_to_term(TxIdBinary3),
							TxIdData3 = term_to_binary([TxId | TxIdArray3])					
					end,			
					ar_kv:put(address_tx_deposits_db, TargetAddress, TxIdData3),
					
					%%% address_tx_send_db
					case ar_kv:get(address_tx_send_db, FromAddress) of
						not_found ->
							TxIdArray4 = [TxId],
							TxIdData4 = term_to_binary(TxIdArray4);
						{ok, TxIdBinary4} ->
							TxIdArray4 = binary_to_term(TxIdBinary4),
							TxIdData4 = term_to_binary([TxId | TxIdArray4])					
					end,			
					ar_kv:put(address_tx_send_db, FromAddress, TxIdData4),
					
					%%% explorer_address_richlist Send
					case ar_kv:get(explorer_address_richlist, FromAddress) of
						not_found ->
							AddressRichListElement1 = [0-Reward-Quantity,1,Reward+Quantity,0],
							AddressRichListElementBin1 = term_to_binary(AddressRichListElement1),
							ar_kv:put(explorer_address_richlist, FromAddress, AddressRichListElementBin1);
						{ok, AddressRichListElementResult1} ->
							[Balance1, TxNumber1, SendAmount1, ReceiveAmount1] = binary_to_term(AddressRichListElementResult1),
							AddressRichListElementNew1 = [Balance1-Reward-Quantity, TxNumber1+1, SendAmount1+Reward+Quantity, ReceiveAmount1],
							AddressRichListElementNewBin = term_to_binary(AddressRichListElementNew1),
							ar_kv:put(explorer_address_richlist, FromAddress, AddressRichListElementNewBin)				
					end,
					
					%%% explorer_address_richlist Receive
					case ar_kv:get(explorer_address_richlist, TargetAddress) of
						not_found ->
							AddressRichListElement2 = [Quantity,1,0,Quantity],
							AddressRichListElementBin2 = term_to_binary(AddressRichListElement2),
							ar_kv:put(explorer_address_richlist, TargetAddress, AddressRichListElementBin2);
						{ok, AddressRichListElementResult2} ->
							[Balance2, TxNumber2, SendAmount2, ReceiveAmount2] = binary_to_term(AddressRichListElementResult2),
							AddressRichListElementNew2 = [Balance2+Quantity, TxNumber2+1, SendAmount2, ReceiveAmount2+Quantity],
							AddressRichListElementNewBin2 = term_to_binary(AddressRichListElementNew2),	
							ar_kv:put(explorer_address_richlist, TargetAddress, AddressRichListElementNewBin2)				
					end
			end,
			%%% Make block data for explorer
			TxBin = term_to_binary({TxId,FromAddress,TargetAddress,TX#tx.data_size,Reward,B#block.height,B#block.timestamp,TX#tx.tags}),
			ar_kv:put(explorer_tx, TxId, TxBin),
			%%% statistics_transaction
			DateString = ar_util:encode(take_first_n_chars(calendar:system_time_to_rfc3339(B#block.timestamp), 10)),
			case ar_kv:get(statistics_transaction, DateString) of
				not_found ->
					StatisticsTxElement = [1,1,1,Reward,Reward,Reward,Reward,Quantity,Quantity,0,0,0,0],
					StatisticsTxElementBin = term_to_binary(StatisticsTxElement),
					ar_kv:put(statistics_transaction, DateString, StatisticsTxElementBin);
				{ok, StatisticsTxElementBinResult} ->
					[Transactions,Cumulative_Transactions,TPS,Transaction_Fees,Cumulative_Fees,Avg_Tx_Fee,Max_Tx_Fee,Trade_Volume,Cumulative_Trade_Volume,Native_Transfers,Native_Interactions,Native_Senders,Native_Receivers] = binary_to_term(StatisticsTxElementBinResult),
					case Max_Tx_Fee>Reward of 
						true ->
							Max_Tx_Fee_New = Max_Tx_Fee;
						false ->
							Max_Tx_Fee_New = Reward
					end,
					StatisticsTxElementBin2 = term_to_binary([Transactions+1,Cumulative_Transactions+1,round(Transactions/86400),Reward,Cumulative_Fees+Reward,round((Cumulative_Fees+Reward)/(Transactions+1)),Max_Tx_Fee_New,Quantity,Cumulative_Trade_Volume+Quantity,Native_Transfers,Native_Interactions,Native_Senders,Native_Receivers]),
					ar_kv:put(statistics_transaction, DateString, StatisticsTxElementBin2)				
			end,
			%% parse bundle tx data
			Tags = lists:map(
				fun({Name, Value}) ->
					{Name, Value}
				end,
				TX#tx.tags),
			case find_value_in_tags(<<"Bundle-Version">>, Tags) of
				<<"2.0.0">> ->
					% Is Bundle
					?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle_____Tags, Tags}]),
					case ar_storage:read_tx_data(TX) of
						{ok, TxData} ->
							<<GetDataItemCountBinary:32/binary, DataItemsBinary/binary>> = TxData,
							GetDataItemCount = binary:decode_unsigned(GetDataItemCountBinary,little),
							?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle_____read_tx_data___TX, TxData}]),
							?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle_____GetDataItemCountBinary, GetDataItemCountBinary}]),
							?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle_____DataItemsBinary, DataItemsBinary}]),
							?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle_____GetDataItemCountBinary, binary_to_integer(GetDataItemCount)}]),
							?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle_____GetDataItemCount, GetDataItemCount}]),
							[];
						{error, enoent} ->
							case ar_data_sync:get_tx_data(TX#tx.id) of
								{ok, TxData} ->
									?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle__parse_bundle_data, ar_util:encode(TX#tx.id)}]),
									parse_bundle_data(TxData, TX, 0, 100, false);
								_ ->
									?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle__get_tx_data___Failed, ar_util:encode(TX#tx.id)}]),
									[]
							end
					end;
				_ ->
					% Not a Bundle
					[]
			end
        end,
        B#block.txs
    ),

	%%% statistics_summary
	TodayDate = ar_util:encode(take_first_n_chars(calendar:system_time_to_rfc3339(B#block.timestamp), 10)),
	case ar_kv:get(statistics_summary, list_to_binary("datelist")) of
		not_found ->
			StatisticsSummary = [TodayDate],
			StatisticsSummaryBin = term_to_binary(StatisticsSummary),
			ar_kv:put(statistics_summary, list_to_binary("datelist"), StatisticsSummaryBin);
		{ok, StatisticsSummaryResult} ->
			DateListArray = binary_to_term(StatisticsSummaryResult),
			case lists:member(TodayDate, DateListArray) of
				true ->
					ok;
				false ->
					StatisticsSummaryBin = term_to_binary([ TodayDate | DateListArray]),
					ar_kv:put(statistics_summary, list_to_binary("datelist"), StatisticsSummaryBin)
			end			
	end,

	%%% statistics_network
	TodayDate = ar_util:encode(take_first_n_chars(calendar:system_time_to_rfc3339(B#block.timestamp), 10)),
	case ar_kv:get(statistics_network, TodayDate) of
		not_found ->
			StatisticsNetwork = [0,0,0,0,0,0,0,0,0,0],
			StatisticsNetworkBin = term_to_binary(StatisticsNetwork);
		{ok, StatisticsNetworkResult} ->
			[Weave_Size,Weave_Size_Growth,Cumulative_Endowment,Avg_Endowment_Growth,Endowment_Growth,Avg_Pending_Txs,Avg_Pending_Size,Node_Count,Cumulative_Difficulty,Difficulty] = binary_to_term(StatisticsNetworkResult),
			StatisticsNetworkBin = term_to_binary([Weave_Size+B#block.weave_size,Weave_Size_Growth+B#block.weave_size,Cumulative_Endowment+B#block.reward_pool,Avg_Endowment_Growth+B#block.reward_pool,Endowment_Growth+B#block.reward_pool,Avg_Pending_Txs,Avg_Pending_Size,Node_Count,Cumulative_Difficulty+B#block.cumulative_diff,Difficulty+B#block.cumulative_diff])					
	end,
	ar_kv:put(statistics_network, TodayDate, StatisticsNetworkBin),

	%%% statistics_data
	TodayDataDate = ar_util:encode(take_first_n_chars(calendar:system_time_to_rfc3339(B#block.timestamp), 10)),
	case ar_kv:get(statistics_data, TodayDataDate) of
		not_found ->
			StatisticsData = [0,0,0,0,0,0,0,'',''],
			StatisticsDataBin = term_to_binary(StatisticsData);
		{ok, StatisticsDataResult} ->
			[Data_Uploaded,Storage_Cost,Data_Size,Data_Fees,Cumulative_Data_Fees,Fees_Towards_Data_Upload,Data_Uploaders,Content_Type,Content_Type_Tx] = binary_to_term(StatisticsDataResult),
			StatisticsDataBin = term_to_binary([Data_Uploaded+B#block.weave_size,Storage_Cost,Data_Size+B#block.weave_size,Data_Fees+TotalTxReward,Cumulative_Data_Fees+TotalTxReward,Fees_Towards_Data_Upload,Data_Uploaders+1,Content_Type,Content_Type_Tx])					
	end,
	ar_kv:put(statistics_data, TodayDataDate, StatisticsDataBin),

	%%% statistics_block
	case ar_kv:get(statistics_block, TodayDataDate) of
		not_found ->
			StatisticsBlock = [0,0,0,0,0,0,0,0,0,0,0,0],
			StatisticsBlockBin = term_to_binary(StatisticsBlock),
			ar_kv:put(statistics_block, TodayDataDate, StatisticsBlockBin);
		{ok, StatisticsBlockResult} ->
			BlockRewardFilter = ar_pricing:reset_block_reward_by_height(B#block.height, B#block.reward),
			[Blocks,Avg_Txs_By_Block,Cumulative_Block_Rewards,Block_Rewards,Rewards_vs_Endowment,Avg_Block_Rewards,Max_Block_Rewards,Min_Block_Rewards,Avg_Block_Time,Max_Block_Time,Min_Block_Time,BlockUsedTime] = binary_to_term(StatisticsBlockResult),
			case BlockRewardFilter>Max_Block_Rewards of
				true ->
					Max_Block_Rewards_New = BlockRewardFilter;
				false ->
					Max_Block_Rewards_New = Max_Block_Rewards
			end,
			case BlockRewardFilter<Min_Block_Rewards of
				true ->
					Min_Block_Rewards_New = BlockRewardFilter;
				false ->
					Min_Block_Rewards_New = Min_Block_Rewards
			end,
			case B#block.height>1 of
				true ->
					StatisticsBlockBin = term_to_binary([Blocks+1,Avg_Txs_By_Block,Cumulative_Block_Rewards+BlockRewardFilter,Block_Rewards+BlockRewardFilter,Rewards_vs_Endowment,Avg_Block_Rewards,Max_Block_Rewards_New,Min_Block_Rewards_New,Avg_Block_Time,Max_Block_Time,Min_Block_Time,BlockUsedTime]),
					ar_kv:put(statistics_block, TodayDataDate, StatisticsBlockBin);
				false ->
					ok
			end				
	end.

take_first_n_chars(Str, N) when is_list(Str), is_integer(N), N >= 0 ->
    lists:sublist(Str, 1, min(length(Str), N)).

generate_range(A, B) when A >= B ->
    [];
generate_range(A, B) ->
    [A | generate_range(A + 1, B)].

read_block_from_height_by_number(FromHeight, BlockNumber, PageId) ->
	BlockHeightArray = generate_range(FromHeight, FromHeight + BlockNumber),
	BlockHeightArrayReverse = lists:reverse(BlockHeightArray),
	?LOG_INFO([{read_block_from_height_by_number________BlockHeightArray, BlockHeightArray}]),
	BlockListElement = lists:map(
		fun(X) -> 
			case X > 0 of 
				true ->
					% ?LOG_INFO([{read_block_from_height_by_number________BlockIdBinaryPrevious, X-1}]),
					case ar_kv:get(explorer_block, list_to_binary(integer_to_list(X-1))) of 
						not_found -> [not_found_1]; 
						{ok, BlockIdBinaryPrevious} -> 
							% ?LOG_INFO([{read_block_from_height_by_number________BlockIdBinaryPrevious, X-1}]),
							% ?LOG_INFO([{read_block_from_height_by_number________BlockIdBinaryPrevious, integer_to_list(X-1)}]),
							% ?LOG_INFO([{read_block_from_height_by_number________BlockIdBinaryPrevious, list_to_binary(integer_to_list(X-1))}]),
							% ?LOG_INFO([{read_block_from_height_by_number________BlockIdBinaryPrevious, BlockIdBinaryPrevious}]),
							BlockIdBinaryResultPrevious = binary_to_term(BlockIdBinaryPrevious),
							TimestampPrevious = lists:nth(5, BlockIdBinaryResultPrevious),
							case ar_kv:get(explorer_block, list_to_binary(integer_to_list(X))) of 
								not_found -> [not_found_2]; 
								{ok, BlockIdBinary} -> 
									% ?LOG_INFO([{read_block_from_height_by_number________BlockIdBinary, BlockIdBinary}]),
									BlockIdBinaryResult = binary_to_term(BlockIdBinary),
									BlockMap = #{
											<<"id">> => lists:nth(1, BlockIdBinaryResult),
											<<"height">> => lists:nth(1, BlockIdBinaryResult),
											<<"indep_hash">> => list_to_binary(binary_to_list(lists:nth(2, BlockIdBinaryResult))),
											<<"reward_addr">> => list_to_binary(binary_to_list(lists:nth(3, BlockIdBinaryResult))),
											<<"reward">> => lists:nth(4, BlockIdBinaryResult),
											<<"timestamp">> => lists:nth(5, BlockIdBinaryResult),
											<<"txs_length">> => lists:nth(6, BlockIdBinaryResult),
											<<"weave_size">> => lists:nth(7, BlockIdBinaryResult),
											<<"block_size">> => lists:nth(8, BlockIdBinaryResult),
											<<"mining_time">> => lists:nth(5, BlockIdBinaryResult) - TimestampPrevious
										},
									BlockMap
							end
					end;
				false ->
					[]
			end
		end, BlockHeightArrayReverse),
	% ?LOG_INFO([{read_block_from_height_by_number________BlockListElement, BlockListElement}]),
	BlockListElementMap = lists:filter(fun(Element) -> is_map(Element) end, BlockListElement),
	CurrentBlockHeight = ar_node:get_height(),
	BlockListElementResult = #{
									<<"data">> => BlockListElementMap,
									<<"total">> => CurrentBlockHeight,
									<<"from">> => FromHeight,
									<<"pageid">> => PageId div 1,
									<<"pagesize">> => BlockNumber,
									<<"allpages">> => ceil(CurrentBlockHeight / BlockNumber) div 1
								},
	BlockListElementResult.
		

read_statistics_network() ->
	case ar_kv:get(statistics_summary, list_to_binary("datelist")) of
		not_found ->
			{404, #{}, []};
		{ok, StatisticsDateListResult} ->
			DateListArray = binary_to_term(StatisticsDateListResult),
			Statistics_network = lists:map(
				fun(X) -> 
					case ar_kv:get(statistics_network, X) of
						not_found ->
							[];
						{ok, DateListBinary} ->
							DateListResult = binary_to_term(DateListBinary),
							DateListMap = #{
								<<"Date">> => ar_util:decode(X),
								<<"Weave_Size">> => lists:nth(1, DateListResult),
								<<"Weave_Size_Growth">> => lists:nth(2, DateListResult),
								<<"Cumulative_Endowment">> => lists:nth(3, DateListResult),
								<<"Avg_Endowment_Growth">> => lists:nth(4, DateListResult),
								<<"Endowment_Growth">> => lists:nth(5, DateListResult),
								<<"Avg_Pending_Txs">> => lists:nth(6, DateListResult),
								<<"Avg_Pending_Size">> => lists:nth(7, DateListResult),
								<<"Node_Count">> => lists:nth(8, DateListResult),
								<<"Cumulative_Difficulty">> => lists:nth(9, DateListResult),
								<<"Difficulty">> => lists:nth(10, DateListResult)
							},
							DateListMap							
					end
				end, DateListArray),
			{200, #{}, ar_serialize:jsonify(Statistics_network)}
	end.

read_statistics_data() ->
	case ar_kv:get(statistics_summary, list_to_binary("datelist")) of
		not_found ->
			{404, #{}, []};
		{ok, StatisticsDateListResult} ->
			DateListArray = binary_to_term(StatisticsDateListResult),
			Statistics_data = lists:map(
				fun(X) -> 
					case ar_kv:get(statistics_data, X) of
						not_found ->
							[];
						{ok, DateListBinary} ->
							DateListResult = binary_to_term(DateListBinary),
							DateListMap = #{
								<<"Date">> => ar_util:decode(X),
								<<"Data_Uploaded">> => lists:nth(1, DateListResult),
								<<"Storage_Cost">> => lists:nth(2, DateListResult),
								<<"Data_Size">> => lists:nth(3, DateListResult),
								<<"Data_Fees">> => lists:nth(4, DateListResult),
								<<"Cumulative_Data_Fees">> => lists:nth(5, DateListResult),
								<<"Fees_Towards_Data_Upload">> => lists:nth(6, DateListResult),
								<<"Data_Uploaders">> => lists:nth(7, DateListResult),
								<<"Content_Type">> => lists:nth(8, DateListResult),
								<<"Content_Type_Tx">> => lists:nth(9, DateListResult)
							},
							DateListMap							
					end
				end, DateListArray),
			{200, #{}, ar_serialize:jsonify(Statistics_data)}
	end.

read_statistics_block() ->
	case ar_kv:get(statistics_summary, list_to_binary("datelist")) of
		not_found ->
			{200, #{}, []};
		{ok, StatisticsDateListResult} ->
			DateListArray = binary_to_term(StatisticsDateListResult),
			Statistics_block = lists:map(
				fun(X) -> 
					case ar_kv:get(statistics_block, X) of
						not_found ->
							[];
						{ok, DateListBinary} ->
							DateListResult = binary_to_term(DateListBinary),
							DateListMap = #{
								<<"Date">> => ar_util:decode(X),
								<<"Blocks">> => lists:nth(1, DateListResult),
								<<"Avg_Txs_By_Block">> => lists:nth(2, DateListResult),
								<<"Cumulative_Block_Rewards">> => lists:nth(3, DateListResult),
								<<"Block_Rewards">> => lists:nth(4, DateListResult),
								<<"Rewards_vs_Endowment">> => lists:nth(5, DateListResult),
								<<"Avg_Block_Rewards">> => lists:nth(6, DateListResult),
								<<"Max_Block_Rewards">> => lists:nth(7, DateListResult),
								<<"Min_Block_Rewards">> => lists:nth(8, DateListResult),
								<<"Avg_Block_Time">> => lists:nth(9, DateListResult),
								<<"Max_Block_Time">> => lists:nth(10, DateListResult),
								<<"Min_Block_Time">> => lists:nth(11, DateListResult),
								<<"BlockUsedTime">> => lists:nth(12, DateListResult)
							},
							DateListMap							
					end
				end, DateListArray),
			{200, #{}, ar_serialize:jsonify(Statistics_block)}
	end.

read_statistics_address() ->
	% TodayDate = take_first_n_chars(calendar:system_time_to_rfc3339(erlang:system_time(second)), 10),
	case ar_kv:get(statistics_summary, list_to_binary("datelist")) of
		not_found ->
			{200, #{}, []};
		{ok, StatisticsDateListResult} ->
			DateListArray = binary_to_term(StatisticsDateListResult),
			statistics_address = lists:map(
				fun(X) -> 
					case ar_kv:get(statistics_address, X) of
						not_found ->
							[];
						{ok, DateListBinary} ->
							DateListResult = binary_to_term(DateListBinary),
							DateListMap = #{
								<<"Date">> => ar_util:decode(X),
								<<"Blocks">> => lists:nth(1, DateListResult),
								<<"Avg_Txs_By_Block">> => lists:nth(2, DateListResult),
								<<"Cumulative_Block_Rewards">> => lists:nth(3, DateListResult),
								<<"Block_Rewards">> => lists:nth(4, DateListResult),
								<<"Rewards_vs_Endowment">> => lists:nth(5, DateListResult),
								<<"Avg_Block_Rewards">> => lists:nth(6, DateListResult),
								<<"Max_Block_Rewards">> => lists:nth(7, DateListResult),
								<<"Min_Block_Rewards">> => lists:nth(8, DateListResult),
								<<"Avg_Block_Time">> => lists:nth(9, DateListResult),
								<<"Max_Block_Time">> => lists:nth(10, DateListResult),
								<<"Min_Block_Time">> => lists:nth(11, DateListResult),
								<<"BlockUsedTime">> => lists:nth(12, DateListResult)
							},
							DateListMap							
					end
				end, DateListArray),
			{200, #{}, ar_serialize:jsonify(statistics_address)}
	end.

read_statistics_transaction() ->
	TodayDate = take_first_n_chars(calendar:system_time_to_rfc3339(erlang:system_time(second)), 10),
	case ar_kv:get(statistics_transaction, ar_util:encode(TodayDate)) of
		not_found ->
			{200, #{}, []};
		{ok, StatisticsTransactionResult} ->
			{200, #{}, ar_serialize:jsonify(binary_to_term(StatisticsTransactionResult))}							
	end.

find_value_in_tags(Key, List) ->
	case lists:keyfind(Key, 1, List) of
		{Key, Val} -> Val;
		false -> 
			case Key of
				<<"File-Parent">> -> <<"Root">>;
				<<"File-Public">> -> <<"Public">>;
				<<"Content-Type">> -> <<"text/plain">>;
				_ -> <<"">>
			end
	end.

contentTypeToFileType(ContentType) ->
	case ContentType of
		<<"image/png">> -> <<"image">>;
		<<"image/jpeg">> -> <<"image">>;
		<<"image/jpg">> -> <<"image">>;
		<<"image/gif">> -> <<"image">>;
		<<"text/plain">> -> <<"text">>;
		<<"application/x-msdownload">> -> <<"exe">>;
		<<"application/pdf">> -> <<"pdf">>;
		<<"application/msword">> -> <<"doc">>;
		<<"application/vnd.ms-word">> -> <<"doc">>;
		<<"application/vnd.openxmlformats-officedocument.wordprocessingml.document">> -> <<"docx">>;
		<<"application/vnd.ms-excel">> -> <<"xls">>;
		<<"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet">> -> <<"xlsx">>;
		<<"application/vnd.ms-powerpoint">> -> <<"ppt">>;
		<<"application/vnd.openxmlformats-officedocument.presentationml.presentation">> -> <<"pptx">>;
		<<"model/stl">> -> <<"stl">>;
		<<"application/stl">> -> <<"stl">>;
		<<"application/sla">> -> <<"stl">>;
		<<"video/mp4">> -> <<"video">>;
		<<"video/webm">> -> <<"video">>;
		<<"video/ogg">> -> <<"video">>;
		<<"video/mpeg">> -> <<"video">>;
		<<"video/quicktime">> -> <<"video">>;
		<<"video/x-msvideo">> -> <<"video">>;		
		<<"audio/mpeg">> -> <<"audio">>;
		<<"audio/wav">> -> <<"audio">>;
		<<"audio/midi">> -> <<"audio">>;
		<<"audio/ogg">> -> <<"audio">>;
		<<"audio/aac">> -> <<"audio">>;
		<<"audio/x-ms-wma">> -> <<"audio">>;
		_ -> <<"unknown">>
	end.

file_to_thumbnail_data(ContentType, TxId, FromAddress, Data) ->
	?LOG_INFO([{serve_format_2_html_data_thumbnail__________ar_storage_____ContentType, ContentType}]),
	FileType = contentTypeToFileType(ContentType),
	case FileType of
		<<"image">> ->
			%% Is image, and will to compress this image
			MayCompressedData = ar_storage:image_thumbnail_compress_to_storage(Data, FromAddress, TxId),
			{<<"image/png">>, MayCompressedData};
		<<"pdf">> ->
			%% Is pdf, and will to compress this image
			MayCompressedData = ar_storage:pdf_office_thumbnail_png_to_storage(Data, FromAddress, TxId),
			{<<"image/png">>, MayCompressedData};
		<<"doc">> ->
			%% Is docx, and will to compress this image
			ar_storage:office_format_convert_to_pdf_storage(Data, FromAddress, TxId),
			MayCompressedData = ar_storage:pdf_office_thumbnail_png_to_storage(Data, FromAddress, TxId),
			{<<"image/png">>, MayCompressedData};
		<<"xls">> ->
			%% Is xlsx, and will to compress this image
			ar_storage:office_format_convert_to_pdf_storage(Data, FromAddress, TxId),
			MayCompressedData = ar_storage:pdf_office_thumbnail_png_to_storage(Data, FromAddress, TxId),
			{<<"image/png">>, MayCompressedData};
		<<"ppt">> ->
			%% Is pptx, and will to compress this image
			ar_storage:office_format_convert_to_pdf_storage(Data, FromAddress, TxId),
			MayCompressedData = ar_storage:pdf_office_thumbnail_png_to_storage(Data, FromAddress, TxId),
			{<<"image/png">>, MayCompressedData};
		<<"docx">> ->
			%% Is docx, and will to compress this image
			MayCompressedData = ar_storage:pdf_office_thumbnail_png_to_storage(Data, FromAddress, TxId),
			{<<"image/png">>, MayCompressedData};
		<<"xlsx">> ->
			%% Is xlsx, and will to compress this image
			MayCompressedData = ar_storage:pdf_office_thumbnail_png_to_storage(Data, FromAddress, TxId),
			{<<"image/png">>, MayCompressedData};
		<<"pptx">> ->
			%% Is pptx, and will to compress this image
			ar_storage:office_format_convert_to_pdf_storage(Data, FromAddress, TxId),
			MayCompressedData = ar_storage:pdf_office_thumbnail_png_to_storage(Data, FromAddress, TxId),
			{<<"image/png">>, MayCompressedData};
		<<"video">> ->
			%% Is video, and will to compress this image
			MayCompressedData = ar_storage:video_thumbnail_png_to_storage(Data, FromAddress, TxId),
			{<<"image/png">>, MayCompressedData};
		_ ->
			%% Not a image, just return the original data
			% ?LOG_INFO([{serve_format_2_html_data_thumbnail___________binary_starts_with_failed, false}]),
			{ContentType, Data}
	end.

file_to_pdf_data(ContentType, TxId, FromAddress, Data) ->
	?LOG_INFO([{serve_format_2_html_data_thumbnail__________ar_storage_____ContentType, ContentType}]),
	FileType = contentTypeToFileType(ContentType),
	case FileType of
		<<"doc">> ->
			%% Is docx, and will to compress this image
			MayCompressedData = ar_storage:office_format_convert_to_pdf_storage(Data, FromAddress, TxId),
			{<<"image/png">>, MayCompressedData};
		<<"xls">> ->
			%% Is xlsx, and will to compress this image
			MayCompressedData = ar_storage:office_format_convert_to_pdf_storage(Data, FromAddress, TxId),
			{<<"image/png">>, MayCompressedData};
		<<"ppt">> ->
			%% Is pptx, and will to compress this image
			MayCompressedData = ar_storage:office_format_convert_to_pdf_storage(Data, FromAddress, TxId),
			{<<"image/png">>, MayCompressedData};
		<<"pptx">> ->
			%% Is pptx, and will to compress this image
			MayCompressedData = ar_storage:office_format_convert_to_pdf_storage(Data, FromAddress, TxId),
			{<<"image/png">>, MayCompressedData};
		_ ->
			%% Not a image, just return the original data
			% ?LOG_INFO([{serve_format_2_html_data_thumbnail___________binary_starts_with_failed, false}]),
			{ContentType, Data}
	end.

read_txs_by_addr(Addr, PageId, PageRecords) ->
	try binary_to_integer(PageRecords) of
		PageRecordsInt ->				
			PageRecordsNew = if
				PageRecordsInt < 0 -> 1;
				PageRecordsInt > 100 -> 100;
				true -> PageRecordsInt
			end,
			try binary_to_integer(PageId) of
				PageIdInt ->
					PageIdNew = if
						PageIdInt < 0 -> 0;
						true -> PageIdInt
					end,
					case ar_kv:get(address_tx_db, Addr) of
						not_found ->
							[];
						{ok, TxIdBinary} ->
							AllArray = binary_to_term(TxIdBinary),
							Length = length(AllArray),
							FromIndex = PageIdNew * PageRecordsNew,
							FromIndexNew = if
								FromIndex < 1 -> 1;
								FromIndex > Length -> Length;
								true -> FromIndex
							end,
							lists:sublist(AllArray, FromIndexNew, PageRecordsNew)
					end
			catch _:_ ->
				[]
			end
	catch _:_ ->
		[]
	end.

read_txs_and_into_parse_bundle_list(Addr) ->
	case ar_kv:get(xwe_storage_parse_bundle_address_status, Addr) of
		not_found ->
			case ar_kv:get(address_tx_db, Addr) of
				not_found ->
					[];
				{ok, TxIdBinary} ->
					ar_kv:put(xwe_storage_parse_bundle_address_status, Addr, term_to_binary([<<"1">>])),
					AllArray = binary_to_term(TxIdBinary),
					case ar_kv:get(statistics_summary, list_to_binary("parsebundletxlist")) of
						not_found ->
							ParseBundleTxListBinEmpty = term_to_binary(AllArray),
							ar_kv:put(statistics_summary, list_to_binary("parsebundletxlist"), ParseBundleTxListBinEmpty),
							AllArray;
						{ok, ParseBundleTxListResult} ->
							ParseBundleTxlLstArray = binary_to_term(ParseBundleTxListResult),
							MergedList = lists:usort(lists:merge(ParseBundleTxlLstArray, AllArray)),
							ParseBundleTxListNewBin = term_to_binary(MergedList),
							ar_kv:put(statistics_summary, list_to_binary("parsebundletxlist"), ParseBundleTxListNewBin),
							MergedList							
					end
			end;
		_ ->
			ok
	end.

parse_bundle_tx_from_list(AllArray) ->
	?LOG_INFO([{handle_get_tx_unbundle________AllArray, AllArray}]),
	ParseResultList = lists:map(
		fun(TxId) ->
			case ar_util:safe_decode(TxId) of
				{error, invalid} ->
					TxId;
				{ok, ID} ->
					case read_tx(ID) of
						unavailable ->
							TxId;
						#tx{} = TX ->
							Tags = lists:map(
								fun({Name, Value}) ->
									{Name, Value}
								end,
								TX#tx.tags),
							% ?LOG_INFO([{handle_get_tx_unbundle_____________TX, ar_util:encode(TX#tx.id)}]),
							ParseResultItem = case find_value_in_tags(<<"Bundle-Version">>, Tags) of
								<<"2.0.0">> ->
									% Is Bundle
									% ?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle___________________Tags, Tags}]),
									case read_tx_data(TX) of
										{ok, TxData} ->
											% ?LOG_INFO([{handle_get_tx_unbundle_________________________IS_Bundle_____read_tx_data___TX, TxData}]),
											% <<GetDataItemCountBinary:32/binary, DataItemsBinary/binary>> = TxData,
											% GetDataItemCount = binary:decode_unsigned(GetDataItemCountBinary,little),
											% ?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle_____GetDataItemCountBinary, GetDataItemCountBinary}]),
											% ?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle_____DataItemsBinary, DataItemsBinary}]),
											% ?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle_____GetDataItemCountBinary, binary_to_integer(GetDataItemCount)}]),
											% ?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle_____GetDataItemCount, GetDataItemCount}]),
											<<"Read_Tx_Data_Success">>;
										{error, enoent} ->
											?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle__parse_errorerrorerrorerror, ar_util:encode(TX#tx.id)}]),
											case ar_data_sync:get_tx_data(TX#tx.id) of
												{ok, TxData2} ->
													% ?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle__parse_bundle_data, ar_util:encode(TX#tx.id)}]),
													ParseBundleData = parse_bundle_data(TxData2, TX, 0, 5, true),
													?LOG_INFO([{handle_get_tx_unbundle_________________________ParseBundleData, ParseBundleData}]),
													ar_util:encode(TX#tx.id);
												_ ->
													% ?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle__get_tx_data___Failed, ar_util:encode(TX#tx.id)}]),
													<<"Get_Bundle_Failed">>
											end;
										_ ->
											% ?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle__get_tx_data___Not_Match, ar_util:encode(TX#tx.id)}]),
											<<"Not_Match_Bundle">>

									end;
								_ ->
									% Not a Bundle
									% ?LOG_INFO([{handle_get_tx_unbundle________NOT_A_Bundle_____Tags, Tags}]),
									<<"NOT_A_Bundle">>
							end,
							% ?LOG_INFO([{handle_get_tx_unbundle________ParseResultItem, ParseResultItem}]),
							ParseResultItem
					end
			end														
		end,
		AllArray),
	% Finished Parse Bundle, Delete The TxId From Task
	case ar_kv:get(statistics_summary, list_to_binary("parsebundletxlist")) of
		{ok, ParseBundleTxListResult} ->
			ParseBundleTxlLstArray = binary_to_term(ParseBundleTxListResult),
			MergedList = lists:usort(lists:subtract(ParseBundleTxlLstArray, AllArray)),
			ParseBundleTxListNewBin = term_to_binary(MergedList),
			ar_kv:put(statistics_summary, list_to_binary("parsebundletxlist"), ParseBundleTxListNewBin),
			% ?LOG_INFO([{handle_get_tx_unbundle________ParseBundleTxListResult, ParseBundleTxListResult}]),
			% ?LOG_INFO([{handle_get_tx_unbundle________ParseBundleTxListNewBin, ParseBundleTxListNewBin}]),
			MergedList
	end,
	ParseResultList.
	
read_txrecord_by_txid(TxId) ->
	case ar_util:safe_decode(TxId) of
		{ok, ID} ->
			case ar_storage:read_tx(ID) of
				unavailable ->
					?LOG_INFO([{txId_____read_tx______unavailable_____________________, TxId}]),
					case ar_mempool:get_tx(ID) of
						not_found ->
							case ar_kv:get(xwe_storage_txid_in_bundle, ar_util:encode(ID))  of
								not_found ->
									[];
								{ok, BundleTxBinary} ->
									binary_to_term(BundleTxBinary)
							end;
						TX ->
							FromAddress = ar_util:encode(ar_wallet:to_address(TX#tx.owner, TX#tx.signature_type)),
							TargetAddress = ar_util:encode(TX#tx.target),									
							Tags = lists:map(
									fun({Name, Value}) ->
										{[{name, Name},{value, Value}]}
									end,
									TX#tx.tags),
							TagsMap = lists:map(
									fun({Name, Value}) ->
										{Name, Value}
									end,
									TX#tx.tags),
							DataType = find_value_in_tags(<<"Content-Type">>, TagsMap),
							TxListMap = #{
								<<"id">> => ar_util:encode(TX#tx.id),
								<<"owner">> => #{<<"address">> => FromAddress},
								<<"recipient">> => TargetAddress,
								<<"quantity">> => #{<<"winston">> => TX#tx.quantity, <<"xwe">>=> float(TX#tx.quantity) / float(?WINSTON_PER_AR)},
								<<"fee">> => #{<<"winston">> => TX#tx.reward, <<"xwe">>=> float(TX#tx.reward) / float(?WINSTON_PER_AR)},
								<<"data">> => #{<<"size">> => TX#tx.data_size, <<"type">> => DataType},
								<<"block">> => #{},
								<<"tags">> => Tags
							},
							TxListMap;
						_ -> 
							[]
					end;
				#tx{} = TX ->
					FromAddress = ar_util:encode(ar_wallet:to_address(TX#tx.owner, TX#tx.signature_type)),
					TargetAddress = ar_util:encode(TX#tx.target),									
					Tags = lists:map(
							fun({Name, Value}) ->
								{[{name, Name},{value, Value}]}
							end,
							TX#tx.tags),
					TagsMap = lists:map(
							fun({Name, Value}) ->
								{Name, Value}
							end,
							TX#tx.tags),
					DataType = find_value_in_tags(<<"Content-Type">>, TagsMap),
					case ar_kv:get(xwe_storage_txid_block_db, ar_util:encode(TX#tx.id)) of
						{ok, BlockInfoByTxIdBinary} ->
							BlockInfoByTxId = binary_to_term(BlockInfoByTxIdBinary),
							TxListMap = #{
								<<"id">> => ar_util:encode(TX#tx.id),
								<<"owner">> => #{<<"address">> => FromAddress},
								<<"recipient">> => TargetAddress,
								<<"quantity">> => #{<<"winston">> => TX#tx.quantity, <<"xwe">>=> float(TX#tx.quantity) / float(?WINSTON_PER_AR)},
								<<"fee">> => #{<<"winston">> => TX#tx.reward, <<"xwe">>=> float(TX#tx.reward) / float(?WINSTON_PER_AR)},
								<<"data">> => #{<<"size">> => TX#tx.data_size, <<"type">> => DataType},
								<<"block">> => #{<<"height">> => lists:nth(1, BlockInfoByTxId), <<"indep_hash">> => list_to_binary(binary_to_list(lists:nth(2, BlockInfoByTxId))), <<"timestamp">> => lists:nth(3, BlockInfoByTxId) },
								<<"tags">> => Tags
							},
							TxListMap;
						not_found ->
							[]
					end
			end
	end.

read_txsrecord_function(TxIdList) ->
	lists:map(
		fun(X) -> 
			case ar_util:safe_decode(X) of
				{ok, ID} ->
					case ar_storage:read_tx(ID) of
						unavailable ->
							case ar_kv:get(xwe_storage_txid_in_bundle, ar_util:encode(ID))  of
								not_found ->
									[];
								{ok, BundleTxBinary} ->
									binary_to_term(BundleTxBinary)
							end;
						#tx{} = TX ->
							FromAddress = ar_util:encode(ar_wallet:to_address(TX#tx.owner, TX#tx.signature_type)),
							TargetAddress = ar_util:encode(TX#tx.target),									
							Tags = lists:map(
									fun({Name, Value}) ->
										{[{name, Name},{value, Value}]}
									end,
									TX#tx.tags),
							TagsMap = lists:map(
									fun({Name, Value}) ->
										{Name, Value}
									end,
									TX#tx.tags),
							DataType = find_value_in_tags(<<"Content-Type">>, TagsMap),
							case ar_kv:get(xwe_storage_txid_block_db, ar_util:encode(TX#tx.id)) of
								{ok, BlockInfoByTxIdBinary} ->
									BlockInfoByTxId = binary_to_term(BlockInfoByTxIdBinary),
									TxListMap = #{
										<<"id">> => ar_util:encode(TX#tx.id),
										<<"owner">> => #{<<"address">> => FromAddress},
										<<"recipient">> => TargetAddress,
										<<"quantity">> => #{<<"winston">> => TX#tx.quantity, <<"xwe">>=> float(TX#tx.quantity) / float(?WINSTON_PER_AR)},
										<<"fee">> => #{<<"winston">> => TX#tx.reward, <<"xwe">>=> float(TX#tx.reward) / float(?WINSTON_PER_AR)},
										<<"data">> => #{<<"size">> => TX#tx.data_size, <<"type">> => DataType},
										<<"block">> => #{<<"height">> => lists:nth(1, BlockInfoByTxId), <<"indep_hash">> => list_to_binary(binary_to_list(lists:nth(2, BlockInfoByTxId))), <<"timestamp">> => lists:nth(3, BlockInfoByTxId) },
										<<"tags">> => Tags
									},
									TxListMap;
								not_found ->
									[]
							end
					end
			end
		end, TxIdList).

get_address_txs(Addr) ->
	case ar_kv:get(address_tx_db, Addr) of
		not_found ->
			0;
		{ok, TxIdBinary} ->
			AllArray = binary_to_term(TxIdBinary),
			Length = length(AllArray),
			Length
	end.

read_txsrecord_by_addr(Addr, PageId, PageRecords) ->
	try binary_to_integer(PageRecords) of
		PageRecordsInt ->				
			PageRecordsNew = if
				PageRecordsInt < 0 -> 1;
				PageRecordsInt > 100 -> 100;
				true -> PageRecordsInt
			end,
			try binary_to_integer(PageId) of
				PageIdInt ->
					PageIdNew = if
						PageIdInt < 0 -> 0;
						true -> PageIdInt
					end,
					case ar_kv:get(address_tx_db, Addr) of
						not_found ->
							[];
						{ok, TxIdBinary} ->
							AllArray = binary_to_term(TxIdBinary),
							Length = length(AllArray),
							FromIndex = PageIdNew * PageRecordsNew,
							FromIndexNew = if
								FromIndex < 1 -> 1;
								FromIndex > Length -> Length;
								true -> FromIndex
							end,
							TargetResult = lists:sublist(AllArray, FromIndexNew, PageRecordsNew),
							TxRecordFunction = read_txsrecord_function(TargetResult),
							TxRecordFunctionPlusMemPool = case PageIdNew == 0 of
																true ->
																	MyPendingTxsRecord = ar_storage:get_mempool_tx_txs_records(Addr),
																	lists:append(MyPendingTxsRecord, TxRecordFunction);
																false ->
																	TxRecordFunction
															end,
							TxsResult = #{
								<<"data">> => TxRecordFunctionPlusMemPool,
								<<"total">> => Length,
								<<"from">> => FromIndexNew,
								<<"pageid">> => PageIdNew,
								<<"pagesize">> => PageRecordsNew,
								<<"allpages">> => ceil(Length / PageRecordsNew) div 1
							},
							TxsResult
					end
			catch _:_ ->
				[]
			end
	catch _:_ ->
		[]
	end.

read_txsrecord_by_addr_deposits(Addr, PageId, PageRecords) ->
	try binary_to_integer(PageRecords) of
		PageRecordsInt ->				
			PageRecordsNew = if
				PageRecordsInt < 0 -> 1;
				PageRecordsInt > 100 -> 100;
				true -> PageRecordsInt
			end,
			try binary_to_integer(PageId) of
				PageIdInt ->
					PageIdNew = if
						PageIdInt < 0 -> 0;
						true -> PageIdInt
					end,
					case ar_kv:get(address_tx_deposits_db, Addr) of
						not_found ->
							[];
						{ok, TxIdBinary} ->
							AllArray = binary_to_term(TxIdBinary),
							Length = length(AllArray),
							FromIndex = PageIdNew * PageRecordsNew,
							FromIndexNew = if
								FromIndex < 1 -> 1;
								FromIndex > Length -> Length;
								true -> FromIndex
							end,
							TargetResult = lists:sublist(AllArray, FromIndexNew, PageRecordsNew),
							TxRecordFunction = read_txsrecord_function(TargetResult),
							TxRecordFunctionPlusMemPool = case PageIdNew == 0 of
																true ->
																	MyPendingTxsRecord = ar_storage:get_mempool_tx_deposits_records(Addr),
																	lists:append(MyPendingTxsRecord, TxRecordFunction);
																false ->
																	TxRecordFunction
															end,
							TxsResult = #{
								<<"data">> => TxRecordFunctionPlusMemPool,
								<<"total">> => Length,
								<<"from">> => FromIndexNew,
								<<"pageid">> => PageIdNew,
								<<"pagesize">> => PageRecordsNew,
								<<"allpages">> => ceil(Length / PageRecordsNew) div 1
							},
							TxsResult
					end
			catch _:_ ->
				[]
			end
	catch _:_ ->
		[]
	end.

read_txsrecord_by_addr_send(Addr, PageId, PageRecords) ->
	try binary_to_integer(PageRecords) of
		PageRecordsInt ->				
			PageRecordsNew = if
				PageRecordsInt < 0 -> 1;
				PageRecordsInt > 100 -> 100;
				true -> PageRecordsInt
			end,
			try binary_to_integer(PageId) of
				PageIdInt ->
					PageIdNew = if
						PageIdInt < 0 -> 0;
						true -> PageIdInt
					end,
					case ar_kv:get(address_tx_send_db, Addr) of
						not_found ->
							[];
						{ok, TxIdBinary} ->
							AllArray = binary_to_term(TxIdBinary),
							Length = length(AllArray),
							FromIndex = PageIdNew * PageRecordsNew,
							FromIndexNew = if
								FromIndex < 1 -> 1;
								FromIndex > Length -> Length;
								true -> FromIndex
							end,
							TargetResult = lists:sublist(AllArray, FromIndexNew, PageRecordsNew),
							TxRecordFunction = read_txsrecord_function(TargetResult),
							TxRecordFunctionPlusMemPool = case PageIdNew == 0 of
																true ->
																	MyPendingTxsRecord = ar_storage:get_mempool_tx_send_records(Addr),
																	lists:append(MyPendingTxsRecord, TxRecordFunction);
																false ->
																	TxRecordFunction
															end,
							TxsResult = #{
								<<"data">> => TxRecordFunctionPlusMemPool,
								<<"total">> => Length,
								<<"from">> => FromIndexNew,
								<<"pageid">> => PageIdNew,
								<<"pagesize">> => PageRecordsNew,
								<<"allpages">> => ceil(Length / PageRecordsNew) div 1
							},
							TxsResult
					end
			catch _:_ ->
				[]
			end
	catch _:_ ->
		[]
	end.

read_datarecord_function(TxIdList) ->
	lists:map(
				fun(X) -> 
					case ar_util:safe_decode(X) of
						{ok, ID} ->
							case ar_storage:read_tx(ID) of
								unavailable ->
									ok;
								#tx{} = TX ->
									FromAddress = ar_util:encode(ar_wallet:to_address(TX#tx.owner, TX#tx.signature_type)),
									TargetAddress = ar_util:encode(TX#tx.target),									
									Tags = lists:map(
											fun({Name, Value}) ->
												{[{name, Name},{value, Value}]}
											end,
											TX#tx.tags),
									TagsMap = lists:map(
											fun({Name, Value}) ->
												{Name, Value}
											end,
											TX#tx.tags),
									DataType = find_value_in_tags(<<"Content-Type">>, TagsMap),
									case ar_kv:get(xwe_storage_txid_block_db, ar_util:encode(TX#tx.id)) of
										{ok, BlockInfoByTxIdBinary} ->
											BlockInfoByTxId = binary_to_term(BlockInfoByTxIdBinary),
											TxListMap = #{
												<<"id">> => ar_util:encode(TX#tx.id),
												<<"owner">> => #{<<"address">> => FromAddress},
												<<"recipient">> => TargetAddress,
												<<"quantity">> => #{<<"winston">> => TX#tx.quantity, <<"xwe">>=> float(TX#tx.quantity) / float(?WINSTON_PER_AR)},
												<<"fee">> => #{<<"winston">> => TX#tx.reward, <<"xwe">>=> float(TX#tx.reward) / float(?WINSTON_PER_AR)},
												<<"data">> => #{<<"size">> => TX#tx.data_size, <<"type">> => DataType},
												<<"block">> => #{<<"height">> => lists:nth(1, BlockInfoByTxId), <<"indep_hash">> => list_to_binary(binary_to_list(lists:nth(2, BlockInfoByTxId))), <<"timestamp">> => lists:nth(3, BlockInfoByTxId) },
												<<"tags">> => Tags
											},
											TxListMap;
										not_found ->
											[]
									end
							end
					end
				end, TxIdList).

get_mempool_tx_data_records(Addr) ->
	IsMySelfTx = fun(TxId) ->
		case ar_mempool:get_tx(TxId) of
			not_found ->
				false;
			TX ->
				FromAddress = ar_util:encode(ar_wallet:to_address(TX#tx.owner, TX#tx.signature_type)),
				TargetAddress = ar_util:encode(TX#tx.target),	
				% ?LOG_INFO([{get_mempool_tx_data_records_______________________TargetAddress_______________________________, TargetAddress}]),
				case TX#tx.data_size > 0 of
					true -> 
						case TargetAddress of
							Addr -> true;
							<<>> -> 
								case FromAddress of
									Addr -> true;
									_ -> 
										false
								end;
							_ -> false
						end;
					false -> false
				end
		end
	end,
	MyPendingTxs = lists:filter(
		IsMySelfTx,
		ar_mempool:get_all_txids()
	),
	MyPendingTxsRecord = lists:map(
		fun(TxId) ->
			case ar_mempool:get_tx(TxId) of
				TX ->
					FromAddress = ar_util:encode(ar_wallet:to_address(TX#tx.owner, TX#tx.signature_type)),
					TargetAddress = ar_util:encode(TX#tx.target),									
					Tags = lists:map(
							fun({Name, Value}) ->
								{[{name, Name},{value, Value}]}
							end,
							TX#tx.tags),
					TagsMap = lists:map(
							fun({Name, Value}) ->
								{Name, Value}
							end,
							TX#tx.tags),
					DataType = find_value_in_tags(<<"Content-Type">>, TagsMap),
					TxListMap = #{
						<<"id">> => ar_util:encode(TX#tx.id),
						<<"owner">> => #{<<"address">> => FromAddress},
						<<"recipient">> => TargetAddress,
						<<"quantity">> => #{<<"winston">> => TX#tx.quantity, <<"xwe">>=> float(TX#tx.quantity) / float(?WINSTON_PER_AR)},
						<<"fee">> => #{<<"winston">> => TX#tx.reward, <<"xwe">>=> float(TX#tx.reward) / float(?WINSTON_PER_AR)},
						<<"data">> => #{<<"size">> => TX#tx.data_size, <<"type">> => DataType},
						<<"block">> => #{},
						<<"tags">> => Tags
					},
					TxListMap
			end
		end,
		MyPendingTxs
	),
	MyPendingTxsRecord.

get_mempool_tx_send_records(Addr) ->
	IsMySelfTx = fun(TxId) ->
		case ar_mempool:get_tx(TxId) of
			not_found ->
				false;
			TX ->
				FromAddress = ar_util:encode(ar_wallet:to_address(TX#tx.owner, TX#tx.signature_type)),
				% ?LOG_INFO([{get_mempool_tx_send_recordss____________FromAddress__________________________________________, FromAddress}]),
				case FromAddress of
					Addr -> true;
					_ -> false
				end
		end
	end,
	MyPendingTxs = lists:filter(
		IsMySelfTx,
		ar_mempool:get_all_txids()
	),
	MyPendingTxsRecord = lists:map(
		fun(TxId) ->
			case ar_mempool:get_tx(TxId) of
				TX ->
					FromAddress = ar_util:encode(ar_wallet:to_address(TX#tx.owner, TX#tx.signature_type)),
					TargetAddress = ar_util:encode(TX#tx.target),									
					Tags = lists:map(
							fun({Name, Value}) ->
								{[{name, Name},{value, Value}]}
							end,
							TX#tx.tags),
					TagsMap = lists:map(
							fun({Name, Value}) ->
								{Name, Value}
							end,
							TX#tx.tags),
					DataType = find_value_in_tags(<<"Content-Type">>, TagsMap),
					TxListMap = #{
						<<"id">> => ar_util:encode(TX#tx.id),
						<<"owner">> => #{<<"address">> => FromAddress},
						<<"recipient">> => TargetAddress,
						<<"quantity">> => #{<<"winston">> => TX#tx.quantity, <<"xwe">>=> float(TX#tx.quantity) / float(?WINSTON_PER_AR)},
						<<"fee">> => #{<<"winston">> => TX#tx.reward, <<"xwe">>=> float(TX#tx.reward) / float(?WINSTON_PER_AR)},
						<<"data">> => #{<<"size">> => TX#tx.data_size, <<"type">> => DataType},
						<<"block">> => #{},
						<<"tags">> => Tags
					},
					TxListMap
			end
		end,
		MyPendingTxs
	),
	MyPendingTxsRecord.

get_mempool_tx_deposits_records(Addr) ->
	IsMySelfTx = fun(TxId) ->
		case ar_mempool:get_tx(TxId) of
			not_found ->
				false;
			TX ->
				% FromAddress = ar_util:encode(ar_wallet:to_address(TX#tx.owner, TX#tx.signature_type)),
				TargetAddress = ar_util:encode(TX#tx.target),	
				% ?LOG_INFO([{get_mempool_tx_deposits_records_______________________TargetAddress_______________________________, TargetAddress}]),
				case TargetAddress of
					Addr -> true;
					_ -> false
				end
		end
	end,
	MyPendingTxs = lists:filter(
		IsMySelfTx,
		ar_mempool:get_all_txids()
	),
	MyPendingTxsRecord = lists:map(
		fun(TxId) ->
			case ar_mempool:get_tx(TxId) of
				TX ->
					FromAddress = ar_util:encode(ar_wallet:to_address(TX#tx.owner, TX#tx.signature_type)),
					TargetAddress = ar_util:encode(TX#tx.target),									
					Tags = lists:map(
							fun({Name, Value}) ->
								{[{name, Name},{value, Value}]}
							end,
							TX#tx.tags),
					TagsMap = lists:map(
							fun({Name, Value}) ->
								{Name, Value}
							end,
							TX#tx.tags),
					DataType = find_value_in_tags(<<"Content-Type">>, TagsMap),
					TxListMap = #{
						<<"id">> => ar_util:encode(TX#tx.id),
						<<"owner">> => #{<<"address">> => FromAddress},
						<<"recipient">> => TargetAddress,
						<<"quantity">> => #{<<"winston">> => TX#tx.quantity, <<"xwe">>=> float(TX#tx.quantity) / float(?WINSTON_PER_AR)},
						<<"fee">> => #{<<"winston">> => TX#tx.reward, <<"xwe">>=> float(TX#tx.reward) / float(?WINSTON_PER_AR)},
						<<"data">> => #{<<"size">> => TX#tx.data_size, <<"type">> => DataType},
						<<"block">> => #{},
						<<"tags">> => Tags
					},
					TxListMap
			end
		end,
		MyPendingTxs
	),
	MyPendingTxsRecord.


get_mempool_tx_txs_records(Addr) ->
	IsMySelfTx = fun(TxId) ->
		case ar_mempool:get_tx(TxId) of
			not_found ->
				false;
			TX ->
				FromAddress = ar_util:encode(ar_wallet:to_address(TX#tx.owner, TX#tx.signature_type)),
				TargetAddress = ar_util:encode(TX#tx.target),	
				% ?LOG_INFO([{targetAddress___________________________, TargetAddress}]),
				case FromAddress of
					Addr -> true;
					_ -> 
						case TargetAddress of
							Addr -> true;
							_ -> false
						end
				end				
		end
	end,
	MyPendingTxs = lists:filter(
		IsMySelfTx,
		ar_mempool:get_all_txids()
	),
	MyPendingTxsRecord = lists:map(
		fun(TxId) ->
			case ar_mempool:get_tx(TxId) of
				TX ->
					FromAddress = ar_util:encode(ar_wallet:to_address(TX#tx.owner, TX#tx.signature_type)),
					TargetAddress = ar_util:encode(TX#tx.target),									
					Tags = lists:map(
							fun({Name, Value}) ->
								{[{name, Name},{value, Value}]}
							end,
							TX#tx.tags),
					TagsMap = lists:map(
							fun({Name, Value}) ->
								{Name, Value}
							end,
							TX#tx.tags),
					DataType = find_value_in_tags(<<"Content-Type">>, TagsMap),
					TxListMap = #{
						<<"id">> => ar_util:encode(TX#tx.id),
						<<"owner">> => #{<<"address">> => FromAddress},
						<<"recipient">> => TargetAddress,
						<<"quantity">> => #{<<"winston">> => TX#tx.quantity, <<"xwe">>=> float(TX#tx.quantity) / float(?WINSTON_PER_AR)},
						<<"fee">> => #{<<"winston">> => TX#tx.reward, <<"xwe">>=> float(TX#tx.reward) / float(?WINSTON_PER_AR)},
						<<"data">> => #{<<"size">> => TX#tx.data_size, <<"type">> => DataType},
						<<"block">> => #{},
						<<"tags">> => Tags
					},
					TxListMap
			end
		end,
		MyPendingTxs
	),
	MyPendingTxsRecord.

get_mempool_tx_txs_records() ->
	PendingTxs = ar_mempool:get_all_txids(),
	PendingTxsRecord = lists:map(
		fun(TxId) ->
			case ar_mempool:get_tx(TxId) of
				TX ->
					FromAddress = ar_util:encode(ar_wallet:to_address(TX#tx.owner, TX#tx.signature_type)),
					TargetAddress = ar_util:encode(TX#tx.target),									
					Tags = lists:map(
							fun({Name, Value}) ->
								{[{name, Name},{value, Value}]}
							end,
							TX#tx.tags),
					TagsMap = lists:map(
							fun({Name, Value}) ->
								{Name, Value}
							end,
							TX#tx.tags),
					DataType = find_value_in_tags(<<"Content-Type">>, TagsMap),
					TxListMap = #{
						<<"id">> => ar_util:encode(TX#tx.id),
						<<"owner">> => #{<<"address">> => FromAddress},
						<<"recipient">> => TargetAddress,
						<<"quantity">> => #{<<"winston">> => TX#tx.quantity, <<"xwe">>=> float(TX#tx.quantity) / float(?WINSTON_PER_AR)},
						<<"fee">> => #{<<"winston">> => TX#tx.reward, <<"xwe">>=> float(TX#tx.reward) / float(?WINSTON_PER_AR)},
						<<"data">> => #{<<"size">> => TX#tx.data_size, <<"type">> => DataType},
						<<"block">> => #{},
						<<"tags">> => Tags
					},
					TxListMap
			end
		end,
		PendingTxs
	),
	PendingTxsRecord.

read_datarecord_by_addr(Addr, PageId, PageRecords) ->
	try binary_to_integer(PageRecords) of
		PageRecordsInt ->				
			PageRecordsNew = if
				PageRecordsInt < 0 -> 1;
				PageRecordsInt > 100 -> 100;
				true -> PageRecordsInt
			end,
			try binary_to_integer(PageId) of
				PageIdInt ->
					PageIdNew = if
						PageIdInt < 0 -> 0;
						true -> PageIdInt
					end,
					case ar_kv:get(address_data_db, Addr) of
						not_found ->
							[];
						{ok, TxIdBinary} ->
							AllArray = binary_to_term(TxIdBinary),
							Length = length(AllArray),
							FromIndex = PageIdNew * PageRecordsNew,
							FromIndexNew = if
								FromIndex < 1 -> 1;
								FromIndex > Length -> Length;
								true -> FromIndex
							end,
							TargetResult = lists:sublist(AllArray, FromIndexNew, PageRecordsNew),
							TxRecordFunction = read_datarecord_function(TargetResult),
							TxRecordFunctionPlusMemPool = case PageIdNew == 0 of
																true ->
																	MyPendingTxsRecord = ar_storage:get_mempool_tx_data_records(Addr),
																	lists:append(MyPendingTxsRecord, TxRecordFunction);
																false ->
																	TxRecordFunction
															end,
							TxsResult = #{
								<<"data">> => TxRecordFunctionPlusMemPool,
								<<"total">> => Length,
								<<"from">> => FromIndexNew,
								<<"pageid">> => PageIdNew,
								<<"pagesize">> => PageRecordsNew,
								<<"allpages">> => ceil(Length / PageRecordsNew) div 1
							},
							TxsResult
					end
			catch _:_ ->
				[]
			end
	catch _:_ ->
		[]
	end.

read_data_by_addr(Addr, PageId, PageRecords) ->
	try binary_to_integer(PageRecords) of
		PageRecordsInt ->				
			PageRecordsNew = if
				PageRecordsInt < 0 -> 1;
				PageRecordsInt > 100 -> 100;
				true -> PageRecordsInt
			end,
			try binary_to_integer(PageId) of
				PageIdInt ->
					PageIdNew = if
						PageIdInt < 0 -> 0;
						true -> PageIdInt
					end,
					case ar_kv:get(address_data_db, Addr) of
						not_found ->
							[];
						{ok, TxIdBinary} ->
							AllArray = binary_to_term(TxIdBinary),
							Length = length(AllArray),
							FromIndex = PageIdNew * PageRecordsNew,
							FromIndexNew = if
								FromIndex < 1 -> 1;
								FromIndex > Length -> Length;
								true -> FromIndex
							end,
							lists:sublist(AllArray, FromIndexNew, PageRecordsNew)
					end
			catch _:_ ->
				[]
			end
	catch _:_ ->
		[]
	end.

parse_bundle_data(TxData, TX, PageId, PageRecords, IsReturn) ->
	%% Begin to save unbundle data
	{ok, Config} = application:get_env(chivesweave, config),
	DataDir = Config#config.data_dir,
	filelib:ensure_dir(filename:join(DataDir, ?UNBUNDLE_DATA_DIR) ++ "/"),

	BlockStructure 	= 	case ar_kv:get(xwe_storage_txid_block_db, ar_util:encode(TX#tx.id)) of
							{ok, BlockInfoByTxIdBinary} ->
								BlockInfoByTxId = binary_to_term(BlockInfoByTxIdBinary),
								BlockStructure2 = #{<<"height">> => lists:nth(1, BlockInfoByTxId), <<"indep_hash">> => list_to_binary(binary_to_list(lists:nth(2, BlockInfoByTxId))), <<"timestamp">> => lists:nth(3, BlockInfoByTxId) },
								BlockStructure2;
							not_found ->
								[]
						end,

	% ?LOG_INFO([{handle_get_tx_unbundle______________________________parse_bundle_data, BlockStructure}]),

	%% Begin to parse bundle data	
	GetDataItemCount = binary:decode_unsigned(binary:part(TxData, {0, 32}), little),
	case GetDataItemCount > 20000 of
		true -> 
			[];
		_ -> 
			HEADER_START = 32,
			case ets:lookup(cache_table, ar_util:encode(TX#tx.id)) of
				[] ->
					ets:insert(cache_table, {ar_util:encode(TX#tx.id), 0});
				[{Key, _}] ->
					ets:delete(cache_table, Key),
					ets:insert(cache_table, {Key, 0})
			end,
			% ?LOG_INFO([{handle_get_tx_unbundle______________________________GetDataItemCount, GetDataItemCount}]),
			GetBundleStart = HEADER_START + 64 * GetDataItemCount,
			GetNumbersList = lists:seq(0, GetDataItemCount - 1),
			
			% ?LOG_INFO([{handle_get_tx_unbundle______________________________GetNumbersList, GetNumbersList}]),

			GetDataItems = lists:map(
				fun(Index) ->
					I = HEADER_START + Index * 64,
					_OFFSET = binary:decode_unsigned(binary:part(TxData, {I, 32}), little),
					_ID = binary:part(TxData, {I + 32, 32}),
					DataItemId = ar_util:encode(_ID),
					case byte_size(_ID) == 0 of
						true ->
							?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle____ID_NOT_VALID, DataItemId}]);
						false ->
							?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle____IS__VALID, DataItemId}]),
							ok
					end,
					OFFSET_START_ITEM = case ets:lookup(cache_table, ar_util:encode(TX#tx.id)) of
											[{_, Value_OFFSET}] ->
												Value_OFFSET
										end,
					DataItemStart = GetBundleStart + OFFSET_START_ITEM,
					DataItemBytes = binary:part(TxData, {DataItemStart, _OFFSET}),
					% ?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle____DataItemStart, DataItemStart}]),
					% ?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle_____OFFSET, _OFFSET}]),
					case ets:lookup(cache_table, ar_util:encode(TX#tx.id)) of
						[{_, ValueItem1}] ->
							ets:delete(cache_table, ar_util:encode(TX#tx.id)),
							ets:insert(cache_table, {ar_util:encode(TX#tx.id), ValueItem1 + _OFFSET})
					end,
					% Tab2list = ets:tab2list(cache_table),
					% ?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle_____Tab2list, Tab2list}]),
					SignatureTypeVal = binary:decode_unsigned(binary:part(DataItemBytes, {0, 2}), little),
					%% [w.ARWEAVE]:{sigLength:512,pubLength:512,sigName:"arweave"},
					%% [w.ED25519]:{sigLength:64,pubLength:32,sigName:"ed25519"},
					%% [w.ETHEREUM]:{sigLength:65,pubLength:65,sigName:"ethereum"}
					TxStructure = case SignatureTypeVal of
						1 ->
							%% Arweave or Chivesweave
							SigLength = 512,
							SignatureBinary = binary:part(DataItemBytes, {2, SigLength}),
							Signature = ar_util:encode(SignatureBinary),
							% ?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle____Signature, Signature}]),
							PubLength = 512,
							% ?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle____DataItemBytes, {2 + SigLength, PubLength}}]),
							OwnerBinary = binary:part(DataItemBytes, {2 + SigLength, PubLength}),
							%% Owner = ar_util:encode(OwnerBinary),
							% ?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle____Owner, OwnerBinary}]),
							TargetBinaryMark = binary:part(DataItemBytes, {2 + SigLength + PubLength, 1}),
							TargetBinaryLength = case TargetBinaryMark of
								<<1>> ->
									TargetBinary = binary:part(DataItemBytes, {2 + SigLength + PubLength + 1, 32}),
									Target = ar_util:encode(TargetBinary),
									% ?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle____TargetBinary, TargetBinary}]),
									% ?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle____Target, Target}]),
									32 + 1;
								<<0>> ->
									Target = <<>>,
									% ?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle____TargetBinaryMark, TargetBinaryMark}]),
									1
							end,
							AnchorBinaryMark = binary:part(DataItemBytes, {2 + SigLength + PubLength + TargetBinaryLength, 1}),
							AnchorBinaryLength = case AnchorBinaryMark of
								<<1>> ->
									Anchor = binary:part(DataItemBytes, {2 + SigLength + PubLength + TargetBinaryLength + 1, 32}),
									% ?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle____Anchor, Anchor}]),
									32 + 1;
								<<0>> ->
									Anchor = <<>>,
									% ?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle____AnchorBinaryMark, AnchorBinaryMark}]),
									1
							end,
							% ?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle____AnchorBinaryLength, AnchorBinaryLength}]),
							% TagsNumbers = binary:decode_unsigned(binary:part(DataItemBytes, {2 + SigLength + PubLength + TargetBinaryLength + AnchorBinaryLength, 8}), little),
							% ?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle____TagsNumbers, TagsNumbers}]),
							TagsBytesNumbers = binary:decode_unsigned(binary:part(DataItemBytes, {2 + SigLength + PubLength + TargetBinaryLength + AnchorBinaryLength + 8, 8}), little),
							% ?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle____TagsBytesNumbers, TagsBytesNumbers}]),

							TagsBytesContent = binary:part(DataItemBytes, {2 + SigLength + PubLength + TargetBinaryLength + AnchorBinaryLength + 8 + 8, TagsBytesNumbers}),
							% ?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle____TagsBytesContent, TagsBytesContent}]),

							DataItemDataStart = 2 + SigLength + PubLength + TargetBinaryLength + AnchorBinaryLength + 8 + 8 + TagsBytesNumbers,
							DataItemContent = binary:part(DataItemBytes, {DataItemDataStart, byte_size(DataItemBytes) - DataItemDataStart}),
							% ?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle____DataItemContent, DataItemContent}]),

							TagType = avro_record:type(
								<<"rec">>,
								[avro_record:define_field(name, string), avro_record:define_field(value, string)],
								[{namespace, 'Tag'}]
							),
							TagsType = avro_array:type(TagType),
							% ?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle____TagsType, TagsType}]),
							Decoder = avro:make_simple_decoder(TagsType, []),
							TagsList = case TagsBytesContent of
											<<>> -> [];
											Data -> Decoder(Data)
										end,
							% ?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle____TagsList, TagsList}]),
							Tags 	= lists:map(
											fun(Tag) ->
												{_, TagName} = lists:nth(1, Tag),
												{_, TagValue} = lists:nth(2, Tag),
												{[{name, TagName},{value, TagValue}]}
											end,
											TagsList),
							TagsMap = lists:map(
											fun(Tag) ->
												{_, TagName} = lists:nth(1, Tag),
												{_, TagValue} = lists:nth(2, Tag),
												{TagName, TagValue}
											end,
											TagsList),
							% ?LOG_INFO([{handle_get_tx_unbundle________avro_record____TagsList, TagsList}]),
							% ?LOG_INFO([{handle_get_tx_unbundle________avro_record____TagsMap, TagsMap}]),
							% ?LOG_INFO([{handle_get_tx_unbundle________avro_record____Tags, Tags}]),
							
							FromAddress = ar_util:encode(ar_wallet:to_address(OwnerBinary, {rsa, 65537})),
							DataType = find_value_in_tags(<<"Content-Type">>, TagsMap),
							FileTxId = find_value_in_tags(<<"File-TxId">>, TagsMap),
							TxRecord =  case ar_util:safe_decode(FileTxId) of
											{ok, ID} ->
												case ar_kv:get(xwe_storage_txid_in_bundle, ar_util:encode(ID))  of
													{ok, BundleTxBinary} ->
														binary_to_term(BundleTxBinary);
													_ ->
														[]
												end;
											_ -> 
												[]
										end,
							TxStructureItem = #{
									<<"id">> => DataItemId,
									<<"owner">> => #{<<"address">> => FromAddress},
									<<"recipient">> => Target,
									<<"quantity">> => #{<<"winston">> => 0, <<"xwe">>=> float(0) / float(?WINSTON_PER_AR)},
									<<"fee">> => #{<<"winston">> => 0, <<"xwe">>=> float(0) / float(?WINSTON_PER_AR)},
									<<"data">> => #{<<"size">> => byte_size(DataItemContent), <<"type">> => DataType},
									<<"block">> => BlockStructure,
									<<"bundleid">> => ar_util:encode(TX#tx.id),
									<<"anchor">> => Anchor,
									<<"tags">> => Tags
								},
							
							%% Write Unbundle tx to arql
							% ?LOG_INFO([{handle_get_tx_unbundle______________________________FileTxId, FileTxId}]),
							% ?LOG_INFO([{handle_get_tx_unbundle______________________________TagsMap, TagsMap}]),
							% ?LOG_INFO([{handle_get_tx_unbundle______________________________TxStructureItem, TxStructureItem}]),
							% ?LOG_INFO([{handle_get_tx_unbundle_____________________________________________________BlockStructure_height, maps:get(<<"height">>, BlockStructure)}]),
							case lists:member(serve_arql, Config#config.enable) of
								true ->
									case maps:is_key(<<"height">>, BlockStructure) of
										true ->
											%% Insert into Txs & Tags
											BlockHeight = maps:get(<<"height">>, BlockStructure),
											BlockHash = maps:get(<<"indep_hash">>, BlockStructure),
											BlockTimestamp = maps:get(<<"timestamp">>, BlockStructure),
											FileName = ar_storage:find_value_in_tags(<<"File-Name">>, TagsMap),
											ContentType = ar_storage:find_value_in_tags(<<"Content-Type">>, TagsMap),
											FileType = ar_storage:contentTypeToFileType(ContentType),
											FileParent = ar_storage:find_value_in_tags(<<"File-Parent">>, TagsMap),
											FileHash = ar_storage:find_value_in_tags(<<"File-Hash">>, TagsMap),
											FileSummary = ar_storage:find_value_in_tags(<<"File-Summary">>, TagsMap),
											CipherALG = ar_storage:find_value_in_tags(<<"Cipher-ALG">>, TagsMap),
											IsPublic = ar_storage:find_value_in_tags(<<"File-Public">>, TagsMap),
											EntityType = ar_storage:find_value_in_tags(<<"Entity-Type">>, TagsMap),
											AppName = ar_storage:find_value_in_tags(<<"App-Name">>, TagsMap),
											AppVersion = ar_storage:find_value_in_tags(<<"App-Version">>, TagsMap),
											AppInstance = ar_storage:find_value_in_tags(<<"App-Instance">>, TagsMap),
											Bundleid = ar_util:encode(TX#tx.id),
											Item_star = <<"">>,
											Item_label = <<"">>,
											Item_download = <<"">>,
											Item_language = ar_storage:find_value_in_tags(<<"File-Language">>, TagsMap),
											Item_pages = ar_storage:find_value_in_tags(<<"File-Pages">>, TagsMap),
											Item_node_label = <<"">>,
											Item_node_group = <<"">>,
											Item_node_star = <<"">>,
											Item_node_hot = <<"">>,
											Item_node_delete = <<"">>,											
											Last_tx_action = DataItemId,
											BundleTxParse = <<"">>,
											TXFields = [
												DataItemId,
												BlockHash,
												ar_util:encode(TX#tx.last_tx),
												ar_util:encode(OwnerBinary),
												FromAddress,
												Target,
												0,
												Signature,
												0,
												BlockTimestamp,
												BlockHeight,
												byte_size(DataItemContent),
												Bundleid,
												FileName,
												FileType,
												FileParent,
												ContentType,
												FileHash,
												FileSummary,
												Item_star,
												Item_label,
												Item_download,
												Item_language,
												Item_pages,
												CipherALG,
												IsPublic,
												EntityType,
												AppName,
												AppVersion,
												AppInstance,
												Item_node_label,
												Item_node_group,
												Item_node_star,
												Item_node_hot,
												Item_node_delete,
												Last_tx_action,
												BundleTxParse
											],
											% ?LOG_INFO([{handle_get_tx_unbundle__________________________________________INSERT_ARQL___TXFields, TXFields}]),
											TagFieldsList = lists:map(
													fun(Tag) ->
														{_, TagName} = lists:nth(1, Tag),
														{_, TagValue} = lists:nth(2, Tag),
														[DataItemId, TagName, TagValue]
													end,
													TagsList),
											% ?LOG_INFO([{handle_get_tx_unbundle__________________________________________INSERT_ARQL___TagFieldsList, TagFieldsList}]),
											ar_arql_db:insert_tx(TXFields, TagFieldsList);
										false ->
											ok
									end;
								false ->
									ok
							end,
							
							%% Compress Image
							% ?LOG_INFO([{handle_get_tx_unbundle________DataType_DataType____DataType, DataType}]),
							% image_thumbnail_compress_to_storage(DataItemContent, FromAddress, DataItemId),

							%% Write Unbundle data to file
							filelib:ensure_dir(binary_to_list(filename:join([DataDir, ?UNBUNDLE_DATA_DIR, FromAddress])) ++ "/"),
							OriginalUnBundleDataFilePath = binary_to_list(filename:join([DataDir, ?UNBUNDLE_DATA_DIR, FromAddress, DataItemId])),
							case filelib:is_file(OriginalUnBundleDataFilePath) of
								true ->
									ok;
								false ->
									{ok, FileData} = file:open(OriginalUnBundleDataFilePath, [write, binary]),
									ok = file:write(FileData, DataItemContent),
									ok = file:close(FileData)
							end,

							%% Write Unbundle tx to rocksdb
							% ?LOG_INFO([{handle_get_tx_unbundle________avro_record____TxStructureItem, TxStructureItem}]),
							ar_kv:put(xwe_storage_txid_in_bundle, DataItemId, term_to_binary(TxStructureItem)),

							%% return Tx structure item
							TxStructureItem;
						2 ->
							%% ED25519
							ok;
						3 ->
							%% ETHEREUM
							ok;
						4 ->
							%% SOLANA
							ok;
						_ ->
							%% None
							ok
					end,
					TxStructure
				end,
				GetNumbersList
			),
			ar_arql_db:update_tx_bundletxparse(<<"1">>, ar_util:encode(TX#tx.id)),
			% ?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle_____GetDataItems, GetDataItems}]),
			case IsReturn of
				true ->
					try binary_to_integer(PageRecords) of
						PageRecordsInt ->				
							PageRecordsNew = if
								PageRecordsInt < 0 -> 5;
								PageRecordsInt > 100 -> 100;
								true -> PageRecordsInt
							end,
							try binary_to_integer(PageId) of
								PageIdInt ->
									MaxRecords = length(GetDataItems),
									AllPages = ceil(MaxRecords / PageRecordsNew),
									PageIdNew = if
										PageIdInt < 0 -> 0;
										PageIdInt > AllPages -> AllPages;
										true -> PageIdInt
									end,
									FromIndex = PageIdNew * PageRecordsNew + 1,
									FromHeightNew = if
										FromIndex < 1 -> 1;
										FromIndex > MaxRecords -> MaxRecords;
										true -> FromIndex
									end,
									TXIDsInPage = lists:sublist(GetDataItems, FromHeightNew, FromHeightNew + PageRecordsNew - 1 ),
									TxInfor = case read_txrecord_by_txid(ar_util:encode(TX#tx.id)) of
										not_found ->
											[];
										Res ->
											Res
									end,
									TxResult = #{ 
												<<"txs">> => TXIDsInPage, 
												<<"tx">> => TxInfor,
												<<"total">> => MaxRecords,
												<<"from">> => FromHeightNew,
												<<"pageid">> => PageIdNew,
												<<"pagesize">> => PageRecordsNew,
												<<"allpages">> => AllPages
												},
									TxResult
							catch _:_ ->
								TxInfor = case read_txrecord_by_txid(ar_util:encode(TX#tx.id)) of
									not_found ->
										[];
									Res ->
										Res
								end,
								TxResult = #{ 
											<<"txs">> => [], 
											<<"tx">> => TxInfor,
											<<"total">> => 0,
											<<"from">> => 0,
											<<"pageid">> => 0,
											<<"pagesize">> => 0,
											<<"allpages">> => 0
											},
								TxResult
							end
					catch _:_ ->
						TxInfor = case read_txrecord_by_txid(ar_util:encode(TX#tx.id)) of
							not_found ->
								[];
							Res ->
								Res
						end,
						TxResult = #{ 
									<<"txs">> => [], 
									<<"tx">> => TxInfor,
									<<"total">> => 0,
									<<"from">> => 0,
									<<"pageid">> => 0,
									<<"pagesize">> => 0,
									<<"allpages">> => 0
									},
						TxResult
					end;
				false ->
					% Not Return, only parse bundle data and into tx table
					[]
			end
	end.

image_thumbnail_compress_to_storage(Data, Address, TxId) ->
	%% Is image, and will to compress this image
	DataSize = byte_size(Data),
	case DataSize > 500 * 1024 of
		true ->
			%% Begin to comporess
			%% Step 1: copy the data as a file, the path is [ADDRESS]/[TX]
			{ok, Config} = application:get_env(chivesweave, config),
			DataDir = Config#config.data_dir,
			% Address = ar_util:encode(ar_wallet:to_address(TX#tx.owner, TX#tx.signature_type)),
			ImageThumbnailDir = binary_to_list(filename:join([DataDir, ?IMAGE_THUMBNAIL_DIR, Address])),
			filelib:ensure_dir(ImageThumbnailDir ++ "/"),
			OriginalFilePath = binary_to_list(filename:join([DataDir, ?IMAGE_THUMBNAIL_DIR, Address, TxId])),
			NewFilePath = binary_to_list(filename:join([DataDir, ?IMAGE_THUMBNAIL_DIR, Address, binary_to_list(TxId) ++ ".png"])),
			case file:read_file_info(NewFilePath) of
				{ok, _FileInfo} ->
					ok;
				_ ->
					%% First to copy data to file
					case file:open(OriginalFilePath, [write]) of
						{ok, File} ->
							case file:write(File, Data) of
								ok ->
									% ?LOG_INFO([{image_thumbnail_compress_to_storage_____________write, OriginalFilePath}]),
									%% Not Exist, need to compress									
									CompressCommand = "convert " ++ OriginalFilePath ++ " -resize 600x " ++ NewFilePath,
									case os:cmd(CompressCommand) of
										"" ->
											file:delete(OriginalFilePath);
										ErrorOutput ->
											NewFilePathFailed = binary_to_list(filename:join([DataDir, ?IMAGE_THUMBNAIL_DIR, Address, binary_to_list(TxId) ++ ""])),
											case file:read_file_info(NewFilePathFailed) of
												{ok, FileInfo} when FileInfo#file_info.type == regular ->
													ok = file:delete(NewFilePathFailed);
												{error, _} ->
													ok
											end,
											?LOG_INFO([{image_thumbnail_compress_to_storage_Compress_Command_Failed________, ErrorOutput}])
									end;
								{error, Reason} ->													
									?LOG_INFO([{image_thumbnail_compress_to_storage_____________write_original_file_failed, Reason}])
							end,
							ok = file:close(File);
						Error ->
							Error
					end				
			end,
			%% Begin to output			
			case file:read_file(NewFilePath) of
				{ok, FileContent} ->
					FileContent;
				{error, _Reason} ->
					Data
			end;
		false ->
			?LOG_INFO([{image_thumbnail_compress_to_storage_____________DataSize_is_too_small, DataSize}]),
			Data
	end.

pdf_office_thumbnail_png_to_storage(Data, Address, TxId) ->
	%% Begin to convert to png
	%% Step 1: copy the data as a file, the path is [ADDRESS]/[TX]
	{ok, Config} = application:get_env(chivesweave, config),
	DataDir = Config#config.data_dir,
	% Address = ar_util:encode(ar_wallet:to_address(TX#tx.owner, TX#tx.signature_type)),
	ImageThumbnailDir = binary_to_list(filename:join([DataDir, ?IMAGE_THUMBNAIL_DIR, Address])),
	filelib:ensure_dir(ImageThumbnailDir ++ "/"),
	OriginalFilePath = binary_to_list(filename:join([DataDir, ?IMAGE_THUMBNAIL_DIR, Address, TxId])),
	TargetDir = binary_to_list(filename:join([DataDir, ?IMAGE_THUMBNAIL_DIR, Address])),
	NewFilePath = binary_to_list(filename:join([DataDir, ?IMAGE_THUMBNAIL_DIR, Address, binary_to_list(TxId) ++ ".png"])),
	case file:read_file_info(NewFilePath) of
		{ok, _FileInfo} ->
			ok;
		_ ->
			%% First to copy data to file
			{ok, File} = file:open(OriginalFilePath, [write]),
			case file:write(File, Data) of
				ok ->
					?LOG_INFO([{image_thumbnail_compress_to_storage_____________write, OriginalFilePath}]),
					%% Not Exist, need to compress			
					% libreoffice --headless --invisible --convert-to png input.pdf						
					CompressCommand = "libreoffice --headless --invisible --convert-to png --outdir " ++ TargetDir ++ " " ++ OriginalFilePath,
					?LOG_INFO([{image_thumbnail_compress_to_storage_____________CompressCommand, CompressCommand}]),
					case os:cmd(CompressCommand) of
						"" ->
							file:delete(OriginalFilePath);
						ErrorOutput ->
							NewFilePathFailed = binary_to_list(filename:join([DataDir, ?IMAGE_THUMBNAIL_DIR, Address, binary_to_list(TxId) ++ ""])),
							case file:read_file_info(NewFilePathFailed) of
								{ok, FileInfo} when FileInfo#file_info.type == regular ->
									ok = file:delete(NewFilePathFailed);
								{error, _} ->
									ok
							end,
							?LOG_INFO([{image_thumbnail_compress_to_storage_Compress_Command_Failed________, ErrorOutput}])
					end;
				{error, Reason} ->													
					?LOG_INFO([{image_thumbnail_compress_to_storage_____________write_original_file_failed, Reason}])
			end,
			ok = file:close(File)
	end,
	%% Begin to output			
	case file:read_file(NewFilePath) of
		{ok, FileContent} ->
			FileContent;
		{error, _Reason} ->
			Data
	end.


office_format_convert_to_docx_storage(Data, Address, TxId) ->
	%% Begin to convert to png
	%% Step 1: copy the data as a file, the path is [ADDRESS]/[TX]
	{ok, Config} = application:get_env(chivesweave, config),
	DataDir = Config#config.data_dir,
	% Address = ar_util:encode(ar_wallet:to_address(TX#tx.owner, TX#tx.signature_type)),
	ImageThumbnailDir = binary_to_list(filename:join([DataDir, ?IMAGE_THUMBNAIL_DIR, Address])),
	filelib:ensure_dir(ImageThumbnailDir ++ "/"),
	OriginalFilePath = binary_to_list(filename:join([DataDir, ?IMAGE_THUMBNAIL_DIR, Address, TxId])),
	TargetDir = binary_to_list(filename:join([DataDir, ?IMAGE_THUMBNAIL_DIR, Address])),
	NewFilePath = binary_to_list(filename:join([DataDir, ?IMAGE_THUMBNAIL_DIR, Address, binary_to_list(TxId) ++ ".docx"])),
	case file:read_file_info(NewFilePath) of
		{ok, _FileInfo} ->
			ok;
		_ ->
			%% First to copy data to file
			{ok, File} = file:open(OriginalFilePath, [write]),
			case file:write(File, Data) of
				ok ->
					?LOG_INFO([{image_thumbnail_compress_to_storage_____________write, OriginalFilePath}]),
					%% Not Exist, need to compress			
					% libreoffice --headless --invisible --convert-to png input.pdf						
					CompressCommand = "libreoffice --headless --invisible --convert-to docx --outdir " ++ TargetDir ++ " " ++ OriginalFilePath,
					?LOG_INFO([{image_thumbnail_compress_to_storage_____________CompressCommand, CompressCommand}]),
					case os:cmd(CompressCommand) of
						"" ->
							file:delete(OriginalFilePath);
						ErrorOutput ->
							NewFilePathFailed = binary_to_list(filename:join([DataDir, ?IMAGE_THUMBNAIL_DIR, Address, binary_to_list(TxId) ++ ""])),
							case file:read_file_info(NewFilePathFailed) of
								{ok, FileInfo} when FileInfo#file_info.type == regular ->
									ok = file:delete(NewFilePathFailed);
								{error, _} ->
									ok
							end,
							?LOG_INFO([{image_thumbnail_compress_to_storage_Compress_Command_Failed________, ErrorOutput}])
					end;
				{error, Reason} ->													
					?LOG_INFO([{image_thumbnail_compress_to_storage_____________write_original_file_failed, Reason}])
			end,
			ok = file:close(File)
	end,
	%% Begin to output			
	case file:read_file(NewFilePath) of
		{ok, FileContent} ->
			FileContent;
		{error, _Reason} ->
			Data
	end.

office_format_convert_to_xlsx_storage(Data, Address, TxId) ->
	%% Begin to convert to png
	%% Step 1: copy the data as a file, the path is [ADDRESS]/[TX]
	{ok, Config} = application:get_env(chivesweave, config),
	DataDir = Config#config.data_dir,
	% Address = ar_util:encode(ar_wallet:to_address(TX#tx.owner, TX#tx.signature_type)),
	ImageThumbnailDir = binary_to_list(filename:join([DataDir, ?IMAGE_THUMBNAIL_DIR, Address])),
	filelib:ensure_dir(ImageThumbnailDir ++ "/"),
	OriginalFilePath = binary_to_list(filename:join([DataDir, ?IMAGE_THUMBNAIL_DIR, Address, TxId])),
	TargetDir = binary_to_list(filename:join([DataDir, ?IMAGE_THUMBNAIL_DIR, Address])),
	NewFilePath = binary_to_list(filename:join([DataDir, ?IMAGE_THUMBNAIL_DIR, Address, binary_to_list(TxId) ++ ".xlsx"])),
	case file:read_file_info(NewFilePath) of
		{ok, _FileInfo} ->
			ok;
		_ ->
			%% First to copy data to file
			{ok, File} = file:open(OriginalFilePath, [write]),
			case file:write(File, Data) of
				ok ->
					?LOG_INFO([{image_thumbnail_compress_to_storage_____________write, OriginalFilePath}]),
					%% Not Exist, need to compress			
					% libreoffice --headless --invisible --convert-to png input.pdf						
					CompressCommand = "libreoffice --headless --invisible --convert-to xlsx --outdir " ++ TargetDir ++ " " ++ OriginalFilePath,
					?LOG_INFO([{image_thumbnail_compress_to_storage_____________CompressCommand, CompressCommand}]),
					case os:cmd(CompressCommand) of
						"" ->
							file:delete(OriginalFilePath);
						ErrorOutput ->
							NewFilePathFailed = binary_to_list(filename:join([DataDir, ?IMAGE_THUMBNAIL_DIR, Address, binary_to_list(TxId) ++ ""])),
							case file:read_file_info(NewFilePathFailed) of
								{ok, FileInfo} when FileInfo#file_info.type == regular ->
									ok = file:delete(NewFilePathFailed);
								{error, _} ->
									ok
							end,
							?LOG_INFO([{image_thumbnail_compress_to_storage_Compress_Command_Failed________, ErrorOutput}])
					end;
				{error, Reason} ->													
					?LOG_INFO([{image_thumbnail_compress_to_storage_____________write_original_file_failed, Reason}])
			end,
			ok = file:close(File)
	end,
	%% Begin to output			
	case file:read_file(NewFilePath) of
		{ok, FileContent} ->
			FileContent;
		{error, _Reason} ->
			Data
	end.

office_format_convert_to_pdf_storage(Data, Address, TxId) ->
	%% Begin to convert to png
	%% Step 1: copy the data as a file, the path is [ADDRESS]/[TX]
	{ok, Config} = application:get_env(chivesweave, config),
	DataDir = Config#config.data_dir,
	% Address = ar_util:encode(ar_wallet:to_address(TX#tx.owner, TX#tx.signature_type)),
	ImageThumbnailDir = binary_to_list(filename:join([DataDir, ?IMAGE_THUMBNAIL_DIR, Address])),
	filelib:ensure_dir(ImageThumbnailDir ++ "/"),
	OriginalFilePath = binary_to_list(filename:join([DataDir, ?IMAGE_THUMBNAIL_DIR, Address, TxId])),
	TargetDir = binary_to_list(filename:join([DataDir, ?IMAGE_THUMBNAIL_DIR, Address])),
	NewFilePath = binary_to_list(filename:join([DataDir, ?IMAGE_THUMBNAIL_DIR, Address, binary_to_list(TxId) ++ ".pdf"])),
	case file:read_file_info(NewFilePath) of
		{ok, _FileInfo} ->
			ok;
		_ ->
			%% First to copy data to file
			{ok, File} = file:open(OriginalFilePath, [write]),
			case file:write(File, Data) of
				ok ->
					?LOG_INFO([{image_thumbnail_compress_to_storage_____________write, OriginalFilePath}]),
					%% Not Exist, need to compress			
					% libreoffice --headless --invisible --convert-to png input.pdf						
					CompressCommand = "libreoffice --headless --invisible --convert-to pdf --outdir " ++ TargetDir ++ " " ++ OriginalFilePath,
					?LOG_INFO([{image_thumbnail_compress_to_storage_____________CompressCommand, CompressCommand}]),
					case os:cmd(CompressCommand) of
						"" ->
							file:delete(OriginalFilePath);
						ErrorOutput ->
							NewFilePathFailed = binary_to_list(filename:join([DataDir, ?IMAGE_THUMBNAIL_DIR, Address, binary_to_list(TxId) ++ ""])),
							case file:read_file_info(NewFilePathFailed) of
								{ok, FileInfo} when FileInfo#file_info.type == regular ->
									ok = file:delete(NewFilePathFailed);
								{error, _} ->
									ok
							end,
							?LOG_INFO([{image_thumbnail_compress_to_storage_Compress_Command_Failed________, ErrorOutput}])
					end;
				{error, Reason} ->													
					?LOG_INFO([{image_thumbnail_compress_to_storage_____________write_original_file_failed, Reason}])
			end,
			ok = file:close(File)
	end,
	%% Begin to output			
	case file:read_file(NewFilePath) of
		{ok, FileContent} ->
			FileContent;
		{error, _Reason} ->
			Data
	end.

video_thumbnail_png_to_storage(Data, Address, TxId) ->
	%% Begin to convert to png
	%% Step 1: copy the data as a file, the path is [ADDRESS]/[TX]
	{ok, Config} = application:get_env(chivesweave, config),
	DataDir = Config#config.data_dir,
	% Address = ar_util:encode(ar_wallet:to_address(TX#tx.owner, TX#tx.signature_type)),
	ImageThumbnailDir = binary_to_list(filename:join([DataDir, ?IMAGE_THUMBNAIL_DIR, Address])),
	filelib:ensure_dir(ImageThumbnailDir ++ "/"),
	OriginalFilePath = binary_to_list(filename:join([DataDir, ?IMAGE_THUMBNAIL_DIR, Address, TxId])),
	NewFilePath = binary_to_list(filename:join([DataDir, ?IMAGE_THUMBNAIL_DIR, Address, binary_to_list(TxId) ++ ".png"])),
	case file:read_file_info(NewFilePath) of
		{ok, _FileInfo} ->
			ok;
		_ ->
			%% First to copy data to file
			case file:open(OriginalFilePath, [write]) of
			{ok, File} ->
				case file:write(File, Data) of
					ok ->
						?LOG_INFO([{video_thumbnail_compress_to_storage_____________write, OriginalFilePath}]),
						%% Not Exist, need to compress			
						% libreoffice --headless --invisible --convert-to png input.pdf						
						% CompressCommand = "libreoffice --headless --invisible --convert-to png --outdir " ++ TargetDir ++ " " ++ OriginalFilePath,
						CompressCommand = "ffmpeg -i " ++ OriginalFilePath ++ " -ss 00:00:05 -vframes 1 " ++ NewFilePath,
						% Command = "ffmpeg",
						% Args = ["-i", "/home/wex5/chivesweave/mainnet_data_dir/image_thumbnail/XrjRoNDx-WVhgK_xJfowYrTrzaFxabt7tGwjVpCf2yE/zCJBW4EM2izHNTdGTsi7rzuW0wpWB3-nPVheDxMQwJ4", "-ss", "00:00:05", "-vframes", "1", "/home/wex5/chivesweave/mainnet_data_dir/image_thumbnail/XrjRoNDx-WVhgK_xJfowYrTrzaFxabt7tGwjVpCf2yE/zCJBW4EM2izHNTdGTsi7rzuW0wpWB3-nPVheDxMQwJ4.png"],
						% Args = ["-i", OriginalFilePath, "-ss", "00:00:05", "-vframes", "1", NewFilePath],
						?LOG_INFO([{video_thumbnail_compress_to_storage_____________CompressCommand, CompressCommand}]),
						case os:cmd(CompressCommand) of
							"" ->
								file:delete(OriginalFilePath);
							ErrorOutput ->
								NewFilePathFailed = binary_to_list(filename:join([DataDir, ?IMAGE_THUMBNAIL_DIR, Address, binary_to_list(TxId) ++ ""])),
								case file:read_file_info(NewFilePathFailed) of
									{ok, FileInfo} when FileInfo#file_info.type == regular ->
										ok = file:delete(NewFilePathFailed);
									{error, _} ->
										ok
								end,
								?LOG_INFO([{video_thumbnail_compress_to_storage_Compress_Command_Failed________, ErrorOutput}])
						end;
					{error, Reason} ->													
						?LOG_INFO([{video_thumbnail_compress_to_storage_____________write_original_file_failed, Reason}])
				end,
				file:close(File);
			Error ->
				Error
			end
	end,
	%% Begin to output			
	case file:read_file(NewFilePath) of
		{ok, FileContent} ->
			FileContent;
		{error, _Reason} ->
			Data
	end.

update_reward_history(B) ->
	case B#block.height >= ar_fork:height_2_6() of
		true ->
			HashRate = ar_difficulty:get_hash_rate(B#block.diff),
			Addr = B#block.reward_addr,
			Bin = term_to_binary({Addr, HashRate, ar_pricing:reset_block_reward_by_height(B#block.height, B#block.reward)}),
			ar_kv:put(reward_history_db, B#block.indep_hash, Bin);
		false ->
			ok
	end.

write_full_block2(BShadow, TXs) ->
	case write_block(BShadow) of
		ok ->
			app_ipfs:maybe_ipfs_add_txs(TXs),
			ok;
		Error ->
			Error
	end.

read_block_from_file(Filename, Encoding) ->
	case read_file_raw(Filename) of
		{ok, Bin} ->
			case Encoding of
				json ->
					parse_block_json(Bin);
				binary ->
					parse_block_binary(Bin)
			end;
		{error, Reason} ->
			?LOG_WARNING([{event, error_reading_block},
					{error, io_lib:format("~p", [Reason])}]),
			unavailable
	end.

parse_block_json(JSON) ->
	case catch ar_serialize:json_decode(JSON) of
		{ok, JiffyStruct} ->
			case catch ar_serialize:json_struct_to_block(JiffyStruct) of
				B when is_record(B, block) ->
					B;
				Error ->
					?LOG_WARNING([{event, error_parsing_block_json},
							{error, io_lib:format("~p", [Error])}]),
					unavailable
			end;
		Error ->
			?LOG_WARNING([{event, error_parsing_block_json},
					{error, io_lib:format("~p", [Error])}]),
			unavailable
	end.

parse_block_binary(Bin) ->
	case catch ar_serialize:binary_to_block(Bin) of
		{ok, B} ->
			B;
		Error ->
			?LOG_WARNING([{event, error_parsing_block_bin},
					{error, io_lib:format("~p", [Error])}]),
			unavailable
	end.

filepath(PathComponents) ->
	{ok, Config} = application:get_env(chivesweave, config),
	to_string(filename:join([Config#config.data_dir | PathComponents])).

to_string(Bin) when is_binary(Bin) ->
	binary_to_list(Bin);
to_string(String) ->
	String.

%% @doc Ensure that all of the relevant storage directories exist.
ensure_directories(DataDir) ->
	%% Append "/" to every path so that filelib:ensure_dir/1 creates a directory
	%% if it does not exist.
	filelib:ensure_dir(filename:join(DataDir, ?TX_DIR) ++ "/"),
	filelib:ensure_dir(filename:join(DataDir, ?BLOCK_DIR) ++ "/"),
	filelib:ensure_dir(filename:join(DataDir, ?WALLET_LIST_DIR) ++ "/"),
	filelib:ensure_dir(filename:join(DataDir, ?HASH_LIST_DIR) ++ "/"),
	filelib:ensure_dir(filename:join(DataDir, ?IMAGE_THUMBNAIL_DIR) ++ "/"),
	filelib:ensure_dir(filename:join(DataDir, ?UNBUNDLE_DATA_DIR) ++ "/"),
	filelib:ensure_dir(filename:join(DataDir, ?STORAGE_MIGRATIONS_DIR) ++ "/"),
	filelib:ensure_dir(filename:join([DataDir, ?TX_DIR, "migrated_v1"]) ++ "/").

get_same_disk_storage_modules_total_size() ->
	{ok, Config} = application:get_env(chivesweave, config),
	DataDir = Config#config.data_dir,
	{ok, Info} = file:read_file_info(DataDir),
	Device = Info#file_info.major_device,
	get_same_disk_storage_modules_total_size(0, Config#config.storage_modules, DataDir,
			Device).

get_same_disk_storage_modules_total_size(TotalSize, [], _DataDir, _Device) ->
	TotalSize;
get_same_disk_storage_modules_total_size(TotalSize,
		[{Size, _Bucket, _Packing} = Module | StorageModules], DataDir, Device) ->
	Path = filename:join([DataDir, "storage_modules", ar_storage_module:id(Module)]),
	filelib:ensure_dir(Path ++ "/"),
	{ok, Info} = file:read_file_info(Path),
	TotalSize2 =
		case Info#file_info.major_device == Device of
			true ->
				TotalSize + Size;
			false ->
				TotalSize
		end,
	get_same_disk_storage_modules_total_size(TotalSize2, StorageModules, DataDir, Device).

tx_filepath(TX) ->
	filepath([?TX_DIR, tx_filename(TX)]).

tx_data_filepath(TX) when is_record(TX, tx) ->
	tx_data_filepath(TX#tx.id);
tx_data_filepath(ID) ->
	filepath([?TX_DIR, tx_data_filename(ID)]).

tx_filename(TX) when is_record(TX, tx) ->
	tx_filename(TX#tx.id);
tx_filename(TXID) when is_binary(TXID) ->
	iolist_to_binary([ar_util:encode(TXID), ".json"]).

tx_data_filename(TXID) ->
	iolist_to_binary([ar_util:encode(TXID), "_data.json"]).

wallet_list_filepath(Hash) when is_binary(Hash) ->
	filepath([?WALLET_LIST_DIR, iolist_to_binary([ar_util:encode(Hash), ".json"])]).

write_file_atomic(Filename, Data) ->
	SwapFilename = Filename ++ ".swp",
	case file:open(SwapFilename, [write, raw]) of
		{ok, F} ->
			case file:write(F, Data) of
				ok ->
					case file:close(F) of
						ok ->
							file:rename(SwapFilename, Filename);
						Error ->
							Error
					end;
				Error ->
					Error
			end;
		Error ->
			Error
	end.

write_term(Name, Term) ->
	{ok, Config} = application:get_env(chivesweave, config),
	DataDir = Config#config.data_dir,
	write_term(DataDir, Name, Term, override).

write_term(Dir, Name, Term) when is_atom(Name) ->
	write_term(Dir, atom_to_list(Name), Term, override);
write_term(Dir, Name, Term) ->
	write_term(Dir, Name, Term, override).

write_term(Dir, Name, Term, Override) ->
	Filepath = filename:join(Dir, Name),
	case Override == do_not_override andalso filelib:is_file(Filepath) of
		true ->
			ok;
		false ->
			case write_file_atomic(Filepath, term_to_binary(Term)) of
				ok ->
					ok;
				{error, Reason} = Error ->
					?LOG_ERROR([{event, failed_to_write_term}, {name, Name},
							{reason, Reason}]),
					Error
			end
	end.

read_term(Name) ->
	{ok, Config} = application:get_env(chivesweave, config),
	DataDir = Config#config.data_dir,
	read_term(DataDir, Name).

read_term(Dir, Name) when is_atom(Name) ->
	read_term(Dir, atom_to_list(Name));
read_term(Dir, Name) ->
	case file:read_file(filename:join(Dir, Name)) of
		{ok, Binary} ->
			{ok, binary_to_term(Binary)};
		{error, enoent} ->
			not_found;
		{error, Reason} = Error ->
			?LOG_ERROR([{event, failed_to_read_term}, {name, Name}, {reason, Reason}]),
			Error
	end.

delete_term(Name) ->
	{ok, Config} = application:get_env(chivesweave, config),
	DataDir = Config#config.data_dir,
	file:delete(filename:join(DataDir, atom_to_list(Name))).

store_account_tree_update(Height, RootHash, Map) ->
	?LOG_INFO([{event, storing_account_tree_update}, {updated_key_count, map_size(Map)},
			{height, Height}, {root_hash, ar_util:encode(RootHash)}]),
	maps:map(
		fun({H, Prefix} = Key, Value) ->
			Prefix2 = case Prefix of root -> <<>>; _ -> Prefix end,
			DBKey = << H/binary, Prefix2/binary >>,
			case ar_kv:get(account_tree_db, DBKey) of
				not_found ->
					case ar_kv:put(account_tree_db, DBKey, term_to_binary(Value)) of
						ok ->
							ok;
						{error, Reason} ->
							?LOG_ERROR([{event, failed_to_store_account_tree_key},
									{key_hash, ar_util:encode(element(1, Key))},
									{key_prefix, case element(2, Key) of root -> root;
											Prefix -> ar_util:encode(Prefix) end},
									{height, Height},
									{root_hash, ar_util:encode(RootHash)},
									{reason, io_lib:format("~p", [Reason])}])
					end;
				{ok, _} ->
					ok;
				{error, Reason} ->
					?LOG_ERROR([{event, failed_to_read_account_tree_key},
							{key_hash, ar_util:encode(element(1, Key))},
							{key_prefix, case element(2, Key) of root -> root;
									Prefix -> ar_util:encode(Prefix) end},
							{height, Height},
							{root_hash, ar_util:encode(RootHash)},
							{reason, io_lib:format("~p", [Reason])}])
			end
		end,
		Map
	),
	?LOG_INFO([{event, stored_account_tree}]).

%% @doc Ignore the prefix when querying a key since the prefix might depend on the order of
%% insertions and is only used to optimize certain lookups.
get_account_tree_value(Key, Prefix) ->
	ar_kv:get_prev(account_tree_db, << Key/binary, Prefix/binary >>).
	% does not work:
	%ar_kv:get_next(account_tree_db, << Key/binary, Prefix/binary >>).
	% works:
	%<< N:(48 * 8) >> = Key,
	%Key2 = << (N + 1):(48 * 8) >>,
	%ar_kv:get_prev(account_tree_db, Key2).

%%%===================================================================
%%% Tests
%%%===================================================================

%% @doc Test block storage.
store_and_retrieve_block_test_() ->
	{timeout, 60, fun test_store_and_retrieve_block/0}.

test_store_and_retrieve_block() ->
	[B0] = ar_weave:init([]),
	ar_test_node:start(B0),
	TXIDs = [TX#tx.id || TX <- B0#block.txs],
	FetchedB0 = read_block(B0#block.indep_hash),
	FetchedB01 = FetchedB0#block{ txs = [tx_id(TX) || TX <- FetchedB0#block.txs] },
	FetchedB02 = read_block(B0#block.height, [{B0#block.indep_hash, B0#block.weave_size,
			B0#block.tx_root}]),
	FetchedB03 = FetchedB02#block{ txs = [tx_id(TX) || TX <- FetchedB02#block.txs] },
	?assertEqual(B0#block{ size_tagged_txs = unset, txs = TXIDs, reward_history = [],
			block_time_history = [], account_tree = undefined }, FetchedB01),
	?assertEqual(B0#block{ size_tagged_txs = unset, txs = TXIDs, reward_history = [],
			block_time_history = [], account_tree = undefined }, FetchedB03),
	ar_node:mine(),
	ar_test_node:wait_until_height(1),
	ar_node:mine(),
	BI1 = ar_test_node:wait_until_height(2),
	[{_, BlockCount}] = ets:lookup(ar_header_sync, synced_blocks),
	ar_util:do_until(
		fun() ->
			3 == BlockCount
		end,
		100,
		2000
	),
	BH1 = element(1, hd(BI1)),
	?assertMatch(#block{ height = 2, indep_hash = BH1 }, read_block(BH1)),
	?assertMatch(#block{ height = 2, indep_hash = BH1 }, read_block(2, BI1)).

tx_id(#tx{ id = TXID }) ->
	TXID;
tx_id(TXID) ->
	TXID.

store_and_retrieve_wallet_list_test_() ->
	[
		{timeout, 20, fun test_store_and_retrieve_wallet_list/0},
		{timeout, 240, fun test_store_and_retrieve_wallet_list_permutations/0}
	].

test_store_and_retrieve_wallet_list() ->
	[B0] = ar_weave:init(),
	[TX] = B0#block.txs,
	Addr = ar_wallet:to_address(TX#tx.owner, {?RSA_SIGN_ALG, 65537}),
	write_block(B0),
	TXID = TX#tx.id,
	ExpectedWL = ar_patricia_tree:from_proplist([{Addr, {0, TXID}}]),
	WalletListHash = write_wallet_list(0, ExpectedWL),
	{ok, ActualWL} = read_wallet_list(WalletListHash),
	assert_wallet_trees_equal(ExpectedWL, ActualWL),
	Addr2 = binary:part(Addr, 0, 16),
	TXID2 = crypto:strong_rand_bytes(32),
	ExpectedWL2 = ar_patricia_tree:from_proplist([{Addr, {0, TXID}}, {Addr2, {0, TXID2}}]),
	WalletListHash2 = write_wallet_list(0, ExpectedWL2),
	{ok, ActualWL2} = read_wallet_list(WalletListHash2),
	?assertEqual({0, TXID}, read_account(Addr, WalletListHash2)),
	?assertEqual({0, TXID2}, read_account(Addr2, WalletListHash2)),
	assert_wallet_trees_equal(ExpectedWL2, ActualWL2),
	{WalletListHash, ActualWL3, _UpdateMap} = ar_block:hash_wallet_list(ActualWL),
	Addr3 = << (binary:part(Addr, 0, 3))/binary, (crypto:strong_rand_bytes(29))/binary >>,
	TXID3 = crypto:strong_rand_bytes(32),
	TXID4 = crypto:strong_rand_bytes(32),
	ActualWL4 = ar_patricia_tree:insert(Addr3, {100, TXID3},
			ar_patricia_tree:insert(Addr2, {0, TXID4}, ActualWL3)),
	{WalletListHash3, ActualWL5, UpdateMap2} = ar_block:hash_wallet_list(ActualWL4),
	store_account_tree_update(1, WalletListHash3, UpdateMap2),
	?assertEqual({100, TXID3}, read_account(Addr3, WalletListHash3)),
	?assertEqual({0, TXID4}, read_account(Addr2, WalletListHash3)),
	?assertEqual({0, TXID}, read_account(Addr, WalletListHash3)),
	{ok, ActualWL6} = read_wallet_list(WalletListHash3),
	assert_wallet_trees_equal(ActualWL5, ActualWL6).

test_store_and_retrieve_wallet_list_permutations() ->
	lists:foreach(
		fun(Permutation) ->
			store_and_retrieve_wallet_list(Permutation)
		end,
		permutations([ <<"a">>, <<"aa">>, <<"ab">>, <<"bb">>, <<"b">>, <<"aaa">> ])),
	lists:foreach(
		fun(Permutation) ->
			store_and_retrieve_wallet_list(Permutation)
		end,
		permutations([ <<"a">>, <<"aa">>, <<"aaa">>, <<"aaaa">>, <<"aaaaa">> ])),
	store_and_retrieve_wallet_list([ <<"a">>, <<"aa">>, <<"ab">>, <<"b">> ]),
	store_and_retrieve_wallet_list([ <<"aa">>, <<"a">>, <<"ab">> ]),
	store_and_retrieve_wallet_list([ <<"aaa">>, <<"bbb">>, <<"aab">>, <<"ab">>, <<"a">> ]),
	store_and_retrieve_wallet_list([
		<<"aaaa">>, <<"aaab">>, <<"aaac">>,
		<<"aaa">>, <<"aab">>, <<"aac">>,
		<<"aa">>, <<"ab">>, <<"ac">>,
		<<"a">>, <<"b">>, <<"c">>
	]),
	store_and_retrieve_wallet_list([
		<<"a">>, <<"b">>, <<"c">>,
		<<"aa">>, <<"ab">>, <<"ac">>,
		<<"aaa">>, <<"aab">>, <<"aac">>,
		<<"aaaa">>, <<"aaab">>, <<"aaac">>,
		<<"a">>, <<"b">>, <<"c">>,
		<<"aa">>, <<"ab">>, <<"ac">>,
		<<"aaa">>, <<"aab">>, <<"aac">>,
		<<"aaaa">>, <<"aaab">>, <<"aaac">>
	]),
	store_and_retrieve_wallet_list([
		<<"aaaa">>, <<"aaa">>, <<"aa">>, <<"a">>,
		<<"aaab">>, <<"aab">>, <<"ab">>, <<"b">>,
		<<"aaac">>, <<"aac">>, <<"ac">>, <<"c">>,
		<<"aaaa">>, <<"aaa">>, <<"aa">>, <<"a">>,
		<<"aaab">>, <<"aab">>, <<"ab">>, <<"b">>,
		<<"aaac">>, <<"aac">>, <<"ac">>, <<"c">>
	]),
	store_and_retrieve_wallet_list([
		<<"aaaa">>, <<"aaab">>, <<"aaac">>,
		<<"a">>, <<"aa">>, <<"aaa">>,
		<<"aaaa">>, <<"aaab">>, <<"aaac">>
	]),
	ok.

store_and_retrieve_wallet_list(Keys) ->
	MinBinary = <<>>,
	MaxBinary = << <<1:1>> || _ <- lists:seq(1, 512) >>,
	ar_kv:delete_range(account_tree_db, MinBinary, MaxBinary),
	store_and_retrieve_wallet_list(Keys, ar_patricia_tree:new(), maps:new(), false).

store_and_retrieve_wallet_list([], Tree, InsertedKeys, IsUpdate) ->
	store_and_retrieve_wallet_list2(Tree, InsertedKeys, IsUpdate);
store_and_retrieve_wallet_list([Key | Keys], Tree, InsertedKeys, IsUpdate) ->
	TXID = crypto:strong_rand_bytes(32),
	Balance = rand:uniform(1000000000),
	Tree2 = ar_patricia_tree:insert(Key, {Balance, TXID}, Tree),
	InsertedKeys2 = maps:put(Key, {Balance, TXID}, InsertedKeys),
	case rand:uniform(2) of
		1 ->
			Tree3 = store_and_retrieve_wallet_list2(Tree2, InsertedKeys2, IsUpdate),
			store_and_retrieve_wallet_list(Keys, Tree3, InsertedKeys2, true);
		_ ->
			store_and_retrieve_wallet_list(Keys, Tree2, InsertedKeys2, IsUpdate)
	end.

store_and_retrieve_wallet_list2(Tree, InsertedKeys, IsUpdate) ->
	{WalletListHash, Tree2} =
		case IsUpdate of
			false ->
				{write_wallet_list(0, Tree), Tree};
			_ ->
				{R, T, Map} = ar_block:hash_wallet_list(Tree),
				store_account_tree_update(0, R, Map),
				{R, T}
		end,
	{ok, ActualTree} = read_wallet_list(WalletListHash),
	maps:foreach(
		fun(Key, {Balance, TXID}) ->
			?assertEqual({Balance, TXID}, read_account(Key, WalletListHash))
		end,
		InsertedKeys
	),
	assert_wallet_trees_equal(Tree, ActualTree),
	assert_wallet_trees_equal(Tree2, ActualTree),
	Tree2.

%% From: https://www.erlang.org/doc/programming_examples/list_comprehensions.html#permutations
permutations([]) -> [[]];
permutations(L)  -> [[H|T] || H <- L, T <- permutations(L--[H])].

assert_wallet_trees_equal(Expected, Actual) ->
	?assertEqual(
		ar_patricia_tree:foldr(fun(K, V, Acc) -> [{K, V} | Acc] end, [], Expected),
		ar_patricia_tree:foldr(fun(K, V, Acc) -> [{K, V} | Acc] end, [], Actual)
	).

read_wallet_list_chunks_test() ->
	TestCases = [
		[random_wallet()], % < chunk size
		[random_wallet() || _ <- lists:seq(1, ?WALLET_LIST_CHUNK_SIZE)], % == chunk size
		[random_wallet() || _ <- lists:seq(1, ?WALLET_LIST_CHUNK_SIZE + 1)], % > chunk size
		[random_wallet() || _ <- lists:seq(1, 10 * ?WALLET_LIST_CHUNK_SIZE)],
		[random_wallet() || _ <- lists:seq(1, 10 * ?WALLET_LIST_CHUNK_SIZE + 1)]
	],
	lists:foreach(
		fun(TestCase) ->
			Tree = ar_patricia_tree:from_proplist(TestCase),
			RootHash = write_wallet_list(0, Tree),
			{ok, ReadTree} = read_wallet_list(RootHash),
			assert_wallet_trees_equal(Tree, ReadTree)
		end,
		TestCases
	).

random_wallet() ->
	{crypto:strong_rand_bytes(32), {rand:uniform(1000000000), crypto:strong_rand_bytes(32)}}.
