-module(ar_http_iface_middleware).

-behaviour(cowboy_middleware).

-export([execute/2, read_body_chunk/4]).

-include_lib("chivesweave/include/ar.hrl").
-include_lib("chivesweave/include/ar_config.hrl").
-include_lib("chivesweave/include/ar_pricing.hrl").
-include_lib("chivesweave/include/ar_data_sync.hrl").
-include_lib("chivesweave/include/ar_data_discovery.hrl").
-include_lib("chivesweave/include/ar_consensus.hrl").

-define(HANDLER_TIMEOUT, 55000).

-define(MAX_SERIALIZED_RECENT_HASH_LIST_DIFF, 2400). % 50 * 48.
-define(MAX_SERIALIZED_MISSING_TX_INDICES, 125). % Every byte encodes 8 positions.

-define(MAX_BLOCK_INDEX_RANGE_SIZE, 10000).

%%%===================================================================
%%% Cowboy handler callbacks.
%%%===================================================================

%% To allow prometheus_cowboy2_handler to be run when the
%% cowboy_router middleware matches on the /metrics route, this
%% middleware runs between the cowboy_router and cowboy_handler
%% middlewares. It uses the `handler` env value set by cowboy_router
%% to determine whether or not it should run, otherwise it lets
%% the cowboy_handler middleware run prometheus_cowboy2_handler.
execute(Req, #{ handler := ar_http_iface_handler } = Env) ->
	Pid = self(),
	Req1 = with_arql_semaphore_req_field(Req, Env),
	HandlerPid = spawn_link(fun() ->
		Pid ! {handled, handle(Req1, Pid)}
	end),
	{ok, TimeoutRef} = timer:send_after(
		?HANDLER_TIMEOUT,
		{timeout, HandlerPid, Req}
	),
	loop(TimeoutRef);
execute(Req, Env) ->
	{ok, Req, Env}.

%%%===================================================================
%%% Private functions.
%%%===================================================================

with_arql_semaphore_req_field(Req, #{ arql_semaphore := Name }) ->
	Req#{ '_ar_http_iface_middleware_arql_semaphore' => Name }.

%% @doc In order to be able to have a handler-side timeout, we need to
%% handle the request asynchronously. However, cowboy doesn't allow
%% reading the request's body from a process other than its handler's.
%% This following loop function allows us to work around this
%% limitation. (see https://github.com/ninenines/cowboy/issues/1374)
%% @end
loop(TimeoutRef) ->
	receive
		{handled, {Status, Headers, Body, HandledReq}} ->
			timer:cancel(TimeoutRef),
			CowboyStatus = handle_custom_codes(Status),
			RepliedReq = cowboy_req:reply(CowboyStatus, Headers, Body, HandledReq),
			{stop, RepliedReq};
		{read_complete_body, From, Req, SizeLimit} ->
			case catch ar_http_req:body(Req, SizeLimit) of
				Term ->
					From ! {read_complete_body, Term}
			end,
			loop(TimeoutRef);
		{read_body_chunk, From, Req, Size, Timeout} ->
			case catch ar_http_req:read_body_chunk(Req, Size, Timeout) of
				Term ->
					From ! {read_body_chunk, Term}
			end,
			loop(TimeoutRef);
		{timeout, HandlerPid, InitialReq} ->
			unlink(HandlerPid),
			exit(HandlerPid, handler_timeout),
			?LOG_WARNING([{event, handler_timeout},
					{method, cowboy_req:method(InitialReq)},
					{path, cowboy_req:path(InitialReq)}]),
			RepliedReq = cowboy_req:reply(500, #{}, <<"Handler timeout">>, InitialReq),
			{stop, RepliedReq}
	end.

handle(Req, Pid) ->
	Peer = ar_http_util:arweave_peer(Req),
	handle(Peer, Req, Pid).

handle(Peer, Req, Pid) ->
	Method = cowboy_req:method(Req),
	SplitPath = ar_http_iface_server:split_path(cowboy_req:path(Req)),
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(http_logging, Config#config.enable) of
		true ->
			?LOG_INFO([
				{event, http_request},
				{method, Method},
				{path, SplitPath},
				{peer, ar_util:format_peer(Peer)}
			]);
		_ ->
			do_nothing
	end,
	%% We break the P3 handling into two steps:
	%% 1. Before the request is processed, ar_p3:allow_request checks whether this is a
	%%    P3 request and if so it validates the header and applies the charge
	%% 2. After the request is processed, handle_p3_response checks if the requet failed,
	%%	  if so it reverses the charge
	%%
	%% This two-step process is needed to ensure clients aren't charged for failed requests.
	Response2 = case ar_p3:allow_request(Req) of
		{true, P3Data} ->
			Response = handle4(Method, SplitPath, Req, Pid),
			handle_p3_error(Response, P3Data),
			Response;
		{false, P3Status} ->
			p3_error_response(P3Status, Req)
	end,
	add_cors_headers(Req, Response2).

add_cors_headers(Req, Response) ->
	case Response of
		{Status, Hdrs, Body, HandledReq} ->
			{Status, maps:merge(?CORS_HEADERS, Hdrs), Body, HandledReq};
		{Status, Body, HandledReq} ->
			{Status, ?CORS_HEADERS, Body, HandledReq};
		{error, timeout} ->
			{503, ?CORS_HEADERS, jiffy:encode(#{ error => timeout }), Req}
	end.

handle_p3_error(Response, P3Data) ->
	Status = element(1, Response),
	case {Status, P3Data} of
		{_, not_p3_service} ->
			do_nothing;
		_ when Status >= 400 ->
			{ok, _} = ar_p3:reverse_charge(P3Data);
		_ ->
			do_nothing
	end,
	ok.
	
p3_error_response(P3Status, Req) ->
	Status = case P3Status of
		invalid_header ->
			400;
		insufficient_funds ->
			402;
		stale_mod_seq ->
			428;
		_ ->
			400
	end,
	{Status, jiffy:encode(#{ error => P3Status }), Req}.

-ifdef(TESTNET).
handle4(<<"POST">>, [<<"mine">>], Req, _Pid) ->
	ar_node:mine(),
	{200, #{}, <<>>, Req};

handle4(<<"GET">>, [<<"tx">>, <<"ready_for_mining">>], Req, _Pid) ->
	{200, #{},
			ar_serialize:jsonify(
				lists:map(
					fun ar_util:encode/1,
					ar_node:get_ready_for_mining_txs()
				)
			),
	Req};

handle4(Method, SplitPath, Req, Pid) ->
	handle(Method, SplitPath, Req, Pid).
-else.
handle4(Method, SplitPath, Req, Pid) ->
	handle(Method, SplitPath, Req, Pid).
-endif.

%% Return network information from a given node.
%% GET request to endpoint /info.
handle(<<"GET">>, [], Req, _Pid) ->
	return_info(Req);

handle(<<"GET">>, [<<"info">>], Req, _Pid) ->
	return_info(Req);

%% Some load balancers use 'HEAD's rather than 'GET's to tell if a node
%% is alive. Appease them.
handle(<<"HEAD">>, [], Req, _Pid) ->
	{200, #{}, <<>>, Req};
handle(<<"HEAD">>, [<<"info">>], Req, _Pid) ->
	{200, #{}, <<>>, Req};

%% Return permissive CORS headers for all endpoints.
handle(<<"OPTIONS">>, [<<"chunk">>], Req, _Pid) ->
	{200, #{<<"access-control-allow-methods">> => <<"GET, POST">>,
			<<"access-control-allow-headers">> => <<"Content-Type">>}, <<"OK">>, Req};
handle(<<"OPTIONS">>, [<<"block">>], Req, _Pid) ->
	{200, #{<<"access-control-allow-methods">> => <<"GET, POST">>,
			<<"access-control-allow-headers">> => <<"Content-Type">>}, <<"OK">>, Req};
handle(<<"OPTIONS">>, [<<"tx">>], Req, _Pid) ->
	{200, #{<<"access-control-allow-methods">> => <<"GET, POST">>,
			<<"access-control-allow-headers">> => <<"Content-Type">>}, <<"OK">>, Req};
handle(<<"OPTIONS">>, [<<"peer">> | _], Req, _Pid) ->
	{200, #{<<"access-control-allow-methods">> => <<"GET, POST">>,
			<<"access-control-allow-headers">> => <<"Content-Type">>}, <<"OK">>, Req};
handle(<<"OPTIONS">>, [<<"arql">>], Req, _Pid) ->
	{200, #{<<"access-control-allow-methods">> => <<"GET, POST">>,
			<<"access-control-allow-headers">> => <<"Content-Type">>}, <<"OK">>, Req};
handle(<<"OPTIONS">>, _, Req, _Pid) ->
	{200, #{<<"access-control-allow-methods">> => <<"GET">>}, <<"OK">>, Req};

%% Return the current universal time in seconds.
handle(<<"GET">>, [<<"time">>], Req, _Pid) ->
	{200, #{}, integer_to_binary(os:system_time(second)), Req};

%% Return all mempool transactions.
%% GET request to endpoint /tx/pending.
handle(<<"GET">>, [<<"tx">>, <<"pending">>], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			{200, #{},
					ar_serialize:jsonify(
						%% Should encode
						lists:map(
							fun ar_util:encode/1,
							ar_mempool:get_all_txids()
						)
					),
			Req}
	end;

%% Return all mempool transaction records.
%% GET request to endpoint /tx/pending/record.
handle(<<"GET">>, [<<"tx">>, <<"pending">>, <<"record">>], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			{200, #{},
					ar_serialize:jsonify(
						%% Should encode
						ar_storage:get_mempool_tx_txs_records()
					),
			Req}
	end;

%% Return outgoing transaction priority queue.
%% GET request to endpoint /queue.
%% @deprecated
handle(<<"GET">>, [<<"queue">>], Req, _Pid) ->
	{200, #{}, <<"[]">>, Req};

%% Return additional information about the transaction with the given identifier (hash).
%% GET request to endpoint /tx/{hash}/status.
handle(<<"GET">>, [<<"tx">>, Hash, <<"status">>], Req, _Pid) ->
	ar_semaphore:acquire(arql_semaphore(Req), 5000),
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			handle_get_tx_status(Hash, Req)
	end;

%% Return a txs list in a bundle if tx is a bundle, and will unbundle txs and storage in disk.
%% GET request to endpoint /tx/{hash}/status.
handle(<<"GET">>, [<<"tx">>, Hash, <<"unbundle">>, PageId, PageRecords], Req, _Pid) ->
	ar_semaphore:acquire(arql_semaphore(Req), 5000),
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			handle_get_tx_unbundle(Hash, Req, PageId, PageRecords)
	end;

%% Return a JSON-encoded transaction.
%% GET request to endpoint /tx/{hash}.
handle(<<"GET">>, [<<"tx">>, Hash], Req, _Pid) ->
	handle_get_tx(Hash, Req, json);

%% Return a binary-encoded transaction.
%% GET request to endpoint /tx2/{hash}.
handle(<<"GET">>, [<<"tx2">>, Hash], Req, _Pid) ->
	handle_get_tx(Hash, Req, binary);

%% Return a possibly unconfirmed JSON-encoded transaction.
%% GET request to endpoint /unconfirmed_tx/{hash}.
handle(<<"GET">>, [<<"unconfirmed_tx">>, Hash], Req, _Pid) ->
	handle_get_unconfirmed_tx(Hash, Req, json);

%% Return a possibly unconfirmed binary-encoded transaction.
%% GET request to endpoint /unconfirmed_tx2/{hash}.
handle(<<"GET">>, [<<"unconfirmed_tx2">>, Hash], Req, _Pid) ->
	handle_get_unconfirmed_tx(Hash, Req, binary);

%% Return the transaction IDs of all txs where the tags in post match the given set
%% of key value pairs. POST request to endpoint /arql with body of request being a logical
%% expression valid in ar_parser.
%%
%% Example logical expression.
%%	{
%%		op:		{ and | or | equals }
%%		expr1:	{ string | logical expression }
%%		expr2:	{ string | logical expression }
%%	}
handle(<<"POST">>, [<<"arql">>], Req, Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_arql, Config#config.enable) of
		true ->
			ar_semaphore:acquire(arql_semaphore(Req), 5000),
			case ar_node:is_joined() of
				false ->
					not_joined(Req);
				true ->
					case read_complete_body(Req, Pid) of
						{ok, QueryJSON, Req2} ->
							case ar_serialize:json_struct_to_query(QueryJSON) of
								{ok, Query} ->
									case catch ar_arql_db:eval_legacy_arql(Query) of
										EncodedTXIDs when is_list(EncodedTXIDs) ->
											Body = ar_serialize:jsonify(EncodedTXIDs),
											{200, #{}, Body, Req2};
										bad_query ->
											{400, #{}, <<"Invalid query.">>, Req2};
										sqlite_parser_stack_overflow ->
											{400, #{},
												<<"The query nesting depth is too big.">>, Req2};
										{'EXIT', {timeout,
												{gen_server, call, [ar_arql_db, _]}}} ->
											{503, #{}, <<"ArQL unavailable.">>, Req2}
									end;
								{error, _} ->
									{400, #{}, <<"Invalid ARQL query.">>, Req2}
							end;
						{error, body_size_too_large} ->
							{413, #{}, <<"Payload too large">>, Req};
						{error, timeout} ->
							{500, #{}, <<"Handler timeout">>, Req}
					end
			end;
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

%% Return the data field of the transaction specified via the transaction ID (hash)
%% served as HTML.
%% GET request to endpoint /tx/{hash}/data.html
handle(<<"GET">>, [<<"tx">>, Hash, << "data.", _/binary >>], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_html_data, Config#config.disable) of
		true ->
			{421, #{}, <<"Serving HTML data is disabled on this node.">>, Req};
		_ ->
			case ar_util:safe_decode(Hash) of
				{error, invalid} ->
					{400, #{}, <<"Invalid hash.">>, Req};
				{ok, ID} ->
					case ar_storage:read_tx(ID) of
						unavailable ->
							DataDir = Config#config.data_dir,							
							case ar_kv:get(xwe_storage_txid_in_bundle, ar_util:encode(ID))  of
								not_found ->
									{404, #{}, sendfile("data/not_found.html"), Req};
								{ok, BundleTxBinary} ->
									BundleTx = binary_to_term(BundleTxBinary),	
									% ?LOG_INFO([{handle_get_tx_unbundle_____________BundleTx, BundleTx}]),
									ContentType = maps:get(<<"type">>, maps:get(<<"data">>, BundleTx, undefined), undefined),
									Address = maps:get(<<"address">>, maps:get(<<"owner">>, BundleTx, undefined), undefined),
									FileName = binary_to_list(filename:join([DataDir, ?UNBUNDLE_DATA_DIR, Address, ar_util:encode(ID) ])),
									% ?LOG_INFO([{image_thumbnail_compress_to_storage_____________FileName, FileName}]),
									case file:read_file(FileName) of
										{ok, Content} ->
											{200, #{ <<"content-type">> => ContentType,  <<"Cache-Control">> => <<"max-age=604800">> }, Content, Req};
										_ ->
											{404, #{}, sendfile("data/not_found.html"), Req}
									end
							end;
						#tx{} = TX ->
							serve_tx_html_data(Req, TX)
					end
			end
	end;

handle(<<"GET">>, [<<"sync_buckets">>], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			ok = ar_semaphore:acquire(get_sync_record, infinity),
			case ar_global_sync_record:get_serialized_sync_buckets() of
				{ok, Binary} ->
					{200, #{}, Binary, Req};
				{error, not_initialized} ->
					{500, #{}, jiffy:encode(#{ error => not_initialized }), Req};
				{error, timeout} ->
					{503, #{}, jiffy:encode(#{ error => timeout }), Req}
			end
	end;

handle(<<"GET">>, [<<"data_sync_record">>], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			Format =
				case cowboy_req:header(<<"content-type">>, Req) of
					<<"application/json">> ->
						json;
					_ ->
						etf
			end,
			ok = ar_semaphore:acquire(get_sync_record, infinity),
			Options = #{ format => Format, random_subset => true },
			case ar_global_sync_record:get_serialized_sync_record(Options) of
				{ok, Binary} ->
					{200, #{}, Binary, Req};
				{error, timeout} ->
					{503, #{}, jiffy:encode(#{ error => timeout }), Req}
			end
	end;

handle(<<"GET">>, [<<"data_sync_record">>, EncodedStart, EncodedLimit], Req, _Pid) ->
	case catch binary_to_integer(EncodedStart) of
		{'EXIT', _} ->
			{400, #{}, jiffy:encode(#{ error => invalid_start_encoding }), Req};
		Start ->
			case catch binary_to_integer(EncodedLimit) of
				{'EXIT', _} ->
					{400, #{}, jiffy:encode(#{ error => invalid_limit_encoding }), Req};
				Limit ->
					case Limit > ?MAX_SHARED_SYNCED_INTERVALS_COUNT of
						true ->
							{400, #{}, jiffy:encode(#{ error => limit_too_big }), Req};
						false ->
							ok = ar_semaphore:acquire(get_sync_record, infinity),
							handle_get_data_sync_record(Start, Limit, Req)
					end
			end
	end;

handle(<<"GET">>, [<<"chunk">>, OffsetBinary], Req, _Pid) ->
	handle_get_chunk(OffsetBinary, Req, json);

handle(<<"GET">>, [<<"chunk2">>, OffsetBinary], Req, _Pid) ->
	handle_get_chunk(OffsetBinary, Req, binary);

handle(<<"GET">>, [<<"tx">>, EncodedID, <<"offset">>], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			case ar_util:safe_decode(EncodedID) of
				{error, invalid} ->
					{400, #{}, jiffy:encode(#{ error => invalid_address }), Req};
				{ok, ID} ->
					case ar_data_sync:get_tx_offset(ID) of
						{ok, {Offset, Size}} ->
							ResponseBody = jiffy:encode(#{
								offset => integer_to_binary(Offset),
								size => integer_to_binary(Size)
							}),
							{200, #{}, ResponseBody, Req};
						{error, not_found} ->
							{404, #{}, <<>>, Req};
						{error, failed_to_read_offset} ->
							{500, #{}, <<>>, Req};
						{error, timeout} ->
							{503, #{}, jiffy:encode(#{ error => timeout }), Req}
					end
			end
	end;

handle(<<"POST">>, [<<"chunk">>], Req, Pid) ->
	Joined =
		case ar_node:is_joined() of
			false ->
				not_joined(Req);
			true ->
				ok
		end,
	Semaphore =
		case Joined of
			ok ->
				case ar_semaphore:acquire(post_chunk, 5000) of
					ok ->
						ok;
					{error, timeout} ->
						{503, #{}, jiffy:encode(#{ error => timeout }), Req}
				end;
			Reply ->
				Reply
		end,
	DataRootKnown =
		case Semaphore of
			ok ->
				case get_data_root_from_headers(Req) of
					not_set ->
						ok;
					{ok, {DataRoot, DataSize}} ->
						case ar_data_sync:has_data_root(DataRoot, DataSize) of
							true ->
								ok;
							false ->
								{400, #{}, jiffy:encode(#{ error => data_root_not_found }),
										Req}
						end
				end;
			Reply2 ->
				Reply2
		end,
	ParseChunk =
		case DataRootKnown of
			ok ->
				parse_chunk(Req, Pid);
			Reply3 ->
				Reply3
		end,
	case ParseChunk of
		{ok, {Proof, Req2}} ->
			handle_post_chunk(Proof, Req2);
		Reply4 ->
			Reply4
	end;

%% Accept an announcement of a block. Reply 412 (no previous block),
%% 200 (optionally specifying missing transactions and chunk in the response)
%% or 208 (already processing the block).
handle(<<"POST">>, [<<"block_announcement">>], Req, Pid) ->
	case read_complete_body(Req, Pid) of
		{ok, Body, Req2} ->
			case catch ar_serialize:binary_to_block_announcement(Body) of
				{ok, Announcement} ->
					handle_block_announcement(Announcement, Req2);
				{'EXIT', _Reason} ->
					{400, #{}, <<>>, Req2};
				{error, _Reason} ->
					{400, #{}, <<>>, Req2}
			end;
		{error, body_size_too_large} ->
			{400, #{}, <<>>, Req};
		{error, timeout} ->
			{503, #{}, jiffy:encode(#{ error => timeout }), Req}
	end;

%% Accept a JSON-encoded block with Base64Url encoded fields.
handle(<<"POST">>, [<<"block">>], Req, Pid) ->
	post_block(request, {Req, Pid, json}, erlang:timestamp());

%% Accept a binary-encoded block.
handle(<<"POST">>, [<<"block2">>], Req, Pid) ->
	erlang:put(post_block2, true),
	post_block(request, {Req, Pid, binary}, erlang:timestamp());

%% Generate a wallet and receive a secret key identifying it.
%% Requires internal_api_secret startup option to be set.
%% WARNING: only use it if you really really know what you are doing.
handle(<<"POST">>, [<<"wallet">>], Req, _Pid) ->
	case check_internal_api_secret(Req) of
		pass ->
			WalletAccessCode = ar_util:encode(crypto:strong_rand_bytes(32)),
			{_, Pub} = ar_wallet:new_keyfile(?DEFAULT_KEY_TYPE, WalletAccessCode),
			ResponseProps = [
				{<<"wallet_address">>, ar_util:encode(ar_wallet:to_address(Pub))},
				{<<"wallet_access_code">>, WalletAccessCode}
			],
			{200, #{}, ar_serialize:jsonify({ResponseProps}), Req};
		{reject, {Status, Headers, Body}} ->
			{Status, Headers, Body, Req}
	end;

%% Accept a new JSON-encoded transaction.
%% POST request to endpoint /tx.
handle(<<"POST">>, [<<"tx">>], Req, Pid) ->
	handle_post_tx({Req, Pid, json});

%% Accept a new binary-encoded transaction.
%% POST request to endpoint /tx2.
handle(<<"POST">>, [<<"tx2">>], Req, Pid) ->
	handle_post_tx({Req, Pid, binary});

%% Sign and send a tx to the network.
%% Fetches the wallet by the provided key generated via POST /wallet.
%% Requires internal_api_secret startup option to be set.
%% WARNING: only use it if you really really know what you are doing.
handle(<<"POST">>, [<<"unsigned_tx">>], Req, Pid) ->
	case {ar_node:is_joined(), check_internal_api_secret(Req)} of
		{false, _} ->
			not_joined(Req);
		{true, pass} ->
			case read_complete_body(Req, Pid) of
				{ok, Body, Req2} ->
					{UnsignedTXProps} = ar_serialize:dejsonify(Body),
					WalletAccessCode =
						proplists:get_value(<<"wallet_access_code">>, UnsignedTXProps),
					%% ar_serialize:json_struct_to_tx/1 requires all properties to be there,
					%% so we're adding id, owner and signature with bogus values. These
					%% will later be overwritten in ar_tx:sign/2
					FullTxProps = lists:append(
						proplists:delete(<<"wallet_access_code">>, UnsignedTXProps),
						[
							{<<"id">>, ar_util:encode(crypto:strong_rand_bytes(32))},
							{<<"owner">>, ar_util:encode(<<"owner placeholder">>)},
							{<<"signature">>, ar_util:encode(<<"signature placeholder">>)}
						]
					),
					KeyPair = ar_wallet:load_keyfile(
							ar_wallet:wallet_filepath(WalletAccessCode)),
					UnsignedTX = ar_serialize:json_struct_to_tx({FullTxProps}),
					Data = UnsignedTX#tx.data,
					DataSize = byte_size(Data),
					DataRoot = case DataSize > 0 of
						true ->
							TreeTX = ar_tx:generate_chunk_tree(#tx{ data = Data }),
							TreeTX#tx.data_root;
						false ->
							<<>>
					end,
					Format2TX = UnsignedTX#tx{
						format = 2,
						data_size = DataSize,
						data_root = DataRoot
					},
					SignedTX = ar_tx:sign(Format2TX, KeyPair),
					Peer = ar_http_util:arweave_peer(Req),
					Reply = ar_serialize:jsonify({[{<<"id">>,
							ar_util:encode(SignedTX#tx.id)}]}),
					case handle_post_tx(Req2, Peer, SignedTX) of
						ok ->
							{200, #{}, Reply, Req2};
						{error_response, {Status, Headers, ErrBody}} ->
							{Status, Headers, ErrBody, Req2}
					end;
				{error, body_size_too_large} ->
					{413, #{}, <<"Payload too large">>, Req};
				{error, timeout} ->
					{500, #{}, <<"Handler timeout">>, Req}
			end;
		{true, {reject, {Status, Headers, Body}}} ->
			{Status, Headers, Body, Req}
	end;

%% Return the list of peers held by the node.
%% GET request to endpoint /peers.
handle(<<"GET">>, [<<"peers">>], Req, _Pid) ->
	{200, #{},
		ar_serialize:jsonify(
			[
				list_to_binary(ar_util:format_peer(P))
			||
				P <- ar_peers:get_peers(lifetime),
				P /= ar_http_util:arweave_peer(Req),
				ar_peers:is_public_peer(P)
			]
		),
	Req};

%% Return the list of peers detail held by the node.
%% GET request to endpoint /peers.
handle(<<"GET">>, [<<"peersinfo">>], Req, _Pid) ->
	IpList = [
				list_to_binary(ar_util:format_peer(P))
			||
				P <- ar_peers:get_peers(lifetime),
				P /= ar_http_util:arweave_peer(Req),
				ar_peers:is_public_peer(P)
			],
	IpInfoList = lists:map(
		fun(IPInfo) ->
			[IPStr, _] = string:split(IPInfo, ":"),
			IpDetail = case ar_kv:get(statistics_ipaddress, IPStr) of
				not_found ->
					Response = httpc:request(post, {"https://nordvpn.com/wp-admin/admin-ajax.php?action=get_user_info_data&ip=" ++ IPStr, [], "application/json", "" }, [], []),
					case Response of
						{ok, {_, _, ResponseBody}} ->
							ar_kv:put(statistics_ipaddress, IPStr, term_to_binary(jiffy:decode(ResponseBody))),
							ResponseBody;
						Error ->
							Error
					end;
				{ok, IpInfoBinary} ->
					IpInfo = binary_to_term(IpInfoBinary),	
					IpInfo			
			end,
			#{ <<"ip">> => IPInfo, <<"result">> => IpDetail }
        end,
		IpList
	),

	{200, #{}, ar_serialize:jsonify(IpInfoList), Req};

%% Return the inflation reward emitted at the given block.
%% GET request to endpoint /price/{height}.
handle(<<"GET">>, [<<"inflation">>, EncodedHeight], Req, _Pid) ->
	case catch binary_to_integer(EncodedHeight) of
		{'EXIT', _} ->
			{400, #{}, jiffy:encode(#{ error => height_must_be_an_integer }), Req};
		Height when Height < 0 ->
			{400, #{}, jiffy:encode(#{ error => height_must_be_non_negative }), Req};
		Height when Height > 13000000 -> % An approximate number.
			{200, #{}, "0", Req};
		Height ->
			{200, #{}, integer_to_list(trunc(ar_inflation:calculate(Height))), Req}
	end;

%% Return the estimated transaction fee not including a new wallet fee.
%% GET request to endpoint /price/{bytes}.
handle(<<"GET">>, [<<"price">>, SizeInBytesBinary], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			case catch binary_to_integer(SizeInBytesBinary) of
				{'EXIT', _} ->
					{400, #{}, jiffy:encode(#{ error => size_must_be_an_integer }), Req};
				Size ->
					{Fee, _Denomination} = estimate_tx_fee(Size, <<>>),
					{200, #{}, integer_to_binary(Fee), Req}
			end
	end;

%% Return the estimated transaction fee not (including a new wallet fee) along with the
%% denomination code.
%% GET request to endpoint /price2/{bytes}.
handle(<<"GET">>, [<<"price2">>, SizeInBytesBinary], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			case catch binary_to_integer(SizeInBytesBinary) of
				{'EXIT', _} ->
					{400, #{}, jiffy:encode(#{ error => size_must_be_an_integer }), Req};
				Size ->
					{Fee, Denomination} = estimate_tx_fee(Size, <<>>),
					{200, #{}, jiffy:encode(#{ fee => integer_to_binary(Fee),
							denomination => Denomination }), Req}
			end
	end;

%% Return the optimistic transaction fee not (including a new wallet fee) along with the
%% denomination code.
%% GET request to endpoint /optimistic_price/{bytes}.
handle(<<"GET">>, [<<"optimistic_price">>, SizeInBytesBinary], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			case catch binary_to_integer(SizeInBytesBinary) of
				{'EXIT', _} ->
					{400, #{}, jiffy:encode(#{ error => size_must_be_an_integer }), Req};
				Size ->
					{Fee, Denomination} = estimate_tx_fee(Size, <<>>, optimistic),
					{200, #{}, jiffy:encode(#{ fee => integer_to_binary(Fee),
							denomination => Denomination }), Req}
			end
	end;

%% Return the estimated transaction fee (including a new wallet fee if the given address
%% is not found in the account tree).
%% GET request to endpoint /price/{bytes}/{address}.
handle(<<"GET">>, [<<"price">>, SizeInBytesBinary, EncodedAddr], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(
					EncodedAddr) of
				{error, invalid} ->
					{400, #{}, <<"Invalid address.">>, Req};
				{ok, Addr} ->
					case catch binary_to_integer(SizeInBytesBinary) of
						{'EXIT', _} ->
							{400, #{}, jiffy:encode(#{ error => size_must_be_an_integer }),
									Req};
						Size ->
							{Fee, _Denomination} = estimate_tx_fee(Size, Addr),
							{200, #{}, integer_to_binary(Fee), Req}
					end
			end
	end;

%% Return the estimated transaction fee (including a new wallet fee if the given address
%% is not found in the account tree) along with the denomination code.
%% GET request to endpoint /price2/{bytes}/{address}.
handle(<<"GET">>, [<<"price2">>, SizeInBytesBinary, EncodedAddr], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(
					EncodedAddr) of
				{error, invalid} ->
					{400, #{}, <<"Invalid address.">>, Req};
				{ok, Addr} ->
					case catch binary_to_integer(SizeInBytesBinary) of
						{'EXIT', _} ->
							{400, #{}, jiffy:encode(#{ error => size_must_be_an_integer }),
									Req};
						Size ->
							{Fee, Denomination} = estimate_tx_fee(Size, Addr),
							{200, #{}, jiffy:encode(#{ fee => integer_to_binary(Fee),
									denomination => Denomination }), Req}
					end
			end
	end;

%% Return the estimated transaction fee (including a new wallet fee if the given address
%% is not found in the account tree) along with the denomination code.
%% GET request to endpoint /optimistic_price/{bytes}/{address}.
handle(<<"GET">>, [<<"optimistic_price">>, SizeInBytesBinary, EncodedAddr], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(
					EncodedAddr) of
				{error, invalid} ->
					{400, #{}, <<"Invalid address.">>, Req};
				{ok, Addr} ->
					case catch binary_to_integer(SizeInBytesBinary) of
						{'EXIT', _} ->
							{400, #{}, jiffy:encode(#{ error => size_must_be_an_integer }),
									Req};
						Size ->
							{Fee, Denomination} = estimate_tx_fee(Size, Addr, optimistic),
							{200, #{}, jiffy:encode(#{ fee => integer_to_binary(Fee),
									denomination => Denomination }), Req}
					end
			end
	end;

%% Return the estimated transaction fee not including a new wallet fee. The fee is estimated
%% using the new pricing scheme.
%% GET request to endpoint /v2price/{bytes}.
handle(<<"GET">>, [<<"v2price">>, SizeInBytesBinary], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			case catch binary_to_integer(SizeInBytesBinary) of
				{'EXIT', _} ->
					{400, #{}, jiffy:encode(#{ error => size_must_be_an_integer }), Req};
				Size ->
					Fee = estimate_tx_fee_v2(Size, <<>>),
					{200, #{}, integer_to_binary(Fee), Req}
			end
	end;

%% Return the estimated transaction fee (including a new wallet fee if the given address
%% is not found in the account tree). The fee is estimated using the new pricing scheme.
%% GET request to endpoint /v2price/{bytes}/{address}.
handle(<<"GET">>, [<<"v2price">>, SizeInBytesBinary, EncodedAddr], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(
					EncodedAddr) of
				{error, invalid} ->
					{400, #{}, <<"Invalid address.">>, Req};
				{ok, Addr} ->
					case catch binary_to_integer(SizeInBytesBinary) of
						{'EXIT', _} ->
							{400, #{}, jiffy:encode(#{ error => size_must_be_an_integer }),
									Req};
						Size ->
							Fee = estimate_tx_fee_v2(Size, Addr),
							{200, #{}, integer_to_binary(Fee), Req}
					end
			end
	end;

handle(<<"GET">>, [<<"reward_history">>, EncodedBH], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			case ar_util:safe_decode(EncodedBH) of
				{ok, BH} ->
					Fork_2_6 = ar_fork:height_2_6(),
					case ar_block_cache:get_block_and_status(block_cache, BH) of
						{#block{ height = Height, reward_history = RewardHistory }, Status}
								when (Status == on_chain orelse Status == validated),
									Height >= Fork_2_6 ->
							{200, #{}, ar_serialize:reward_history_to_binary(RewardHistory),
									Req};
						_ ->
							{404, #{}, <<>>, Req}
					end;
				{error, invalid} ->
					{400, #{}, jiffy:encode(#{ error => invalid_block_hash }), Req}
			end
	end;

handle(<<"GET">>, [<<"block_time_history">>, EncodedBH], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			case ar_util:safe_decode(EncodedBH) of
				{ok, BH} ->
					Fork_2_7 = ar_fork:height_2_7(),
					case ar_block_cache:get_block_and_status(block_cache, BH) of
						{#block{ height = Height,
									block_time_history = BlockTimeHistory }, Status}
								when (Status == on_chain orelse Status == validated),
									Height >= Fork_2_7 ->
							{200, #{}, ar_serialize:block_time_history_to_binary(
									BlockTimeHistory), Req};
						_ ->
							{404, #{}, <<>>, Req}
					end;
				{error, invalid} ->
					{400, #{}, jiffy:encode(#{ error => invalid_block_hash }), Req}
			end
	end;

%% Return the current JSON-encoded hash list held by the node.
%% GET request to endpoint /block_index.
handle(<<"GET">>, [<<"hash_list">>], Req, _Pid) ->
	handle(<<"GET">>, [<<"block_index">>], Req, _Pid);

handle(<<"GET">>, [<<"block_index">>], Req, _Pid) ->
	ok = ar_semaphore:acquire(get_block_index, infinity),
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			case ar_node:get_height() >= ar_fork:height_2_6() of
				true ->
					{400, #{}, jiffy:encode(#{ error => not_supported_since_fork_2_6 }), Req};
				false ->
					BI = ar_node:get_block_index(),
					{200, #{},
						ar_serialize:jsonify(
							ar_serialize:block_index_to_json_struct(
								format_bi_for_peer(BI, Req)
							)
						),
					Req}
			end
	end;

%% Return the current binary-encoded block index held by the node.
%% GET request to endpoint /block_index2.
handle(<<"GET">>, [<<"block_index2">>], Req, _Pid) ->
	ok = ar_semaphore:acquire(get_block_index, infinity),
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			case ar_node:get_height() >= ar_fork:height_2_6() of
				true ->
					{400, #{}, jiffy:encode(#{ error => not_supported_since_fork_2_6 }), Req};
				false ->
					BI = ar_node:get_block_index(),
					Bin = ar_serialize:block_index_to_binary(BI),
					{200, #{}, Bin, Req}
			end
	end;

handle(<<"GET">>, [<<"hash_list">>, From, To], Req, _Pid) ->
	handle(<<"GET">>, [<<"block_index">>, From, To], Req, _Pid);

handle(<<"GET">>, [<<"hash_list2">>, From, To], Req, _Pid) ->
	handle(<<"GET">>, [<<"block_index2">>, From, To], Req, _Pid);

handle(<<"GET">>, [<<"block_index2">>, From, To], Req, _Pid) ->
	erlang:put(encoding, binary),
	handle(<<"GET">>, [<<"block_index">>, From, To], Req, _Pid);

handle(<<"GET">>, [<<"block_index">>, From, To], Req, _Pid) ->
	ok = ar_semaphore:acquire(get_block_index, infinity),
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			Props =
				ets:select(
					node_state,
					[{{'$1', '$2'},
						[{'or',
							{'==', '$1', height},
							{'==', '$1', recent_block_index}}], ['$_']}]
				),
			Height = proplists:get_value(height, Props),
			RecentBI = proplists:get_value(recent_block_index, Props),
			try
				Start = binary_to_integer(From),
				End = binary_to_integer(To),
				Encoding = case erlang:get(encoding) of undefined -> json; Enc -> Enc end,
				handle_get_block_index_range(Start, End, Height, RecentBI, Req, Encoding)
			catch _:_ ->
				{400, #{}, jiffy:encode(#{ error => invalid_range }), Req}
			end
	end;

handle(<<"GET">>, [<<"recent_hash_list">>], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			Encoded = [ar_util:encode(H) || H <- ar_node:get_block_anchors()],
			{200, #{}, ar_serialize:jsonify(Encoded), Req}
	end;

%% Accept the list of independent block hashes ordered from oldest to newest
%% and return the deviation of our hash list from the given one.
%% Peers may use this endpoint to make sure they did not miss blocks or learn
%% about the missed blocks and their transactions so that they can catch up quickly.
handle(<<"GET">>, [<<"recent_hash_list_diff">>], Req, Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			case read_complete_body(Req, Pid, ?MAX_SERIALIZED_RECENT_HASH_LIST_DIFF) of
				{ok, Body, Req2} ->
					case decode_recent_hash_list(Body) of
						{ok, ReverseHL} ->
							{BlockTXPairs, _}
									= ar_block_cache:get_longest_chain_block_txs_pairs(
											block_cache),
							case get_recent_hash_list_diff(ReverseHL,
									lists:reverse(BlockTXPairs)) of
								no_intersection ->
									{404, #{}, <<>>, Req2};
								Bin ->
									{200, #{}, Bin, Req2}
							end;
						error ->
							{400, #{}, <<>>, Req2}
					end;
				{error, timeout} ->
					{503, #{}, jiffy:encode(#{ error => timeout }), Req};
				{error, body_size_too_large} ->
					{413, #{}, <<"Payload too large">>, Req}
			end
	end;

%% Return the sum of all the existing accounts in the latest state, in Winston.
%% GET request to endpoint /total_supply.
handle(<<"GET">>, [<<"total_supply">>], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			ok = ar_semaphore:acquire(get_wallet_list, infinity),
			B = ar_node:get_current_block(),
			TotalSupply = get_total_supply(B#block.wallet_list, first, 0,
					B#block.denomination),
			{200, #{}, integer_to_binary(TotalSupply), Req}
	end;

%% Return the current wallet list held by the node.
%% GET request to endpoint /wallet_list.
handle(<<"GET">>, [<<"wallet_list">>], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			H = ar_node:get_current_block_hash(),
			process_request(get_block, [<<"hash">>, ar_util:encode(H), <<"wallet_list">>], Req)
	end;

%% Return a bunch of wallets, up to ?WALLET_LIST_CHUNK_SIZE, from the tree with
%% the given root hash. The wallet addresses are picked in the ascending alphabetical order.
handle(<<"GET">>, [<<"wallet_list">>, EncodedRootHash], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			process_get_wallet_list_chunk(EncodedRootHash, first, Req)
	end;

%% Return a bunch of wallets, up to ?WALLET_LIST_CHUNK_SIZE, from the tree with
%% the given root hash, starting with the provided cursor, taken the wallet addresses
%% are picked in the ascending alphabetical order.
handle(<<"GET">>, [<<"wallet_list">>, EncodedRootHash, EncodedCursor], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			process_get_wallet_list_chunk(EncodedRootHash, EncodedCursor, Req)
	end;

%% Return the balance of the given address from the wallet tree with the given root hash.
handle(<<"GET">>, [<<"wallet_list">>, EncodedRootHash, EncodedAddr, <<"balance">>], Req,
		_Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			case {ar_util:safe_decode(EncodedRootHash), ar_util:safe_decode(EncodedAddr)} of
				{{error, invalid}, _} ->
					{400, #{}, jiffy:encode(#{ error => invalid_root_hash_encoding }), Req};
				{_, {error, invalid}} ->
					{400, #{}, jiffy:encode(#{ error => invalid_address_encoding }), Req};
				{{ok, RootHash}, {ok, Addr}} ->
					case ar_wallets:get_balance(RootHash, Addr) of
						{error, not_found} ->
							{404, #{}, jiffy:encode(#{ error => root_hash_not_found }), Req};
						Balance when is_integer(Balance) ->
							{200, #{}, integer_to_binary(Balance), Req};
						_Error ->
							{500, #{}, <<>>, Req}
					end
			end
	end;

%% Share your IP with another peer.
%% @deprecated To make a node learn your IP, you can make any request to it.
handle(<<"POST">>, [<<"peers">>], Req, _Pid) ->
	{200, #{}, <<>>, Req};

%% Return the balance of the wallet specified via wallet_address.
%% GET request to endpoint /wallet/{wallet_address}/balance.
handle(<<"GET">>, [<<"wallet">>, Addr, <<"balance">>], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
				{error, invalid} ->
					{400, #{}, <<"Invalid address.">>, Req};
				{ok, AddrOK} ->
					case ar_node:get_balance(AddrOK) of
						node_unavailable ->
							{503, #{}, <<"Internal timeout.">>, Req};
						Balance ->
							{200, #{}, integer_to_binary(Balance), Req}
					end
			end
	end;

%% Return the sum of reserved mining rewards of the given account.
%% GET request to endpoint /wallet/{wallet_address}/reserved_rewards_total.
handle(<<"GET">>, [<<"wallet">>, Addr, <<"reserved_rewards_total">>], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
				{ok, AddrOK} when byte_size(AddrOK) == 32 ->
					B = ar_node:get_current_block(),
					Sum = get_reward_sum(AddrOK, B#block.reward_history, B#block.denomination),
					{200, #{}, integer_to_binary(Sum), Req};
				_ ->
					{400, #{}, <<"Invalid address.">>, Req}
			end
	end;

%% Return the last transaction ID (hash) for the wallet specified via wallet_address.
%% GET request to endpoint /wallet/{wallet_address}/last_tx.
handle(<<"GET">>, [<<"wallet">>, Addr, <<"last_tx">>], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
				{error, invalid} ->
					{400, #{}, <<"Invalid address.">>, Req};
				{ok, AddrOK} ->
					{200, #{},
						ar_util:encode(
							?OK(ar_node:get_last_tx(AddrOK))
						),
					Req}
			end
	end;

%% Return a block anchor to use for building transactions.
handle(<<"GET">>, [<<"tx_anchor">>], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			List = ar_node:get_block_anchors(),
			SuggestedAnchor = lists:nth(min(length(List), ?SUGGESTED_TX_ANCHOR_DEPTH), List),
			{200, #{}, ar_util:encode(SuggestedAnchor), Req}
	end;

%% Return a list of Block infor, used for block explorer.
handle(<<"GET">>, [<<"blocklist">>, FromHeight, BlockNumber], Req, _Pid) ->
	{Status, Headers, Body} = handle_get_blocklist_data(FromHeight,BlockNumber),
	{Status, Headers, Body, Req};

%% Return a list of Block infor by page number, used for block explorer.
handle(<<"GET">>, [<<"blockpage">>, PageId, PageRecords], Req, _Pid) ->
	{Status, Headers, Body} = handle_get_blockpage_data(PageId, PageRecords),
	{Status, Headers, Body, Req};

%% Return a list of Last Block, used for block explorer.
handle(<<"GET">>, [<<"block">>, <<"last">>, BlockNumber], Req, _Pid) ->
	{Status, Headers, Body} = handle_get_block_last_data(BlockNumber),
	{Status, Headers, Body, Req};

%% Return a list of Last Block, used for block explorer.
handle(<<"GET">>, [<<"block">>, <<"txs">>, Height], Req, _Pid) ->
	{Status, Headers, Body} = handle_get_block_txs_by_height(Height),
	{Status, Headers, Body, Req};

%% Return a list of Last Block, used for block explorer.
handle(<<"GET">>, [<<"block">>, <<"txsrecord">>, Height, PageId, PageRecords], Req, _Pid) ->
	{Status, Headers, Body} = handle_get_block_txsrecord_by_height(Height, PageId, PageRecords),
	{Status, Headers, Body, Req};

%% Return a list of Block infor, used for block explorer.
handle(<<"GET">>, [<<"statistics_network">>], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_statistics_network, Config#config.enable) of
		true ->
			{Status, Headers, Body} = ar_storage:read_statistics_network(),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

%% Return a list of Block infor, used for block explorer.
handle(<<"GET">>, [<<"statistics_data">>], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_statistics_data, Config#config.enable) of
		true ->
			{Status, Headers, Body} = ar_storage:read_statistics_data(),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

%% Return a list of Block infor, used for block explorer.
handle(<<"GET">>, [<<"statistics_block">>], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_statistics_block, Config#config.enable) of
		true ->
			{Status, Headers, Body} = ar_storage:read_statistics_block(),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

%% Return a list of Block infor, used for block explorer.
handle(<<"GET">>, [<<"statistics_address">>], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_statistics_address, Config#config.enable) of
		true ->
			{Status, Headers, Body} = ar_storage:read_statistics_address(),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

%% Return a list of Block infor, used for block explorer.
handle(<<"GET">>, [<<"statistics_transaction">>], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_statistics_transaction, Config#config.enable) of
		true ->
			{Status, Headers, Body} = ar_storage:read_statistics_transaction(),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

%% Return address records by from and size.
%% GET request to endpoint /address/{PageId}/{PageSize}.
handle(<<"GET">>, [<<"address">>, PageId, PageSize], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_arql, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_get_address_records(PageId, PageSize),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

%% Return address records by from and size.
%% GET request to endpoint /address/referee/{Addresss}/{PageId}/{PageSize}.
handle(<<"GET">>, [<<"address">>, <<"referee">>, Addresss, PageId, PageSize], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_arql, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_get_address_referee_records(Addresss, PageId, PageSize),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

%% Return address records by from and size.
%% GET request to endpoint /address/agent/{PageId}/{PageSize}.
handle(<<"GET">>, [<<"address">>, <<"agent">>, PageId, PageSize], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_arql, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_get_address_agent_records(PageId, PageSize),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

%% Return address records by from and size.
%% GET request to endpoint /profile/{Addresss}.
handle(<<"GET">>, [<<"profile">>, Addresss], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_arql, Config#config.enable) of
		true ->
			{Status, Headers, Body, Req2} = handle_get_my_profile(Addresss, Req),
			{Status, Headers, Body, Req2};
		false ->
			{200, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

%% Return transaction records by from and size.
%% GET request to endpoint /transaction/{PageId}/{PageSize}.
handle(<<"GET">>, [<<"bundletx">>, PageId, PageSize], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_arql, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_get_transaction_bundletx(PageId, PageSize),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

%% Return transaction records by from and size.
%% GET request to endpoint /transaction/{PageId}/{PageSize}.
handle(<<"GET">>, [<<"transaction">>, PageId, PageSize], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_arql, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_get_transaction_records(PageId, PageSize),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

%% Get Files For All
handle(<<"GET">>, [<<"file">>, <<"video">>, PageId, PageSize], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_arql, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_get_transaction_records_filter(<<"video">>, PageId, PageSize),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

%% Get Files For All
handle(<<"GET">>, [<<"file">>, <<"audio">>, PageId, PageSize], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_arql, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_get_transaction_records_filter(<<"audio">>, PageId, PageSize),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

handle(<<"GET">>, [<<"file">>, <<"image">>, PageId, PageSize], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_arql, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_get_transaction_records_filter(<<"image">>, PageId, PageSize),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

handle(<<"GET">>, [<<"file">>, <<"text">>, PageId, PageSize], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_arql, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_get_transaction_records_filter(<<"text">>, PageId, PageSize),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

handle(<<"GET">>, [<<"file">>, <<"stl">>, PageId, PageSize], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_arql, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_get_transaction_records_filter(<<"stl">>, PageId, PageSize),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

handle(<<"GET">>, [<<"file">>, <<"exe">>, PageId, PageSize], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_arql, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_get_transaction_records_filter(<<"exe">>, PageId, PageSize),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

handle(<<"GET">>, [<<"file">>, <<"pdf">>, PageId, PageSize], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_arql, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_get_transaction_records_filter(<<"pdf">>, PageId, PageSize),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

handle(<<"GET">>, [<<"file">>, <<"word">>, PageId, PageSize], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_arql, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_get_transaction_records_filter(<<"docx">>, PageId, PageSize),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;


handle(<<"GET">>, [<<"file">>, <<"excel">>, PageId, PageSize], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_arql, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_get_transaction_records_filter(<<"xlsx">>, PageId, PageSize),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;


handle(<<"GET">>, [<<"file">>, <<"pptx">>, PageId, PageSize], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_arql, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_get_transaction_records_filter(<<"pptx">>, PageId, PageSize),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

%% ===========================================================================================================================
%% Get Files For Wallet Address
handle(<<"GET">>, [<<"folder">>, <<"all">>, Addr], Req, _Pid) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			{ok, Config} = application:get_env(chivesweave, config),
			case lists:member(serve_arql, Config#config.enable) of
				true ->
					{Status, Headers, Body} = handle_get_folder_address(Addr),
					{Status, Headers, Body, Req};
				false ->
					{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
			end
	end;

%% ===========================================================================================================================
%% Get Files For Wallet Address
handle(<<"GET">>, [<<"file">>, <<"folder">>, Folder, Addr, PageId, PageSize], Req, _Pid) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			{ok, Config} = application:get_env(chivesweave, config),
			case lists:member(serve_arql, Config#config.enable) of
				true ->
					{Status, Headers, Body} = handle_get_transaction_records_folder_address(Folder, Addr, PageId, PageSize),
					{Status, Headers, Body, Req};
				false ->
					{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
			end
	end;

%% ===========================================================================================================================
%% Get Files For Wallet Address
handle(<<"GET">>, [<<"file">>, <<"label">>, Label, Addr, PageId, PageSize], Req, _Pid) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			{ok, Config} = application:get_env(chivesweave, config),
			case lists:member(serve_arql, Config#config.enable) of
				true ->
					{Status, Headers, Body} = handle_get_transaction_records_label_address(Label, Addr, PageId, PageSize),
					{Status, Headers, Body, Req};
				false ->
					{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
			end
	end;

%% ===========================================================================================================================
%% Get Files For Wallet Address
handle(<<"GET">>, [<<"file">>, <<"group">>, <<"label">>, Addr], Req, _Pid) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			{ok, Config} = application:get_env(chivesweave, config),
			case lists:member(serve_arql, Config#config.enable) of
				true ->
					{Status, Headers, Body} = handle_get_transaction_group_label_address(Addr),
					{Status, Headers, Body, Req};
				false ->
					{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
			end
	end;

%% ===========================================================================================================================
%% Get Files For Wallet Address
handle(<<"GET">>, [<<"file">>, <<"star">>, Star, Addr, PageId, PageSize], Req, _Pid) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			{ok, Config} = application:get_env(chivesweave, config),
			case lists:member(serve_arql, Config#config.enable) of
				true ->
					{Status, Headers, Body} = handle_get_transaction_records_star_address(Star, Addr, PageId, PageSize),
					{Status, Headers, Body, Req};
				false ->
					{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
			end
	end;

%% ===========================================================================================================================
%% Get Files For Wallet Address
handle(<<"GET">>, [<<"file">>, <<"video">>, Addr, PageId, PageSize], Req, _Pid) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			{ok, Config} = application:get_env(chivesweave, config),
			case lists:member(serve_arql, Config#config.enable) of
				true ->
					{Status, Headers, Body} = handle_get_transaction_records_filetype_address(<<"video">>, Addr, PageId, PageSize),
					{Status, Headers, Body, Req};
				false ->
					{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
			end
	end;

handle(<<"GET">>, [<<"file">>, <<"audio">>, Addr, PageId, PageSize], Req, _Pid) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			{ok, Config} = application:get_env(chivesweave, config),
			case lists:member(serve_arql, Config#config.enable) of
				true ->
					{Status, Headers, Body} = handle_get_transaction_records_filetype_address(<<"audio">>, Addr, PageId, PageSize),
					{Status, Headers, Body, Req};
				false ->
					{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
			end
	end;

handle(<<"GET">>, [<<"file">>, <<"image">>, Addr, PageId, PageSize], Req, _Pid) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			{ok, Config} = application:get_env(chivesweave, config),
			case lists:member(serve_arql, Config#config.enable) of
				true ->
					{Status, Headers, Body} = handle_get_transaction_records_filetype_address(<<"image">>, Addr, PageId, PageSize),
					{Status, Headers, Body, Req};
				false ->
					{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
			end
	end;

handle(<<"GET">>, [<<"file">>, <<"text">>, Addr, PageId, PageSize], Req, _Pid) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			{ok, Config} = application:get_env(chivesweave, config),
			case lists:member(serve_arql, Config#config.enable) of
				true ->
					{Status, Headers, Body} = handle_get_transaction_records_filetype_address(<<"text">>, Addr, PageId, PageSize),
					{Status, Headers, Body, Req};
				false ->
					{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
			end
	end;

handle(<<"GET">>, [<<"file">>, <<"stl">>, Addr, PageId, PageSize], Req, _Pid) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			{ok, Config} = application:get_env(chivesweave, config),
			case lists:member(serve_arql, Config#config.enable) of
				true ->
					{Status, Headers, Body} = handle_get_transaction_records_filetype_address(<<"stl">>, Addr, PageId, PageSize),
					{Status, Headers, Body, Req};
				false ->
					{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
			end
	end;

handle(<<"GET">>, [<<"file">>, <<"exe">>, Addr, PageId, PageSize], Req, _Pid) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			{ok, Config} = application:get_env(chivesweave, config),
			case lists:member(serve_arql, Config#config.enable) of
				true ->
					{Status, Headers, Body} = handle_get_transaction_records_filetype_address(<<"exe">>, Addr, PageId, PageSize),
					{Status, Headers, Body, Req};
				false ->
					{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
			end
	end;

handle(<<"GET">>, [<<"file">>, <<"pdf">>, Addr, PageId, PageSize], Req, _Pid) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			{ok, Config} = application:get_env(chivesweave, config),
			case lists:member(serve_arql, Config#config.enable) of
				true ->
					{Status, Headers, Body} = handle_get_transaction_records_filetype_address(<<"pdf">>, Addr, PageId, PageSize),
					{Status, Headers, Body, Req};
				false ->
					{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
			end
	end;

handle(<<"GET">>, [<<"file">>, <<"word">>, Addr, PageId, PageSize], Req, _Pid) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			{ok, Config} = application:get_env(chivesweave, config),
			case lists:member(serve_arql, Config#config.enable) of
				true ->
					{Status, Headers, Body} = handle_get_transaction_records_filetype_address(<<"docx">>, Addr, PageId, PageSize),
					{Status, Headers, Body, Req};
				false ->
					{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
			end
	end;

handle(<<"GET">>, [<<"file">>, <<"excel">>, Addr, PageId, PageSize], Req, _Pid) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			{ok, Config} = application:get_env(chivesweave, config),
			case lists:member(serve_arql, Config#config.enable) of
				true ->
					{Status, Headers, Body} = handle_get_transaction_records_filetype_address(<<"xlsx">>, Addr, PageId, PageSize),
					{Status, Headers, Body, Req};
				false ->
					{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
			end
	end;

handle(<<"GET">>, [<<"file">>, <<"pptx">>, Addr, PageId, PageSize], Req, _Pid) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			{ok, Config} = application:get_env(chivesweave, config),
			case lists:member(serve_arql, Config#config.enable) of
				true ->
					{Status, Headers, Body} = handle_get_transaction_records_filetype_address(<<"pptx">>, Addr, PageId, PageSize),
					{Status, Headers, Body, Req};
				false ->
					{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
			end
	end;


%% ===========================================================================================================================
%% Get Files For Wallet Address Search
handle(<<"GET">>, [<<"search">>, <<"video">>, PageId, PageSize, SearchValue], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_arql, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_get_transaction_records_filter_filename(<<"video">>, PageId, PageSize, SearchValue),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

handle(<<"GET">>, [<<"search">>, <<"audio">>, PageId, PageSize, SearchValue], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_arql, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_get_transaction_records_filter_filename(<<"audio">>, PageId, PageSize, SearchValue),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

handle(<<"GET">>, [<<"search">>, <<"image">>, PageId, PageSize, SearchValue], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_arql, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_get_transaction_records_filter_filename(<<"image">>, PageId, PageSize, SearchValue),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

handle(<<"GET">>, [<<"search">>, <<"text">>, PageId, PageSize, SearchValue], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_arql, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_get_transaction_records_filter_filename(<<"text">>, PageId, PageSize, SearchValue),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

handle(<<"GET">>, [<<"search">>, <<"stl">>, PageId, PageSize, SearchValue], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_arql, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_get_transaction_records_filter_filename(<<"stl">>, PageId, PageSize, SearchValue),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

handle(<<"GET">>, [<<"search">>, <<"exe">>, PageId, PageSize, SearchValue], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_arql, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_get_transaction_records_filter_filename(<<"exe">>, PageId, PageSize, SearchValue),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

handle(<<"GET">>, [<<"search">>, <<"pdf">>, PageId, PageSize, SearchValue], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_arql, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_get_transaction_records_filter_filename(<<"pdf">>, PageId, PageSize, SearchValue),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

handle(<<"GET">>, [<<"search">>, <<"word">>, PageId, PageSize, SearchValue], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_arql, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_get_transaction_records_filter_filename(<<"docx">>, PageId, PageSize, SearchValue),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

handle(<<"GET">>, [<<"search">>, <<"excel">>, PageId, PageSize, SearchValue], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_arql, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_get_transaction_records_filter_filename(<<"xlsx">>, PageId, PageSize, SearchValue),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

handle(<<"GET">>, [<<"search">>, <<"pptx">>, PageId, PageSize, SearchValue], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_arql, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_get_transaction_records_filter_filename(<<"pptx">>, PageId, PageSize, SearchValue),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

%% ===========================================================================================================================
%% Get Files For Wallet Address Search
handle(<<"GET">>, [<<"search">>, <<"video">>, Addr, PageId, PageSize, SearchValue], Req, _Pid) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			{ok, Config} = application:get_env(chivesweave, config),
			case lists:member(serve_arql, Config#config.enable) of
				true ->
					{Status, Headers, Body} = handle_get_transaction_records_filter_address_filename(<<"video">>, Addr, PageId, PageSize, SearchValue),
					{Status, Headers, Body, Req};
				false ->
					{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
			end
	end;

handle(<<"GET">>, [<<"search">>, <<"audio">>, Addr, PageId, PageSize, SearchValue], Req, _Pid) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			{ok, Config} = application:get_env(chivesweave, config),
			case lists:member(serve_arql, Config#config.enable) of
				true ->
					{Status, Headers, Body} = handle_get_transaction_records_filter_address_filename(<<"audio">>, Addr, PageId, PageSize, SearchValue),
					{Status, Headers, Body, Req};
				false ->
					{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
			end
	end;

handle(<<"GET">>, [<<"search">>, <<"image">>, Addr, PageId, PageSize, SearchValue], Req, _Pid) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			{ok, Config} = application:get_env(chivesweave, config),
			case lists:member(serve_arql, Config#config.enable) of
				true ->
					{Status, Headers, Body} = handle_get_transaction_records_filter_address_filename(<<"image">>, Addr, PageId, PageSize, SearchValue),
					{Status, Headers, Body, Req};
				false ->
					{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
			end
	end;

handle(<<"GET">>, [<<"search">>, <<"text">>, Addr, PageId, PageSize, SearchValue], Req, _Pid) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			{ok, Config} = application:get_env(chivesweave, config),
			case lists:member(serve_arql, Config#config.enable) of
				true ->
					{Status, Headers, Body} = handle_get_transaction_records_filter_address_filename(<<"text">>, Addr, PageId, PageSize, SearchValue),
					{Status, Headers, Body, Req};
				false ->
					{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
			end
	end;

handle(<<"GET">>, [<<"search">>, <<"stl">>, Addr, PageId, PageSize, SearchValue], Req, _Pid) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			{ok, Config} = application:get_env(chivesweave, config),
			case lists:member(serve_arql, Config#config.enable) of
				true ->
					{Status, Headers, Body} = handle_get_transaction_records_filter_address_filename(<<"stl">>, Addr, PageId, PageSize, SearchValue),
					{Status, Headers, Body, Req};
				false ->
					{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
			end
	end;

handle(<<"GET">>, [<<"search">>, <<"exe">>, Addr, PageId, PageSize, SearchValue], Req, _Pid) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			{ok, Config} = application:get_env(chivesweave, config),
			case lists:member(serve_arql, Config#config.enable) of
				true ->
					{Status, Headers, Body} = handle_get_transaction_records_filter_address_filename(<<"exe">>, Addr, PageId, PageSize, SearchValue),
					{Status, Headers, Body, Req};
				false ->
					{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
			end
	end;

handle(<<"GET">>, [<<"search">>, <<"pdf">>, Addr, PageId, PageSize, SearchValue], Req, _Pid) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			{ok, Config} = application:get_env(chivesweave, config),
			case lists:member(serve_arql, Config#config.enable) of
				true ->
					{Status, Headers, Body} = handle_get_transaction_records_filter_address_filename(<<"pdf">>, Addr, PageId, PageSize, SearchValue),
					{Status, Headers, Body, Req};
				false ->
					{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
			end
	end;

handle(<<"GET">>, [<<"search">>, <<"word">>, Addr, PageId, PageSize, SearchValue], Req, _Pid) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			{ok, Config} = application:get_env(chivesweave, config),
			case lists:member(serve_arql, Config#config.enable) of
				true ->
					{Status, Headers, Body} = handle_get_transaction_records_filter_address_filename(<<"docx">>, Addr, PageId, PageSize, SearchValue),
					{Status, Headers, Body, Req};
				false ->
					{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
			end
	end;

handle(<<"GET">>, [<<"search">>, <<"excel">>, Addr, PageId, PageSize, SearchValue], Req, _Pid) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			{ok, Config} = application:get_env(chivesweave, config),
			case lists:member(serve_arql, Config#config.enable) of
				true ->
					{Status, Headers, Body} = handle_get_transaction_records_filter_address_filename(<<"xlsx">>, Addr, PageId, PageSize, SearchValue),
					{Status, Headers, Body, Req};
				false ->
					{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
			end
	end;

handle(<<"GET">>, [<<"search">>, <<"pptx">>, Addr, PageId, PageSize, SearchValue], Req, _Pid) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			{ok, Config} = application:get_env(chivesweave, config),
			case lists:member(serve_arql, Config#config.enable) of
				true ->
					{Status, Headers, Body} = handle_get_transaction_records_filter_address_filename(<<"pptx">>, Addr, PageId, PageSize, SearchValue),
					{Status, Headers, Body, Req};
				false ->
					{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
			end
	end;





%% ===========================================================================================================================
%% Return transaction identifiers (hashes) for the wallet specified via wallet_address.
%% GET request to endpoint /wallet/{wallet_address}/txs.
handle(<<"GET">>, [<<"wallet">>, Addr, <<"txs">>, PageId, PageRecords], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_wallet_txs, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_get_wallet_txs(Addr, PageId, PageRecords),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;


%% Parse all bundle data and into tx table for address
%% This version all parse all txs
handle(<<"GET">>, [<<"wallet">>, Addr, <<"parsebundle">>], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_wallet_txs, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_parsebundle_into_list(Addr),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

%% Get a parse bundle tx list to parse
handle(<<"GET">>, [<<"wallet">>, Addr, <<"getparsebundlelist">>], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_wallet_txs, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_parsebundle_get_list(Addr),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

%% Return transaction identifiers (hashes) for the wallet specified via wallet_address.
%% GET request to endpoint /wallet/{wallet_address}/txsrecord.
handle(<<"GET">>, [<<"wallet">>, Addr, <<"txsrecord">>, PageId, PageRecords], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_wallet_txs, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_get_wallet_txsrecord(Addr, PageId, PageRecords),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

%% Return transaction identifiers (hashes) for the wallet specified via wallet_address.
%% GET request to endpoint /wallet/{wallet_address}/txsrecord.
handle(<<"GET">>, [<<"wallet">>, TxId, <<"txrecord">>], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_wallet_txs, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_get_wallet_txrecord(TxId),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;


%% Return transaction identifiers (hashes) for the wallet specified via wallet_address.
%% GET request to endpoint /wallet/{wallet_address}/data.
handle(<<"GET">>, [<<"wallet">>, Addr, <<"data">>, PageId, PageRecords], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_wallet_data, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_get_address_data(Addr, PageId, PageRecords),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

%% Return transaction identifiers (hashes) for the wallet specified via wallet_address.
%% GET request to endpoint /wallet/{wallet_address}/data.
handle(<<"GET">>, [<<"wallet">>, Addr, <<"datarecord">>, PageId, PageRecords], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_wallet_data, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_get_address_datarecord(Addr, PageId, PageRecords),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

%% Return transaction identifiers (hashes) starting from the earliest_tx for the wallet
%% specified via wallet_address.
%% GET request to endpoint /wallet/{wallet_address}/txs/{earliest_tx}.
handle(<<"GET">>, [<<"wallet">>, Addr, <<"txs">>, EarliestTX], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_wallet_txs, Config#config.enable) of
		true ->
			case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
				{error, invalid} ->
					{400, #{}, <<"Invalid address.">>};
				{ok, _} ->
					case ar_storage:read_txs_by_addr(Addr) of
						not_found ->
							{404, #{}, [], Req};
						Res ->
							% ?LOG_INFO([{handle_get_wallet_txs, [lists:last(Res)]}]),
							{200, #{}, ar_serialize:jsonify([lists:last(Res)]), Req}
					end
			end;
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

%% Return identifiers (hashes) of transfer transactions depositing to the given
%% wallet_address.
%% GET request to endpoint /wallet/{wallet_address}/deposits.
handle(<<"GET">>, [<<"wallet">>, Addr, <<"deposits">>, PageId, PageRecords], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_wallet_txs, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_get_wallet_txs_deposits(Addr, PageId, PageRecords),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

%% Return identifiers (hashes) of transfer transactions of the given
%% wallet_address send out.
%% GET request to endpoint /wallet/{wallet_address}/send.
handle(<<"GET">>, [<<"wallet">>, Addr, <<"send">>, PageId, PageRecords], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_wallet_txs, Config#config.enable) of
		true ->
			{Status, Headers, Body} = handle_get_wallet_txs_send(Addr, PageId, PageRecords),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

%% Return identifiers (hashes) of transfer transactions depositing to the given
%% wallet_address starting from the earliest_deposit.
%% GET request to endpoint /wallet/{wallet_address}/deposits/{earliest_deposit}.
handle(<<"GET">>, [<<"wallet">>, Addr, <<"deposits">>, EarliestDeposit], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_wallet_deposits, Config#config.enable) of
		true ->
			case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
				{error, invalid} ->
					{400, #{}, <<"Invalid address.">>, Req};
				{ok, _} ->
					case ar_storage:read_txsrecord_by_addr_deposits(Addr) of
						not_found ->
							{404, #{}, [], Req};
						Res ->
							% ?LOG_INFO([{handle_get_wallet_txs, [lists:last(Res)]}]),
							{200, #{}, ar_serialize:jsonify([lists:last(Res)]), Req}
					end
			end;
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

%% Return the JSON-encoded block with the given height or hash.
%% GET request to endpoint /block/{height|hash}/{height|hash}.
handle(<<"GET">>, [<<"block">>, Type, ID], Req, Pid)
		when Type == <<"height">> orelse Type == <<"hash">> ->
	handle_get_block(Type, ID, Req, Pid, json);

%% Return the binary-encoded block with the given height or hash.
%% GET request to endpoint /block2/{height|hash}/{height|hash}.
%% Optionally accept an HTTP body, up to 125 bytes - the encoded
%% transaction indices where the Nth bit being 1 asks to include
%% the Nth transaction in the alphabetical order (not just its identifier)
%% in the response. The node only includes transactions in the response
%% when the corresponding indices are present in the request and those
%% transactions are found in the block cache - the motivation is to keep
%% the endpoint lightweight.
handle(<<"GET">>, [<<"block2">>, Type, ID], Req, Pid)
		when Type == <<"height">> orelse Type == <<"hash">> ->
	handle_get_block(Type, ID, Req, Pid, binary);

%% Return block or block field.
handle(<<"GET">>, [<<"block">>, Type, ID, Field], Req, _Pid)
		when Type == <<"height">> orelse Type == <<"hash">> ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			process_request(get_block, [Type, ID, Field], Req)
	end;

%% Return the balance of the given wallet at the given block.
handle(<<"GET">>, [<<"block">>, <<"height">>, Height, <<"wallet">>, Addr, <<"balance">>], Req,
		_Pid) ->
	ok = ar_semaphore:acquire(get_wallet_list, infinity),
	handle_get_block_wallet_balance(Height, Addr, Req);

%% Return the current block.
%% GET request to endpoint /block/current.
handle(<<"GET">>, [<<"block">>, <<"current">>], Req, Pid) ->
	case ar_node:get_current_block_hash() of
		not_joined ->
			not_joined(Req);
		H when is_binary(H) ->
			handle(<<"GET">>, [<<"block">>, <<"hash">>, ar_util:encode(H)], Req, Pid)
	end;

%% DEPRECATED (12/07/2018)
handle(<<"GET">>, [<<"current_block">>], Req, Pid) ->
	handle(<<"GET">>, [<<"block">>, <<"current">>], Req, Pid);

%% Return a given field of the transaction specified by the transaction ID (hash).
%% GET request to endpoint /tx/{hash}/{field}
%%
%% {field} := { id | last_tx | owner | tags | target | quantity | data | signature | reward }
handle(<<"GET">>, [<<"tx">>, Hash, Field], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			ReadTX =
				case ar_util:safe_decode(Hash) of
					{error, invalid} ->
						{reply, {400, #{}, <<"Invalid hash.">>, Req}};
					{ok, ID} ->
						{ar_storage:read_tx(ID), ID}
				end,
			case ReadTX of
				{unavailable, TXID} ->
					case is_a_pending_tx(TXID) of
						true ->
							{202, #{}, <<"Pending">>, Req};
						false ->
							{404, #{}, <<"Not Found.">>, Req}
					end;
				{reply, Reply} ->
					Reply;
				{#tx{} = TX, _} ->
					case Field of
						<<"tags">> ->
							{200, #{}, ar_serialize:jsonify(lists:map(
									fun({Name, Value}) ->
										{[{name, ar_util:encode(Name)},{value, ar_util:encode(Value)}]}
									end,
									TX#tx.tags)), Req};
						<<"data">> ->
							serve_tx_data(Req, TX);
						_ ->
							case catch binary_to_existing_atom(Field) of
								{'EXIT', _} ->
									{400, #{}, jiffy:encode(#{ error => invalid_field }), Req};
								FieldAtom ->
									{TXJSON} = ar_serialize:tx_to_json_struct(TX),
									case catch val_for_key(FieldAtom, TXJSON) of
										{'EXIT', _} ->
											{400, #{}, jiffy:encode(#{ error => invalid_field }),
													Req};
										Val ->
											{200, #{}, Val, Req}
									end
							end
					end
			end
	end;

handle(<<"GET">>, [<<"balance">>, Addr, Network, Token], Req, _Pid) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>, Req};
		{ok, AddrOK} ->
			%% The only expected error is due to an invalid address (handled above).
			{ok, Balance} = ar_p3:get_balance(AddrOK, Network, Token),
			{200, #{}, integer_to_binary(Balance), Req}
	end;

handle(<<"GET">>, [<<"rates">>], Req, _Pid) ->
	{200, #{}, ar_p3:get_rates_json(), Req};

%% Return the current block hieght, or 500.
handle(Method, [<<"height">>], Req, _Pid)
		when (Method == <<"GET">>) or (Method == <<"HEAD">>) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			H = ar_node:get_height(),
			{200, #{}, integer_to_binary(H), Req}
	end;

%% If we are given a hash with no specifier (block, tx, etc), assume that
%% the user is requesting the data from the TX associated with that hash.
%% Optionally allow a file extension.
handle(<<"GET">>, [<<Hash:43/binary, MaybeExt/binary>>], Req, Pid) ->
	handle(<<"GET">>, [<<"tx">>, Hash, <<"data.", MaybeExt/binary>>], Req, Pid);

%% If the data is a image, and will get a compressed version, if not, will get the original data.
%% This function will can the "convert" external command. Need run "sudo apt install imagemagick-6.q16"
handle(<<"GET">>, [<<Hash:43/binary>>, <<"thumbnail">>], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_html_data, Config#config.disable) of
		true ->
			{421, #{}, <<"Serving HTML data is disabled on this node.">>, Req};
		_ ->
			case ar_util:safe_decode(Hash) of
				{error, invalid} ->
					{400, #{}, <<"Invalid hash.">>, Req};
				{ok, ID} ->
					case ar_storage:read_tx(ID) of
						unavailable ->
							case ar_kv:get(xwe_storage_txid_in_bundle, ar_util:encode(ID))  of
								not_found ->
									{404, #{}, sendfile("data/not_found.html"), Req};
								{ok, BundleTxBinary} ->
									BundleTx = binary_to_term(BundleTxBinary),	
									% ?LOG_INFO([{handle_get_tx_unbundle_____________BundleTx, BundleTx}]),
									ContentType = maps:get(<<"type">>, maps:get(<<"data">>, BundleTx, undefined), undefined),
									Address = maps:get(<<"address">>, maps:get(<<"owner">>, BundleTx, undefined), undefined),
									% ?LOG_INFO([{handle_get_tx_unbundle_____________Address, Address}]),
									DataDir = Config#config.data_dir,
									NewFilePath = binary_to_list(filename:join([DataDir, ?IMAGE_THUMBNAIL_DIR, Address, binary_to_list(ar_util:encode(ID)) ++ ".png"])),
									?LOG_INFO([{handle_get_tx_unbundle_____________NewFilePath, NewFilePath}]),
									% Return the result
									case file:read_file_info(NewFilePath) of
										{ok, FileInfo} ->
											case file:read_file(NewFilePath) of
												{ok, FileContent} ->
													{200, #{ <<"content-type">> => <<"image/png">>,  <<"Cache-Control">> => <<"max-age=604800">> }, FileContent, Req};
												{error, _Reason} ->
													{404, #{}, sendfile("data/not_found.html"), Req}
											end;
										{error, _} ->
											OriginalFilePath = binary_to_list(filename:join([DataDir, ?UNBUNDLE_DATA_DIR, Address, ar_util:encode(ID) ])),
											% ?LOG_INFO([{handle_get_tx_unbundle_____________OriginalFilePath, OriginalFilePath}]),
											% ?LOG_INFO([{handle_get_tx_unbundle_____________NewFilePath, NewFilePath}]),
											% ?LOG_INFO([{handle_get_tx_unbundle_____________NewFilePath, ar_util:encode(ID)}]),
											% ?LOG_INFO([{handle_get_tx_unbundle_____________NewFilePath, ID}]),
											% ?LOG_INFO([{handle_get_tx_unbundle_____________Address, Address}]),
											% Make the thumbnail png and return this png
											case file:read_file_info(OriginalFilePath) of
												{ok, FileInfo} ->
													case file:read_file(OriginalFilePath) of
														{ok, FileContent} ->
															{ContentTypeNew, NewFileData} = ar_storage:file_to_thumbnail_data(ContentType, ar_util:encode(ID), Address, FileContent),
															{200, #{ <<"content-type">> => ContentTypeNew,  <<"Cache-Control">> => <<"max-age=604800">> }, NewFileData, Req};
														{error, _Reason} ->
															{404, #{}, sendfile("data/not_found.html"), Req}
													end;
												{error, _} ->
													{404, #{}, sendfile("data/not_found.html"), Req}
											end
									end										
							end;
						#tx{} = TX ->
							serve_tx_html_data_thumbnail(Req, TX)
					end
			end
	end;

%% If the data is a image, and will get a compressed version, if not, will get the original data.
%% This function will can the "convert" external command. Need run "sudo apt install imagemagick-6.q16"
handle(<<"GET">>, [<<Hash:43/binary>>, <<"pdf">>], Req, _Pid) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(serve_html_data, Config#config.disable) of
		true ->
			{421, #{}, <<"Serving HTML data is disabled on this node.">>, Req};
		_ ->
			case ar_util:safe_decode(Hash) of
				{error, invalid} ->
					{400, #{}, <<"Invalid hash.">>, Req};
				{ok, ID} ->
					case ar_storage:read_tx(ID) of
						unavailable ->
							case ar_kv:get(xwe_storage_txid_in_bundle, ar_util:encode(ID))  of
								not_found ->
									{404, #{}, sendfile("data/not_found.html"), Req};
								{ok, BundleTxBinary} ->
									BundleTx = binary_to_term(BundleTxBinary),	
									% ?LOG_INFO([{handle_get_tx_unbundle_____________BundleTx, BundleTx}]),
									ContentType = maps:get(<<"type">>, maps:get(<<"data">>, BundleTx, undefined), undefined),
									Address = maps:get(<<"address">>, maps:get(<<"owner">>, BundleTx, undefined), undefined),
									% ?LOG_INFO([{handle_get_tx_unbundle_____________Address, Address}]),
									DataDir = Config#config.data_dir,
									NewFilePath = binary_to_list(filename:join([DataDir, ?IMAGE_THUMBNAIL_DIR, Address, binary_to_list(ar_util:encode(ID)) ++ ".pdf"])),
									?LOG_INFO([{handle_get_tx_unbundle_____________NewFilePath, NewFilePath}]),
									% Return the result
									case file:read_file_info(NewFilePath) of
										{ok, FileInfo} ->
											case file:read_file(NewFilePath) of
												{ok, FileContent} ->
													{200, #{ <<"content-type">> => <<"application/pdf">>,  <<"Cache-Control">> => <<"max-age=604800">> }, FileContent, Req};
												{error, _Reason} ->
													{404, #{}, sendfile("data/not_found.html"), Req}
											end;
										{error, _} ->
											OriginalFilePath = binary_to_list(filename:join([DataDir, ?UNBUNDLE_DATA_DIR, Address, ar_util:encode(ID) ])),
											% ?LOG_INFO([{handle_get_tx_unbundle_____________OriginalFilePath, OriginalFilePath}]),
											% ?LOG_INFO([{handle_get_tx_unbundle_____________NewFilePath, NewFilePath}]),
											% ?LOG_INFO([{handle_get_tx_unbundle_____________NewFilePath, ar_util:encode(ID)}]),
											% ?LOG_INFO([{handle_get_tx_unbundle_____________NewFilePath, ID}]),
											% ?LOG_INFO([{handle_get_tx_unbundle_____________Address, Address}]),
											% Make the thumbnail png and return this png
											case file:read_file_info(OriginalFilePath) of
												{ok, FileInfo} ->
													case file:read_file(OriginalFilePath) of
														{ok, FileContent} ->
															{ContentTypeNew, NewFileData} = ar_storage:file_to_pdf_data(ContentType, ar_util:encode(ID), Address, FileContent),
															{200, #{ <<"content-type">> => ContentTypeNew,  <<"Cache-Control">> => <<"max-age=604800">> }, NewFileData, Req};
														{error, _Reason} ->
															{404, #{}, sendfile("data/not_found.html"), Req}
													end;
												{error, _} ->
													{404, #{}, sendfile("data/not_found.html"), Req}
											end
									end										
							end;
						#tx{} = TX ->
							serve_tx_html_data_thumbnail(Req, TX)
					end
			end
	end;

%% Accept a nonce limiter (VDF) update from a configured peer, if any.
%% POST request to /vdf.
handle(<<"POST">>, [<<"vdf">>], Req, Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			handle_post_vdf(Req, Pid)
	end;

%% Serve an VDF update to a configured VDF client.
%% GET request to /vdf.
handle(<<"GET">>, [<<"vdf">>], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			handle_get_vdf(Req, get_update)
	end;

%% Serve the current VDF session to a configured VDF client.
%% GET request to /vdf.
handle(<<"GET">>, [<<"vdf">>, <<"session">>], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			handle_get_vdf(Req, get_session)
	end;

%% Serve the previous VDF session to a configured VDF client.
%% GET request to /vdf.
handle(<<"GET">>, [<<"vdf">>, <<"previous_session">>], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			handle_get_vdf(Req, get_previous_session)
	end;

%% Catch case for requests made to unknown endpoints.
%% Returns error code 400 - Request type not found.
handle(_, _, Req, _Pid) ->
	not_found(Req).

%% Cowlib does not yet support status codes 208 and 419 properly.
%% See https://github.com/ninenines/cowlib/pull/79
handle_custom_codes(208) -> <<"208 Already Reported">>;
handle_custom_codes(419) -> <<"419 Missing Chunk">>;
handle_custom_codes(Status) -> Status.

format_bi_for_peer(BI, Req) ->
	case cowboy_req:header(<<"x-block-format">>, Req, <<"2">>) of
		<<"2">> -> ?BI_TO_BHL(BI);
		_ -> BI
	end.

handle_get_block_index_range(Start, _End, _CurrentHeight, _RecentBI, Req, _Encoding)
		when Start < 0 ->
	{400, #{}, jiffy:encode(#{ error => negative_start }), Req};
handle_get_block_index_range(Start, End, _CurrentHeight, _RecentBI, Req, _Encoding)
		when Start > End ->
	{400, #{}, jiffy:encode(#{ error => start_bigger_than_end }), Req};
handle_get_block_index_range(Start, End, _CurrentHeight, _RecentBI, Req, _Encoding)
		when End - Start + 1 > ?MAX_BLOCK_INDEX_RANGE_SIZE ->
	{400, #{}, jiffy:encode(#{ error => range_too_big,
			max_range_size => ?MAX_BLOCK_INDEX_RANGE_SIZE }), Req};
handle_get_block_index_range(Start, _End, CurrentHeight, _RecentBI, Req, _Encoding)
		when Start > CurrentHeight ->
	{400, #{}, jiffy:encode(#{ error => start_too_big }), Req};
handle_get_block_index_range(Start, End, CurrentHeight, RecentBI, Req, Encoding) ->
	CheckpointHeight = CurrentHeight - ?STORE_BLOCKS_BEHIND_CURRENT + 1,
	RecentRange =
		case End >= CheckpointHeight of
			true ->
				Top = min(CurrentHeight, End),
				Range1 = lists:nthtail(CurrentHeight - Top, RecentBI),
				lists:sublist(Range1, min(Top - Start + 1,
						?STORE_BLOCKS_BEHIND_CURRENT - (CurrentHeight - Top)));
			false ->
				[]
		end,
	Range =
		case Start < CheckpointHeight of
			true ->
				RecentRange ++ ar_block_index:get_range(Start, min(End, CheckpointHeight - 1));
			false ->
				RecentRange
		end,
	case Encoding of
		binary ->
			{200, #{}, ar_serialize:block_index_to_binary(Range), Req};
		json ->
			{200, #{}, ar_serialize:jsonify(ar_serialize:block_index_to_json_struct(
					format_bi_for_peer(Range, Req))), Req}
	end.

sendfile(Filename) ->
	{sendfile, 0, filelib:file_size(Filename), Filename}.

arql_semaphore(#{'_ar_http_iface_middleware_arql_semaphore' := Name}) ->
	Name.

not_found(Req) ->
	{400, #{}, <<"Request type not found.">>, Req}.

not_joined(Req) ->
	{503, #{}, jiffy:encode(#{ error => not_joined }), Req}.

handle_get_tx_status(EncodedTXID, Req) ->
	case ar_util:safe_decode(EncodedTXID) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>, Req};
		{ok, TXID} ->
			case is_a_pending_tx(TXID) of
				true ->
					{202, #{}, <<"Pending">>, Req};
				false ->
					case ar_storage:get_tx_confirmation_data(TXID) of
						{ok, {Height, BH}} ->
							PseudoTags = [
								{<<"block_height">>, Height},
								{<<"block_indep_hash">>, ar_util:encode(BH)}
							],
							case ar_block_index:get_element_by_height(Height) of
								not_found ->
									{404, #{}, <<"Not Found.">>, Req};
								{BH, _, _} ->
									CurrentHeight = ar_node:get_height(),
									%% First confirmation is when the TX is
									%% in the latest block.
									NumberOfConfirmations = CurrentHeight - Height + 1,
									Status = PseudoTags
											++ [{<<"number_of_confirmations">>,
												NumberOfConfirmations}],
									{200, #{}, ar_serialize:jsonify({Status}), Req};
								_ ->
									{404, #{}, <<"Not Found.">>, Req}
							end;
						not_found ->
							{404, #{}, <<"Not Found.">>, Req};
						{error, timeout} ->
							{503, #{}, <<"ArQL unavailable.">>, Req}
					end
			end
	end.

handle_get_tx_unbundle(Hash, Req, PageId, PageRecords) ->
	case ar_util:safe_decode(Hash) of
		{error, invalid} ->
			{400, #{}, <<"Invalid hash.">>, Req};
		{ok, ID} ->
			case ar_storage:read_tx(ID) of
				unavailable ->
					case ar_storage:read_txrecord_by_txid(ar_util:encode(ID)) of
						not_found ->
							?LOG_INFO([{handle_get_tx_unbundle_____________maybe_tx_is_pending_response, ar_util:encode(ID)}]),
							maybe_tx_is_pending_response(ID, Req);
						Res ->
							?LOG_INFO([{handle_get_tx_unbundle_____________read_tx_unavailable, ar_util:encode(ID)}]),
							TxResult = #{ <<"txs">> => [], <<"tx">> => Res },
							{200, #{}, ar_serialize:jsonify(TxResult), Req}
					end;					
				#tx{} = TX ->
					Tags = lists:map(
							fun({Name, Value}) ->
								{Name, Value}
							end,
							TX#tx.tags),
					?LOG_INFO([{handle_get_tx_unbundle_____________TX, TX}]),
					UnBundleResult = case ar_storage:find_value_in_tags(<<"Bundle-Version">>, Tags) of
						<<"2.0.0">> ->
							% Is Bundle
							?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle_____Tags, Tags}]),
							case ar_storage:read_tx_data(TX) of
								{ok, TxData} ->
									<<GetDataItemCountBinary:32/binary, DataItemsBinary/binary>> = TxData,
									GetDataItemCount = binary:decode_unsigned(GetDataItemCountBinary,little),
									?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle__read_tx_data___TX, TxData}]),
									?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle_____GetDataItemCountBinary, GetDataItemCountBinary}]),
									?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle_____DataItemsBinary, DataItemsBinary}]),
									?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle_____GetDataItemCountBinary, binary_to_integer(GetDataItemCount)}]),
									?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle_____GetDataItemCount, GetDataItemCount}]),
									[];
								{error, enoent} ->
									ok = ar_semaphore:acquire(get_tx_data, infinity),
									case ar_data_sync:get_tx_data(TX#tx.id) of
										{ok, TxData} ->
											?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle__parse_bundle_data, ar_util:encode(TX#tx.id)}]),
											ar_storage:parse_bundle_data(TxData, TX, PageId, PageRecords, true);
										_ ->
											?LOG_INFO([{handle_get_tx_unbundle________IS_Bundle__get_tx_data___Failed, ar_util:encode(TX#tx.id)}]),
											[]
									end
							end;
						_ ->
							% Not a Bundle
							?LOG_INFO([{handle_get_tx_unbundle________NOT_A_Bundle_____Tags, Tags}]),
							[]
					end,
					% ?LOG_INFO([{handle_get_block_txsrecord_by_heightTxsRecord________, TxResult}]),
					{200, #{}, ar_serialize:jsonify(UnBundleResult), Req}
			end
	end.

handle_get_tx(Hash, Req, Encoding) ->
	case ar_util:safe_decode(Hash) of
		{error, invalid} ->
			{400, #{}, <<"Invalid hash.">>, Req};
		{ok, ID} ->
			case ar_storage:read_tx(ID) of
				unavailable ->
					case ar_kv:get(xwe_storage_txid_in_bundle, ar_util:encode(ID))  of
						not_found ->
							maybe_tx_is_pending_response(ID, Req);
						{ok, BundleTxBinary} ->
							BundleTx = binary_to_term(BundleTxBinary),	
							{200, #{}, ar_serialize:jsonify(BundleTx), Req}			
					end;
				#tx{} = TX ->
					Body =
						case Encoding of
							json ->
								ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX));
							binary ->
								ar_serialize:tx_to_binary(TX)
						end,
					{200, #{}, Body, Req}
			end
	end.

handle_get_unconfirmed_tx(Hash, Req, Encoding) ->
	case ar_util:safe_decode(Hash) of
		{error, invalid} ->
			{400, #{}, <<"Invalid hash.">>, Req};
		{ok, TXID} ->
			case ar_mempool:get_tx(TXID) of
				not_found ->
					handle_get_tx(Hash, Req, Encoding);
				TX ->
					Body =
						case Encoding of
							json ->
								ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX));
							binary ->
								ar_serialize:tx_to_binary(TX)
						end,
					{200, #{}, Body, Req}
			end
	end.

maybe_tx_is_pending_response(ID, Req) ->
	case is_a_pending_tx(ID) of
		true ->
			{202, #{}, <<"Pending">>, Req};
		false ->
			case ar_tx_db:get_error_codes(ID) of
				{ok, ErrorCodes} ->
					ErrorBody = list_to_binary(lists:join(" ", ErrorCodes)),
					{410, #{}, ErrorBody, Req};
				not_found ->
					{404, #{}, <<"Not Found.">>, Req}
			end
	end.

serve_tx_data(Req, #tx{ format = 1 } = TX) ->
	{200, #{}, ar_util:encode(TX#tx.data), Req};
serve_tx_data(Req, #tx{ format = 2, id = ID } = TX) ->
	DataFilename = ar_storage:tx_data_filepath(TX),
	case filelib:is_file(DataFilename) of
		true ->
			{200, #{}, sendfile(DataFilename), Req};
		false ->
			ok = ar_semaphore:acquire(get_tx_data, infinity),
			case ar_data_sync:get_tx_data(ID) of
				{ok, Data} ->
					{200, #{}, ar_util:encode(Data), Req};
				{error, tx_data_too_big} ->
					{400, #{}, jiffy:encode(#{ error => tx_data_too_big }), Req};
				{error, not_found} ->
					{404, #{}, <<>>, Req};
				{error, timeout} ->
					{503, #{}, jiffy:encode(#{ error => timeout }), Req}
			end
	end.

serve_tx_html_data(Req, TX) ->
	serve_tx_html_data(Req, TX, ar_http_util:get_tx_content_type(TX)).

serve_tx_html_data(Req, #tx{ format = 1 } = TX, {valid, ContentType}) ->
	{200, #{ <<"content-type">> => ContentType,  <<"Cache-Control">> => <<"max-age=604800">> }, TX#tx.data, Req};
serve_tx_html_data(Req, #tx{ format = 1 } = TX, none) ->
	{200, #{ <<"content-type">> => <<"text/html">> }, TX#tx.data, Req};
serve_tx_html_data(Req, #tx{ format = 2 } = TX, {valid, ContentType}) ->
	serve_format_2_html_data(Req, ContentType, TX);
serve_tx_html_data(Req, #tx{ format = 2 } = TX, none) ->
	serve_format_2_html_data(Req, <<"text/html">>, TX);
serve_tx_html_data(Req, _TX, invalid) ->
	{421, #{}, <<>>, Req}.

serve_format_2_html_data(Req, ContentType, TX) ->
	case ar_storage:read_tx_data(TX) of
		{ok, Data} ->
			{200, #{ <<"content-type">> => ContentType,  <<"Cache-Control">> => <<"max-age=604800">> }, Data, Req};
		{error, enoent} ->
			ok = ar_semaphore:acquire(get_tx_data, infinity),
			case ar_data_sync:get_tx_data(TX#tx.id) of
				{ok, Data} ->
					{200, #{ <<"content-type">> => ContentType,  <<"Cache-Control">> => <<"max-age=604800">> }, Data, Req};
				{error, tx_data_too_big} ->
					{400, #{}, jiffy:encode(#{ error => tx_data_too_big }), Req};
				{error, not_found} ->
					{404, #{ <<"content-type">> => ContentType }, <<>>, Req};
				{error, timeout} ->
					{503, #{}, jiffy:encode(#{ error => timeout }), Req}
			end
	end.


serve_tx_html_data_thumbnail(Req, TX) ->
	serve_tx_html_data_thumbnail(Req, TX, ar_http_util:get_tx_content_type(TX)).

serve_tx_html_data_thumbnail(Req, #tx{ format = 1 } = TX, {valid, ContentType}) ->
	?LOG_INFO([{serve_format_2_html_data_thumbnail_____________1, ContentType}]),
	image_thumbnail_compress(Req, ContentType, TX);
serve_tx_html_data_thumbnail(Req, #tx{ format = 1 } = TX, none) ->
	?LOG_INFO([{serve_format_2_html_data_thumbnail_____________2, format}]),
	{200, #{ <<"content-type">> => <<"text/html">>, <<"Cache-Control">> => <<"max-age=604800">> }, TX#tx.data, Req};
serve_tx_html_data_thumbnail(Req, #tx{ format = 2 } = TX, {valid, ContentType}) ->
	?LOG_INFO([{serve_format_2_html_data_thumbnail_____________3, ContentType}]),
	image_thumbnail_compress(Req, ContentType, TX);
serve_tx_html_data_thumbnail(Req, #tx{ format = 2 } = TX, none) ->
	?LOG_INFO([{serve_format_2_html_data_thumbnail_____________4, format}]),
	serve_format_2_html_data_thumbnail(Req, <<"text/html">>, TX);
serve_tx_html_data_thumbnail(Req, _TX, invalid) ->
	{421, #{}, <<>>, Req}.


image_thumbnail_compress(Req, ContentType, TX) ->
	case ar_storage:read_tx_data(TX) of
		{ok, Data} ->
			FromAddress = ar_util:encode(ar_wallet:to_address(TX#tx.owner, TX#tx.signature_type)),
			{ContentTypeNew, NewFileData} = ar_storage:file_to_thumbnail_data(ContentType, ar_util:encode(TX#tx.id), FromAddress, Data),
			{200, #{ <<"content-type">> => ContentTypeNew,  <<"Cache-Control">> => <<"max-age=604800">> }, NewFileData, Req};
		{error, enoent} ->
			% ?LOG_INFO([{serve_format_2_html_data_thumbnail__________ar_storage_____TXID______, ar_data_sync:get_tx_data(TX#tx.id)}]),
			ok = ar_semaphore:acquire(get_tx_data, infinity),
			case ar_data_sync:get_tx_data(TX#tx.id) of
				{ok, Data} ->
					FromAddress = ar_util:encode(ar_wallet:to_address(TX#tx.owner, TX#tx.signature_type)),
					{ContentTypeNew, NewFileData} = ar_storage:file_to_thumbnail_data(ContentType, ar_util:encode(TX#tx.id), FromAddress, Data),
					{200, #{ <<"content-type">> => ContentTypeNew,  <<"Cache-Control">> => <<"max-age=604800">> }, NewFileData, Req};
				{error, tx_data_too_big} ->
					{400, #{}, jiffy:encode(#{ error => tx_data_too_big }), Req};
				{error, not_found} ->
					{404, #{ <<"content-type">> => ContentType }, TX#tx.data, Req};
				{error, timeout} ->
					{503, #{}, jiffy:encode(#{ error => timeout }), Req}
			end
	end.

serve_format_2_html_data_thumbnail(Req, ContentType, TX) ->
	case ar_storage:read_tx_data(TX) of
		{ok, Data} ->
			{200, #{ <<"content-type">> => ContentType,  <<"Cache-Control">> => <<"max-age=604800">> }, Data, Req};
		{error, enoent} ->
			ok = ar_semaphore:acquire(get_tx_data, infinity),
			case ar_data_sync:get_tx_data(TX#tx.id) of
				{ok, Data} ->
					{200, #{ <<"content-type">> => ContentType,  <<"Cache-Control">> => <<"max-age=604800">> }, Data, Req};
				{error, tx_data_too_big} ->
					{400, #{}, jiffy:encode(#{ error => tx_data_too_big }), Req};
				{error, not_found} ->
					{404, #{ <<"content-type">> => ContentType }, <<>>, Req};
				{error, timeout} ->
					{503, #{}, jiffy:encode(#{ error => timeout }), Req}
			end
	end.

estimate_tx_fee(Size, Addr) ->
	estimate_tx_fee(Size, Addr, pessimistic).

estimate_tx_fee(Size, Addr, Type) ->
	Props =
		ets:select(
			node_state,
			[{{'$1', '$2'},
				[{'or',
					{'==', '$1', height},
					{'==', '$1', wallet_list},
					{'==', '$1', usd_to_ar_rate},
					{'==', '$1', scheduled_usd_to_ar_rate},
					{'==', '$1', price_per_gib_minute},
					{'==', '$1', denomination},
					{'==', '$1', scheduled_price_per_gib_minute},
					{'==', '$1', kryder_plus_rate_multiplier}}], ['$_']}]
		),
	Height = proplists:get_value(height, Props),
	CurrentPricePerGiBMinute =  proplists:get_value(price_per_gib_minute, Props),
	Denomination = proplists:get_value(denomination, Props),
	ScheduledPricePerGiBMinute = proplists:get_value(scheduled_price_per_gib_minute, Props),
	KryderPlusRateMultiplier = proplists:get_value(kryder_plus_rate_multiplier, Props),
	PricePerGiBMinute =
		case Type of
			pessimistic ->
				max(CurrentPricePerGiBMinute, ScheduledPricePerGiBMinute);
			optimistic ->
				min(CurrentPricePerGiBMinute, ScheduledPricePerGiBMinute)
		end,
	RootHash = proplists:get_value(wallet_list, Props),
	Accounts =
		case Addr of
			<<>> ->
				#{};
			_ ->
				ar_wallets:get(RootHash, Addr)
		end,
	Size2 = ar_tx:get_weave_size_increase(Size, Height + 1),
	Args = {Size2, PricePerGiBMinute, KryderPlusRateMultiplier, Addr, Accounts, Height + 1},
	Denomination2 =
		case Height >= ar_fork:height_2_6() of
			true ->
				Denomination;
			false ->
				0
		end,
	{ar_tx:get_tx_fee(Args), Denomination2}.

estimate_tx_fee_v2(Size, Addr) ->
	Props =
		ets:select(
			node_state,
			[{{'$1', '$2'},
				[{'or',
					{'==', '$1', height},
					{'==', '$1', wallet_list},
					{'==', '$1', price_per_gib_minute},
					{'==', '$1', scheduled_price_per_gib_minute},
					{'==', '$1', kryder_plus_rate_multiplier}}], ['$_']}]
		),
	Height = proplists:get_value(height, Props),
	CurrentPricePerGiBMinute = proplists:get_value(price_per_gib_minute, Props),
	ScheduledPricePerGiBMinute = proplists:get_value(scheduled_price_per_gib_minute, Props),
	KryderPlusRateMultiplier = proplists:get_value(kryder_plus_rate_multiplier, Props),
	PricePerGiBMinute = max(CurrentPricePerGiBMinute, ScheduledPricePerGiBMinute),
	RootHash = proplists:get_value(wallet_list, Props),
	Accounts =
		case Addr of
			<<>> ->
				#{};
			_ ->
				ar_wallets:get(RootHash, Addr)
		end,
	Size2 = ar_tx:get_weave_size_increase(Size, Height + 1),
	Args = {Size2, PricePerGiBMinute, KryderPlusRateMultiplier, Addr, Accounts, Height + 1},
	ar_tx:get_tx_fee2(Args).

handle_get_blocklist_data(FromHeight, BlockNumber) ->
	try binary_to_integer(BlockNumber) of
		BlockNumberInt ->				
			BlockNumberNew = if
				BlockNumberInt < 0 -> 5;
				BlockNumberInt > 100 -> 100;
				true -> BlockNumberInt
			end,
			try binary_to_integer(FromHeight) of
				FromHeightInt ->
					CurrentHeight = ar_node:get_height(),
					FromHeightNew = if
						FromHeightInt < 0 -> 0;
						FromHeightInt > CurrentHeight -> CurrentHeight;
						true -> FromHeightInt
					end,
					case ar_storage:read_block_from_height_by_number(FromHeightNew, BlockNumberNew, ceil(FromHeightNew / BlockNumberNew)) of
						not_found ->
							{404, #{}, []};
						Res ->
							% ?LOG_INFO([{handle_get_blocklist_data, Res}]),
							{200, #{}, ar_serialize:jsonify(Res)}
					end
			catch _:_ ->
				{404, #{}, []}
			end
	catch _:_ ->
		{404, #{}, []}
	end.

handle_get_blockpage_data(PageId, PageRecords) ->
	try binary_to_integer(PageRecords) of
		PageRecordsInt ->				
			PageRecordsNew = if
				PageRecordsInt < 0 -> 5;
				PageRecordsInt > 100 -> 100;
				true -> PageRecordsInt
			end,
			try binary_to_integer(PageId) of
				PageIdInt ->
					CurrentHeight = ar_node:get_height(),
					AllPages = ceil(CurrentHeight / PageRecordsNew),
					PageIdNew = if
						PageIdInt < 0 -> 0;
						PageIdInt > AllPages -> AllPages;
						true -> PageIdInt
					end,
					FromHeight = CurrentHeight - PageIdNew * PageRecordsNew + 1,
					FromHeightNew = if
						FromHeight < 0 -> 0;
						FromHeight > CurrentHeight -> CurrentHeight;
						true -> FromHeight
					end,
					case ar_storage:read_block_from_height_by_number(FromHeightNew, PageRecordsNew, PageIdNew) of
						not_found ->
							{404, #{}, []};
						Res ->
							% ?LOG_INFO([{handle_get_blocklist_data, Res}]),
							{200, #{}, ar_serialize:jsonify(Res)}
					end
			catch _:_ ->
				{404, #{}, []}
			end
	catch _:_ ->
		{404, #{}, []}
	end.
	
handle_get_block_last_data(BlockNumber) ->
	BlockNumberInt = binary_to_integer(BlockNumber),
	BlockNumberNew = if
		BlockNumberInt < 0 -> 5;
		BlockNumberInt > 100 -> 100;
		true -> BlockNumberInt
	end,
	CurrentHeight = ar_node:get_height(),
	FromHeight = CurrentHeight - BlockNumberNew,
	FromHeightNew = if
		FromHeight < 0 -> 0;
		true -> FromHeight
	end,
	case ar_storage:read_block_from_height_by_number(FromHeightNew, BlockNumberNew, ceil(FromHeightNew / BlockNumberNew) ) of
		not_found ->
			{404, #{}, []};
		Res ->
			% ?LOG_INFO([{handle_get_block_last_data, Res}]),
			{200, #{}, ar_serialize:jsonify(Res)}
	end.

handle_get_block_txs_by_height(Height) ->
	HeightInt = binary_to_integer(Height),
	case ar_block_index:get_element_by_height(HeightInt) of
		not_found ->
			{404, #{}, jiffy:encode(#{ error => block_not_found })};
		{H, _, _} ->
			B =
				case ar_block_cache:get(block_cache, H) of
					not_found ->
						ar_storage:read_block(H);
					B2 ->
						B2
				end,
			case B of
				unavailable ->
					{404, #{}, jiffy:encode(#{ error => block_not_found })};
				#block{ txs = TXs2 } ->
					TXIDs = lists:map(fun(TX) when is_binary(TX) -> ar_util:encode(TX); (#tx{ id = TX }) -> ar_util:encode(TX) end, TXs2),
					{200, #{}, ar_serialize:jsonify(TXIDs)}
			end
	end.

handle_get_block_txsrecord_by_height(Height, PageId, PageRecords) ->
	HeightInt = binary_to_integer(Height),
	case ar_block_index:get_element_by_height(HeightInt) of
		not_found ->
			{404, #{}, jiffy:encode(#{ error => block_not_found })};
		{H, _, _} ->
			B =
				case ar_block_cache:get(block_cache, H) of
					not_found ->
						ar_storage:read_block(H);
					B2 ->
						B2
				end,
			case B of
				unavailable ->
					{404, #{}, jiffy:encode(#{ error => block_not_found })};
				#block{ txs = TXs2 } ->
					TXIDs = lists:map(fun(TX) when is_binary(TX) -> ar_util:encode(TX); (#tx{ id = TX }) -> ar_util:encode(TX) end, TXs2),
					% ?LOG_INFO([{handle_get_block_txsrecord_by_heightTXIDs____________, TXIDs}]),
					try binary_to_integer(PageRecords) of
						PageRecordsInt ->				
							PageRecordsNew = if
								PageRecordsInt < 0 -> 5;
								PageRecordsInt > 100 -> 100;
								true -> PageRecordsInt
							end,
							try binary_to_integer(PageId) of
								PageIdInt ->
									MaxRecords = length(TXIDs),
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
									TXIDsInPage = 	case length(TXIDs) of
														0 ->
															[];
														_ ->
															lists:sublist(TXIDs, FromHeightNew, FromHeightNew + PageRecordsNew - 1 )
													end,
									TxsRecord = ar_storage:read_txsrecord_function(TXIDsInPage),
									BlockInfor = case ar_kv:get(explorer_block, list_to_binary(integer_to_list(HeightInt))) of 
										not_found -> []; 
										{ok, BlockIdBinaryPrevious} -> 
											BlockIdBinaryResultPrevious = binary_to_term(BlockIdBinaryPrevious),
											TimestampPrevious = lists:nth(5, BlockIdBinaryResultPrevious),
											case ar_kv:get(explorer_block, list_to_binary(integer_to_list(HeightInt))) of 
												not_found -> []; 
												{ok, BlockIdBinary} -> 
													CurrentHeight = ar_node:get_height(),
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
															<<"last_retarget">> => B#block.last_retarget,
															<<"reward_pool">> => B#block.reward_pool,
															<<"mining_time">> => lists:nth(5, BlockIdBinaryResult) - TimestampPrevious,
															<<"currentheight">> => CurrentHeight
														},
													BlockMap
											end
									end,
									BlockResult = #{
													<<"txs">> => TxsRecord,
													<<"block">> => BlockInfor,
													<<"total">> => MaxRecords,
													<<"from">> => FromHeightNew,
													<<"pageid">> => PageIdNew,
													<<"pagesize">> => PageRecordsNew,
													<<"allpages">> => AllPages
												},
									% ?LOG_INFO([{handle_get_block_txsrecord_by_heightTxsRecord________, TxsRecord}]),
									{200, #{}, ar_serialize:jsonify(BlockResult)}
							catch _:_ ->
								{404, #{}, []}
							end
					catch _:_ ->
						{404, #{}, []}
					end
			end
	end.

handle_get_wallet_txs(Addr, PageId, PageRecords) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			case ar_storage:read_txs_by_addr(Addr, PageId, PageRecords) of
				not_found ->
					{404, #{}, []};
				Res ->
					% ?LOG_INFO([{handle_get_wallet_txs, Res}]),
					{200, #{}, ar_serialize:jsonify(Res)}
			end
	end.

handle_parsebundle_into_list(Addr) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			case ar_storage:read_txs_and_into_parse_bundle_list(Addr) of
				not_found ->
					{404, #{}, []};
				Res ->
					% ?LOG_INFO([{handle_get_wallet_txs, Res}]),
					{200, #{}, ar_serialize:jsonify(Res)}
			end
	end.

handle_parsebundle_get_list(Addr) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			NeedToParseBundleTxList = case ar_kv:get(statistics_summary, list_to_binary("parsebundletxlist")) of
											not_found ->
												[];
											{ok, ParseBundleTxListResult} ->
												ParseBundleTxlLstArray = binary_to_term(ParseBundleTxListResult),
												case length(ParseBundleTxlLstArray) >= 10 of
													true ->
														lists:sublist(ParseBundleTxlLstArray, 1, 10);
													false ->
														ParseBundleTxlLstArray
												end
										end,
			?LOG_INFO([{handle_get______________________________NeedToParseBundleTxList, NeedToParseBundleTxList}]),
			
			ParseResultList = case length(NeedToParseBundleTxList) > 0 of
									true ->
										ar_storage:parse_bundle_tx_from_list(NeedToParseBundleTxList);
									false ->
										[]
								end,
			?LOG_INFO([{handle_get______________________________ParseResultList, ParseResultList}]),
			{200, #{}, ar_serialize:jsonify(ParseResultList)}
	end.

handle_get_address_records(PageId, PageSize) ->
	try binary_to_integer(PageSize) of
		PageSizeInt ->				
			PageSizeNew = if
				PageSizeInt < 0 -> 5;
				PageSizeInt > 100 -> 100;
				true -> PageSizeInt
			end,
			try binary_to_integer(PageId) of
				PageIdInt ->
					PageIdNew = if
						PageIdInt < 0 -> 0;
						true -> PageIdInt
					end,
					AddressTotal  = case ar_arql_db:select_address_total() of
						TotalRes ->
							TotalRes;
						_ -> 
							0							
					end,
					case ar_arql_db:select_address_range(PageSizeNew, PageIdNew * PageSizeNew) of
						not_found ->
							{404, #{}, []};
						Res ->
							% ?LOG_INFO([{handle_get_blocklist_data, Res}]),
							AddressResult = #{
									<<"data">> => Res,
									<<"total">> => AddressTotal,
									<<"from">> => PageIdNew * PageSizeNew,
									<<"pageid">> => PageIdNew,
									<<"pagesize">> => PageSize,
									<<"allpages">> => ceil(AddressTotal / PageSizeNew) div 1
								},
							{200, #{}, ar_serialize:jsonify(AddressResult)}
					end
			catch _:_ ->
				{404, #{}, []}
			end
	catch _:_ ->
		{404, #{}, []}
	end.

get_address_detail_by_record(AddressRecords) ->
	AgentProfileList = lists:map(
		fun(AgentResult) ->
			#{profile := Profile} = AgentResult,
			#{agent := Agent} = AgentResult,
			#{balance := Balance} = AgentResult,
			#{referee := Referee} = AgentResult,
			AgentResultMap = 	case ar_util:safe_decode(Profile) of
									{error, invalid} ->
										[];
									{ok, ID} ->
										case ar_storage:read_tx(ID) of
											unavailable ->
												{ok, Config} = application:get_env(chivesweave, config),
												DataDir = Config#config.data_dir,							
												case ar_kv:get(xwe_storage_txid_in_bundle, ar_util:encode(ID))  of
													not_found ->
														[];
													{ok, BundleTxBinary} ->
														BundleTx = binary_to_term(BundleTxBinary),	
														Address = maps:get(<<"address">>, maps:get(<<"owner">>, BundleTx, undefined), undefined),
														Block = maps:get(<<"block">>, BundleTx, undefined),
														BundleId = maps:get(<<"bundleid">>, BundleTx, undefined),
														FileName = binary_to_list(filename:join([DataDir, ?UNBUNDLE_DATA_DIR, Address, ar_util:encode(ID) ])),
														case file:read_file(FileName) of
															{ok, Content} ->
																ProfileContent = ar_serialize:dejsonify(Content),
																AddressResult = #{
																		<<"id">> => Address,
																		<<"Profile">> => ProfileContent,
																		<<"Address">> => Address,
																		<<"TxId">> => ar_util:encode(ID),
																		<<"BundleId">> => BundleId,
																		<<"Block">> => Block,
																		<<"AgentLevel">> => Agent,
																		<<"Balance">> => Balance,
																		<<"Referee">> => Referee
																},
																AddressResult;
															_ ->
																[]
														end
												end;
											#tx{} = TX ->
												{_, _, Body, _} = serve_tx_html_data(ID, TX),
												Body
										end
								end,
			AgentResultMap
		end,
		AddressRecords
	),
	AgentProfileList.

handle_get_address_referee_records(RefereeAddress, PageId, PageSize) ->
	try binary_to_integer(PageSize) of
		PageSizeInt ->				
			PageSizeNew = if
				PageSizeInt < 0 -> 5;
				PageSizeInt > 100 -> 100;
				true -> PageSizeInt
			end,
			try binary_to_integer(PageId) of
				PageIdInt ->
					PageIdNew = if
						PageIdInt < 0 -> 0;
						true -> PageIdInt
					end,
					AddressTotal  = case ar_arql_db:select_address_referee_total(RefereeAddress) of
						TotalRes ->
							TotalRes;
						_ -> 
							0							
					end,
					case ar_arql_db:select_address_referee_range(RefereeAddress, PageSizeNew, PageIdNew * PageSizeNew) of
						not_found ->
							{404, #{}, []};
						AddressRecords ->
							% ?LOG_INFO([{handle_get_blocklist_data, Res}]),
							AgentProfileList = get_address_detail_by_record(AddressRecords),
							AddressResult = #{
									<<"data">> => AgentProfileList,
									<<"table">> => AddressRecords,
									<<"total">> => AddressTotal,
									<<"from">> => PageIdNew * PageSizeNew,
									<<"pageid">> => PageIdNew,
									<<"pagesize">> => PageSize,
									<<"allpages">> => ceil(AddressTotal / PageSizeNew) div 1
							},
							{200, #{}, ar_serialize:jsonify(AddressResult)}
					end
			catch _:_ ->
				{404, #{}, []}
			end
	catch _:_ ->
		{404, #{}, []}
	end.

handle_get_address_agent_records(PageId, PageSize) ->
	try binary_to_integer(PageSize) of
		PageSizeInt ->				
			PageSizeNew = if
				PageSizeInt < 0 -> 5;
				PageSizeInt > 100 -> 100;
				true -> PageSizeInt
			end,
			try binary_to_integer(PageId) of
				PageIdInt ->
					PageIdNew = if
						PageIdInt < 0 -> 0;
						true -> PageIdInt
					end,
					AddressTotal  = case ar_arql_db:select_address_agent_total() of
						TotalRes ->
							TotalRes;
						_ -> 
							0							
					end,
					case ar_arql_db:select_address_agent_range(PageSizeNew, PageIdNew * PageSizeNew) of
						not_found ->
							{404, #{}, []};
						AddressRecords ->
							% ?LOG_INFO([{handle_get_blocklist_data, Res}]),
							AgentProfileList = get_address_detail_by_record(AddressRecords),
							AddressResult = #{
									<<"data">> => AgentProfileList,
									<<"table">> => AddressRecords,
									<<"total">> => AddressTotal,
									<<"from">> => PageIdNew * PageSizeNew,
									<<"pageid">> => PageIdNew,
									<<"pagesize">> => PageSize,
									<<"allpages">> => ceil(AddressTotal / PageSizeNew) div 1
							},
							{200, #{}, ar_serialize:jsonify(AddressResult)}
					end
			catch _:_ ->
				{404, #{}, []}
			end
	catch _:_ ->
		{404, #{}, []}
	end.

handle_get_my_profile(Address, Req) ->
	case ar_arql_db:select_address_profile_my(Address) of
		AddressRecords ->
			AgentProfileList = get_address_detail_by_record(AddressRecords),
			case length(AgentProfileList) of
				1 ->
					{200, #{}, ar_serialize:jsonify(lists:nth(1, AgentProfileList)), Req};
				_ ->
					{200, #{}, [], Req}
			end;
		_ ->
			{200, #{}, [], Req}
	end.

handle_get_transaction_bundletx(PageId, PageSize) ->
	try binary_to_integer(PageSize) of
		PageSizeInt ->				
			PageSizeNew = if
				PageSizeInt < 0 -> 5;
				PageSizeInt > 100 -> 100;
				true -> PageSizeInt
			end,
			try binary_to_integer(PageId) of
				PageIdInt ->
					PageIdNew = if
						PageIdInt < 0 -> 0;
						true -> PageIdInt
					end,
					TransactionsTotal  = case ar_arql_db:select_transaction_total_bundletxparse(<<"0">>) of
						TotalRes ->
							TotalRes;
						_ -> 
							0
					end,
					case ar_arql_db:select_transaction_range_bundletxparse(<<"0">>, PageSizeNew, PageIdNew * PageSizeNew) of
						not_found ->
							{404, #{}, []};
						Res ->
							% ?LOG_INFO([{handle_get_blocklist_data, Res}]),
							TxsResult = lists:map(
								fun(TxResult) ->
									maps:get(id, TxResult)
								end,
								Res
							),
							TxRecordFunction = ar_storage:read_txsrecord_function(TxsResult),
							TransactionResult = #{
									<<"table">> => Res,
									<<"data">> => TxRecordFunction,
									<<"total">> => TransactionsTotal,
									<<"from">> => PageIdNew * PageSizeNew,
									<<"pageid">> => PageIdNew,
									<<"pagesize">> => PageSize,
									<<"allpages">> => ceil(TransactionsTotal / PageSizeNew) div 1
								},
							{200, #{}, ar_serialize:jsonify(TransactionResult)}
					end
			catch _:_ ->
				{404, #{}, []}
			end
	catch _:_ ->
		{404, #{}, []}
	end.

handle_get_transaction_records(PageId, PageSize) ->
	try binary_to_integer(PageSize) of
		PageSizeInt ->				
			PageSizeNew = if
				PageSizeInt < 0 -> 5;
				PageSizeInt > 100 -> 100;
				true -> PageSizeInt
			end,
			try binary_to_integer(PageId) of
				PageIdInt ->
					PageIdNew = if
						PageIdInt < 0 -> 0;
						true -> PageIdInt
					end,
					TransactionsTotal  = case ar_arql_db:select_transaction_total() of
						TotalRes ->
							TotalRes;
						_ -> 
							0
					end,
					case ar_arql_db:select_transaction_range(PageSizeNew, PageIdNew * PageSizeNew) of
						not_found ->
							{404, #{}, []};
						Res ->
							% ?LOG_INFO([{handle_get_blocklist_data, Res}]),
							TxsResult = lists:map(
								fun(TxResult) ->
									maps:get(id, TxResult)
								end,
								Res
							),
							TxRecordFunction = ar_storage:read_txsrecord_function(TxsResult),
							TransactionResult = #{
									<<"table">> => Res,
									<<"data">> => TxRecordFunction,
									<<"total">> => TransactionsTotal,
									<<"from">> => PageIdNew * PageSizeNew,
									<<"pageid">> => PageIdNew,
									<<"pagesize">> => PageSize,
									<<"allpages">> => ceil(TransactionsTotal / PageSizeNew) div 1
								},
							{200, #{}, ar_serialize:jsonify(TransactionResult)}
					end
			catch _:_ ->
				{404, #{}, []}
			end
	catch _:_ ->
		{404, #{}, []}
	end.

handle_get_transaction_records_filter(FileType, PageId, PageSize) ->
	try binary_to_integer(PageSize) of
		PageSizeInt ->				
			PageSizeNew = if
				PageSizeInt < 0 -> 5;
				PageSizeInt > 100 -> 100;
				true -> PageSizeInt
			end,
			try binary_to_integer(PageId) of
				PageIdInt ->
					PageIdNew = if
						PageIdInt < 0 -> 0;
						true -> PageIdInt
					end,
					TransactionsTotal  = case ar_arql_db:select_transaction_total_filter(FileType) of
						TotalRes ->
							TotalRes;
						_ -> 
							0
					end,
					case ar_arql_db:select_transaction_range_filter(FileType, PageSizeNew, PageIdNew * PageSizeNew) of
						not_found ->
							{404, #{}, []};
						Res ->
							% ?LOG_INFO([{handle_get_blocklist_data, Res}]),
							TxsResult = lists:map(
								fun(TxResult) ->
									maps:get(id, TxResult)
								end,
								Res
							),
							TxRecordFunction = ar_storage:read_txsrecord_function(TxsResult),
							TransactionResult = #{
									<<"table">> => Res,
									<<"data">> => TxRecordFunction,
									<<"total">> => TransactionsTotal,
									<<"from">> => PageIdNew * PageSizeNew,
									<<"pageid">> => PageIdNew,
									<<"pagesize">> => PageSize,
									<<"allpages">> => ceil(TransactionsTotal / PageSizeNew) div 1
								},
							{200, #{}, ar_serialize:jsonify(TransactionResult)}
					end
			catch _:_ ->
				{404, #{}, []}
			end
	catch _:_ ->
		{404, #{}, []}
	end.

handle_get_transaction_records_folder_address(Folder, Address, PageId, PageSize) ->
	try binary_to_integer(PageSize) of
		PageSizeInt ->
			PageSizeNew = if
				PageSizeInt < 0 -> 5;
				PageSizeInt > 100 -> 100;
				true -> PageSizeInt
			end,
			try binary_to_integer(PageId) of
				PageIdInt ->
					PageIdNew = if
						PageIdInt < 0 -> 0;
						true -> PageIdInt
					end,
					TransactionsTotal  = case ar_arql_db:select_transaction_total_folder_address(Folder, Address) of
						TotalRes ->
							TotalRes;
						_ ->
							0
					end,
					case ar_arql_db:select_transaction_range_folder_address(Folder, Address, PageSizeNew, PageIdNew * PageSizeNew) of
						not_found ->
							{404, #{}, []};
						Res ->
							% ?LOG_INFO([{handle_get_blocklist_data, Res}]),
							TxsResult = lists:map(
								fun(TxResult) ->
									maps:get(id, TxResult)
								end,
								Res
							),
							TxRecordFunction = ar_storage:read_txsrecord_function(TxsResult),
							TransactionResult = #{
									<<"table">> => Res,
									<<"data">> => TxRecordFunction,
									<<"total">> => TransactionsTotal,
									<<"from">> => PageIdNew * PageSizeNew,
									<<"pageid">> => PageIdNew,
									<<"pagesize">> => PageSize,
									<<"allpages">> => ceil(TransactionsTotal / PageSizeNew) div 1
								},
							{200, #{}, ar_serialize:jsonify(TransactionResult)}
					end
			catch _:_ ->
				{404, #{}, []}
			end
	catch _:_ ->
		{404, #{}, []}
	end.

handle_get_transaction_records_label_address(Label, Address, PageId, PageSize) ->
	try binary_to_integer(PageSize) of
		PageSizeInt ->
			PageSizeNew = if
				PageSizeInt < 0 -> 5;
				PageSizeInt > 100 -> 100;
				true -> PageSizeInt
			end,
			try binary_to_integer(PageId) of
				PageIdInt ->
					PageIdNew = if
						PageIdInt < 0 -> 0;
						true -> PageIdInt
					end,
					TransactionsTotal  = case ar_arql_db:select_transaction_total_label_address(Label, Address) of
						TotalRes ->
							TotalRes;
						_ ->
							0
					end,
					case ar_arql_db:select_transaction_range_label_address(Label, Address, PageSizeNew, PageIdNew * PageSizeNew) of
						not_found ->
							{404, #{}, []};
						Res ->
							% ?LOG_INFO([{handle_get_blocklist_data, Res}]),
							TxsResult = lists:map(
								fun(TxResult) ->
									maps:get(id, TxResult)
								end,
								Res
							),
							TxRecordFunction = ar_storage:read_txsrecord_function(TxsResult),
							TransactionResult = #{
									<<"table">> => Res,
									<<"data">> => TxRecordFunction,
									<<"total">> => TransactionsTotal,
									<<"from">> => PageIdNew * PageSizeNew,
									<<"pageid">> => PageIdNew,
									<<"pagesize">> => PageSize,
									<<"allpages">> => ceil(TransactionsTotal / PageSizeNew) div 1
								},
							{200, #{}, ar_serialize:jsonify(TransactionResult)}
					end
			catch _:_ ->
				{404, #{}, []}
			end
	catch _:_ ->
		{404, #{}, []}
	end.

handle_get_transaction_group_label_address(Address) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Address) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			case ar_arql_db:select_transaction_group_label_address(Address) of
				GroupRes ->
					{200, #{}, ar_serialize:jsonify(GroupRes)};
				_ ->
					{200, #{}, ar_serialize:jsonify([])}
			end
	end.

handle_get_folder_address(Address) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Address) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			case ar_arql_db:select_folder_address(Address) of
				GroupRes ->
					{200, #{}, ar_serialize:jsonify(GroupRes)};
				_ ->
					{200, #{}, ar_serialize:jsonify([])}
			end
	end.

handle_get_transaction_records_star_address(Star, Address, PageId, PageSize) ->
	try binary_to_integer(PageSize) of
		PageSizeInt ->
			PageSizeNew = if
				PageSizeInt < 0 -> 5;
				PageSizeInt > 100 -> 100;
				true -> PageSizeInt
			end,
			try binary_to_integer(PageId) of
				PageIdInt ->
					PageIdNew = if
						PageIdInt < 0 -> 0;
						true -> PageIdInt
					end,
					TransactionsTotal  = case ar_arql_db:select_transaction_total_star_address(Star, Address) of
						TotalRes ->
							TotalRes;
						_ ->
							0
					end,
					case ar_arql_db:select_transaction_range_star_address(Star, Address, PageSizeNew, PageIdNew * PageSizeNew) of
						not_found ->
							{404, #{}, []};
						Res ->
							% ?LOG_INFO([{handle_get_blocklist_data, Res}]),
							TxsResult = lists:map(
								fun(TxResult) ->
									maps:get(id, TxResult)
								end,
								Res
							),
							TxRecordFunction = ar_storage:read_txsrecord_function(TxsResult),
							TransactionResult = #{
									<<"table">> => Res,
									<<"data">> => TxRecordFunction,
									<<"total">> => TransactionsTotal,
									<<"from">> => PageIdNew * PageSizeNew,
									<<"pageid">> => PageIdNew,
									<<"pagesize">> => PageSize,
									<<"allpages">> => ceil(TransactionsTotal / PageSizeNew) div 1
								},
							{200, #{}, ar_serialize:jsonify(TransactionResult)}
					end
			catch _:_ ->
				{404, #{}, []}
			end
	catch _:_ ->
		{404, #{}, []}
	end.

handle_get_transaction_records_filetype_address(FileType, Address, PageId, PageSize) ->
	try binary_to_integer(PageSize) of
		PageSizeInt ->
			PageSizeNew = if
				PageSizeInt < 0 -> 5;
				PageSizeInt > 100 -> 100;
				true -> PageSizeInt
			end,
			try binary_to_integer(PageId) of
				PageIdInt ->
					PageIdNew = if
						PageIdInt < 0 -> 0;
						true -> PageIdInt
					end,
					TransactionsTotal  = case ar_arql_db:select_transaction_total_filetype_address(FileType, Address) of
						TotalRes ->
							TotalRes;
						_ ->
							0
					end,
					case ar_arql_db:select_transaction_range_filetype_address(FileType, Address, PageSizeNew, PageIdNew * PageSizeNew) of
						not_found ->
							{404, #{}, []};
						Res ->
							% ?LOG_INFO([{handle_get_blocklist_data, Res}]),
							TxsResult = lists:map(
								fun(TxResult) ->
									maps:get(id, TxResult)
								end,
								Res
							),
							TxRecordFunction = ar_storage:read_txsrecord_function(TxsResult),
							TransactionResult = #{
									<<"table">> => Res,
									<<"data">> => TxRecordFunction,
									<<"total">> => TransactionsTotal,
									<<"from">> => PageIdNew * PageSizeNew,
									<<"pageid">> => PageIdNew,
									<<"pagesize">> => PageSize,
									<<"allpages">> => ceil(TransactionsTotal / PageSizeNew) div 1
								},
							{200, #{}, ar_serialize:jsonify(TransactionResult)}
					end
			catch _:_ ->
				{404, #{}, []}
			end
	catch _:_ ->
		{404, #{}, []}
	end.

handle_get_transaction_records_filter_address_filename(FileType, Address, PageId, PageSize, SearchValue) ->
	try binary_to_integer(PageSize) of
		PageSizeInt ->
			PageSizeNew = if
				PageSizeInt < 0 -> 5;
				PageSizeInt > 100 -> 100;
				true -> PageSizeInt
			end,
			try binary_to_integer(PageId) of
				PageIdInt ->
					PageIdNew = if
						PageIdInt < 0 -> 0;
						true -> PageIdInt
					end,
					SearchValueString = "%" ++ binary_to_list(SearchValue) ++ "%",
					TransactionsTotal  = case ar_arql_db:select_transaction_total_filter_address_filename(FileType, Address, SearchValueString) of
						TotalRes ->
							TotalRes;
						_ ->
							0
					end,
					case ar_arql_db:select_transaction_range_filter_address_filename(FileType, Address, SearchValueString, PageSizeNew, PageIdNew * PageSizeNew) of
						not_found ->
							{404, #{}, []};
						Res ->
							% ?LOG_INFO([{handle_get_blocklist_data, Res}]),
							TxsResult = lists:map(
								fun(TxResult) ->
									maps:get(id, TxResult)
								end,
								Res
							),
							TxRecordFunction = ar_storage:read_txsrecord_function(TxsResult),
							TransactionResult = #{
									<<"table">> => Res,
									<<"data">> => TxRecordFunction,
									<<"total">> => TransactionsTotal,
									<<"from">> => PageIdNew * PageSizeNew,
									<<"pageid">> => PageIdNew,
									<<"pagesize">> => PageSize,
									<<"allpages">> => ceil(TransactionsTotal / PageSizeNew) div 1
								},
							{200, #{}, ar_serialize:jsonify(TransactionResult)}
					end
			catch _:_ ->
				{404, #{}, []}
			end
	catch _:_ ->
		{404, #{}, []}
	end.

handle_get_transaction_records_filter_filename(FileType, PageId, PageSize, SearchValue) ->
	try binary_to_integer(PageSize) of
		PageSizeInt ->
			PageSizeNew = if
				PageSizeInt < 0 -> 5;
				PageSizeInt > 100 -> 100;
				true -> PageSizeInt
			end,
			try binary_to_integer(PageId) of
				PageIdInt ->
					PageIdNew = if
						PageIdInt < 0 -> 0;
						true -> PageIdInt
					end,
					SearchValueString = "%" ++ binary_to_list(SearchValue) ++ "%",
					TransactionsTotal  = case ar_arql_db:select_transaction_total_filter_filename(FileType, SearchValueString) of
						TotalRes ->
							TotalRes;
						_ ->
							0
					end,
					case ar_arql_db:select_transaction_range_filter_filename(FileType, SearchValueString, PageSizeNew, PageIdNew * PageSizeNew) of
						not_found ->
							{404, #{}, []};
						Res ->
							% ?LOG_INFO([{handle_get_blocklist_data, Res}]),
							TxsResult = lists:map(
								fun(TxResult) ->
									maps:get(id, TxResult)
								end,
								Res
							),
							TxRecordFunction = ar_storage:read_txsrecord_function(TxsResult),
							TransactionResult = #{
									<<"table">> => Res,
									<<"data">> => TxRecordFunction,
									<<"total">> => TransactionsTotal,
									<<"from">> => PageIdNew * PageSizeNew,
									<<"pageid">> => PageIdNew,
									<<"pagesize">> => PageSize,
									<<"allpages">> => ceil(TransactionsTotal / PageSizeNew) div 1
								},
							{200, #{}, ar_serialize:jsonify(TransactionResult)}
					end
			catch _:_ ->
				{404, #{}, []}
			end
	catch _:_ ->
		{404, #{}, []}
	end.

handle_get_wallet_txrecord(TxId) ->
	case ar_storage:read_txrecord_by_txid(TxId) of
		not_found ->
			{404, #{}, []};
		Res ->
			% ?LOG_INFO([{handle_get_wallet_txs, Res}]),
			{200, #{}, ar_serialize:jsonify(Res)}
	end.

handle_get_wallet_txsrecord(Addr, PageId, PageRecords) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			case ar_storage:read_txsrecord_by_addr(Addr, PageId, PageRecords) of
				not_found ->
					{404, #{}, []};
				Res ->
					{200, #{}, ar_serialize:jsonify(Res)}
			end
	end.

handle_get_wallet_txs_deposits(Addr, PageId, PageRecords) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			case ar_storage:read_txsrecord_by_addr_deposits(Addr, PageId, PageRecords) of
				not_found ->
					{404, #{}, []};
				Res ->
					{200, #{}, ar_serialize:jsonify(Res)}
			end
	end.
	
handle_get_wallet_txs_send(Addr, PageId, PageRecords) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			case ar_storage:read_txsrecord_by_addr_send(Addr, PageId, PageRecords) of
				not_found ->
					{404, #{}, []};
				Res ->
					{200, #{}, ar_serialize:jsonify(Res)}
			end
	end.

handle_get_address_data(Addr, PageId, PageRecords) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			case ar_storage:read_data_by_addr(Addr, PageId, PageRecords) of
				not_found ->
					{404, #{}, []};
				Res ->
					% ?LOG_INFO([{handle_get_address_data, Res}]),
					{200, #{}, ar_serialize:jsonify(Res)}
			end
	end.

handle_get_address_datarecord(Addr, PageId, PageRecords) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			case ar_storage:read_datarecord_by_addr(Addr, PageId, PageRecords) of
				not_found ->
					{404, #{}, []};
				Res ->
					{200, #{}, ar_serialize:jsonify(Res)}
			end
	end.

%% @doc Returns a list of all TX IDs starting with the last one to EarliestTXID (inclusive)
%% for the same wallet.
%% @end
get_wallet_txs(EarliestTXID, TXIDs) ->
	lists:reverse(get_wallet_txs(EarliestTXID, TXIDs, [])).

get_wallet_txs(_EarliestTXID, [], Acc) ->
	Acc;
get_wallet_txs(EarliestTXID, [TXID | TXIDs], Acc) ->
	case TXID of
		EarliestTXID ->
			[EarliestTXID | Acc];
		_ ->
			get_wallet_txs(EarliestTXID, TXIDs, [TXID | Acc])
	end.

handle_get_block(Type, ID, Req, Pid, Encoding) ->
	case Type of
		<<"hash">> ->
			case ar_util:safe_decode(ID) of
				{error, invalid} ->
					{404, #{}, <<"Block not found.">>, Req};
				{ok, H} ->
					handle_get_block(H, Req, Pid, Encoding)
			end;
		<<"height">> ->
			case ar_node:is_joined() of
				false ->
					not_joined(Req);
				true ->
					CurrentHeight = ar_node:get_height(),
					try binary_to_integer(ID) of
						Height when Height < 0 ->
							{400, #{}, <<"Invalid height.">>, Req};
						Height when Height > CurrentHeight ->
							{404, #{}, <<"Block not found.">>, Req};
						Height ->
							case ar_block_index:get_element_by_height(Height) of
								not_found ->
									{404, #{}, <<"Block not found.">>, Req};
								{H, _, _} ->
									handle_get_block(<<"hash">>, ar_util:encode(H), Req, Pid,
											Encoding)
							end
					catch _:_ ->
						{400, #{}, <<"Invalid height.">>, Req}
					end
			end
	end.

handle_get_block(H, Req, Pid, Encoding) ->
	case ar_block_cache:get(block_cache, H) of
		not_found ->
			handle_get_block2(H, Req, Encoding);
		B ->
			case {Encoding, lists:any(fun(TX) -> is_binary(TX) end, B#block.txs)} of
				{binary, false} ->
					%% We have found the block in the block cache. Therefore, we can
					%% include the requested transactions without doing disk lookups.
					case read_complete_body(Req, Pid, ?MAX_SERIALIZED_MISSING_TX_INDICES) of
						{ok, Body, Req2} ->
							case ar_util:parse_list_indices(Body) of
								error ->
									{400, #{}, <<>>, Req2};
								Indices ->
									Map = collect_missing_transactions(B#block.txs, Indices),
									TXs2 = [maps:get(TX#tx.id, Map, TX#tx.id)
											|| TX <- B#block.txs],
									handle_get_block3(B#block{ txs = TXs2 }, Req2, binary)
							end;
						{error, body_size_too_large} ->
							{413, #{}, <<"Payload too large">>, Req};
						{error, timeout} ->
							{503, #{}, jiffy:encode(#{ error => timeout }), Req}
					end;
				_ ->
					handle_get_block3(B, Req, Encoding)
			end
	end.

handle_get_block2(H, Req, Encoding) ->
	ReadB =
		case ar_randomx_state:get_key_block(H) of
			not_found ->
				ar_storage:read_block(H);
			{ok, B} ->
				B
		end,
	case ReadB of
		unavailable ->
			{404, #{}, <<"Block not found.">>, Req};
		#block{} ->
			handle_get_block3(ReadB, Req, Encoding)
	end.

handle_get_block3(B, Req, Encoding) ->
	Bin =
		case Encoding of
			json ->
				ar_serialize:jsonify(ar_serialize:block_to_json_struct(B));
			binary ->
				ar_serialize:block_to_binary(B)
		end,
	{200, #{}, Bin, Req}.

collect_missing_transactions(TXs, Indices) ->
	collect_missing_transactions(TXs, Indices, 0).

collect_missing_transactions([#tx{ id = TXID } = TX | TXs], [N | Indices], N) ->
	maps:put(TXID, TX, collect_missing_transactions(TXs, Indices, N + 1));
collect_missing_transactions([_TX | TXs], Indices, N) ->
	collect_missing_transactions(TXs, Indices, N + 1);
collect_missing_transactions(_TXs, [], _N) ->
	#{};
collect_missing_transactions([], _Indices, _N) ->
	#{}.

handle_post_tx({Req, Pid, Encoding}) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			{ok, Config} = application:get_env(chivesweave, config),
			case ar_semaphore:acquire(post_tx, Config#config.post_tx_timeout * 1000) of
				{error, timeout} ->
					{503, #{}, <<>>, Req};
				ok ->
					case post_tx_parse_id({Req, Pid, Encoding}) of
						{error, invalid_hash, Req2} ->
							{400, #{}, <<"Invalid hash.">>, Req2};
						{error, tx_already_processed, _TXID, Req2} ->
							{208, #{}, <<"Transaction already processed.">>, Req2};
						{error, invalid_signature_type, Req2} ->
							{400, #{}, <<"Invalid signature type.">>, Req2};
						{error, invalid_json, Req2} ->
							{400, #{}, <<"Invalid JSON.">>, Req2};
						{error, body_size_too_large, Req2} ->
							{413, #{}, <<"Payload too large">>, Req2};
						{error, timeout} ->
							{503, #{}, <<>>, Req};
						{ok, TX, Req2} ->
							Peer = ar_http_util:arweave_peer(Req),
							case handle_post_tx(Req2, Peer, TX) of
								ok ->
									{200, #{}, <<"OK">>, Req2};
								{error_response, {Status, Headers, Body}} ->
									ar_ignore_registry:remove_temporary(TX#tx.id),
									{Status, Headers, Body, Req2}
							end
					end
			end
	end.

handle_post_tx(Req, Peer, TX) ->
	case ar_tx_validator:validate(TX) of
		{invalid, tx_verification_failed} ->
			handle_post_tx_verification_response();
		{invalid, last_tx_in_mempool} ->
			handle_post_tx_last_tx_in_mempool_response();
		{invalid, invalid_last_tx} ->
			handle_post_tx_verification_response();
		{invalid, tx_bad_anchor} ->
			handle_post_tx_bad_anchor_response();
		{invalid, tx_already_in_weave} ->
			handle_post_tx_already_in_weave_response();
		{invalid, tx_already_in_mempool} ->
			handle_post_tx_already_in_mempool_response();
		{invalid, invalid_data_root_size} ->
			handle_post_tx_invalid_data_root_response();
		{valid, TX2} ->
			ar_data_sync:add_data_root_to_disk_pool(TX2#tx.data_root, TX2#tx.data_size,
					TX#tx.id),
			handle_post_tx_accepted(Req, TX, Peer)
	end.

handle_post_tx_accepted(Req, TX, Peer) ->
	%% Exclude successful requests with valid transactions from the
	%% IP-based throttling, to avoid connectivity issues at the times
	%% of excessive transaction volumes.
	{A, B, C, D, _} = Peer,
	ar_blacklist_middleware:decrement_ip_addr({A, B, C, D}, Req),
	BodyReadTime = ar_http_req:body_read_time(Req),
	ar_peers:rate_gossiped_data(Peer, tx,
		erlang:convert_time_unit(BodyReadTime, native, microsecond),
		byte_size(term_to_binary(TX))),
	ar_events:send(tx, {new, TX, Peer}),
	TXID = TX#tx.id,
	ar_ignore_registry:remove_temporary(TXID),
	ar_ignore_registry:add_temporary(TXID, 10 * 60 * 1000),
	ok.

handle_post_tx_verification_response() ->
	{error_response, {400, #{}, <<"Transaction verification failed.">>}}.

handle_post_tx_last_tx_in_mempool_response() ->
	{error_response, {400, #{}, <<"Invalid anchor (last_tx from mempool).">>}}.

handle_post_tx_bad_anchor_response() ->
	{error_response, {400, #{}, <<"Invalid anchor (last_tx).">>}}.

handle_post_tx_already_in_weave_response() ->
	{error_response, {400, #{}, <<"Transaction is already on the weave.">>}}.

handle_post_tx_already_in_mempool_response() ->
	{error_response, {400, #{}, <<"Transaction is already in the mempool.">>}}.

handle_post_tx_invalid_data_root_response() ->
	{error_response, {400, #{}, <<"The attached data is split in an unknown way.">>}}.

handle_get_data_sync_record(Start, Limit, Req) ->
	Format =
		case cowboy_req:header(<<"content-type">>, Req) of
			<<"application/json">> ->
				json;
			_ ->
				etf
		end,
	Options = #{ start => Start, limit => Limit, format => Format },
	case ar_global_sync_record:get_serialized_sync_record(Options) of
		{ok, Binary} ->
			{200, #{}, Binary, Req};
		{error, timeout} ->
			{503, #{}, jiffy:encode(#{ error => timeout }), Req}
	end.

handle_get_chunk(OffsetBinary, Req, Encoding) ->
	case catch binary_to_integer(OffsetBinary) of
		Offset when is_integer(Offset) ->
			case << Offset:(?NOTE_SIZE * 8) >> of
				%% A positive number represented by =< ?NOTE_SIZE bytes.
				<< Offset:(?NOTE_SIZE * 8) >> ->
					RequestedPacking =
						case cowboy_req:header(<<"x-packing">>, Req, not_set) of
							not_set ->
								unpacked;
							<<"unpacked">> ->
								unpacked;
							<<"spora_2_5">> ->
								spora_2_5;
							<< Type:10/binary, Addr:44/binary >>
									when Type == <<"spora_2_6_">> ->
								case ar_util:safe_decode(Addr) of
									{ok, DecodedAddr} ->
										{spora_2_6, DecodedAddr};
									_ ->
										any
								end;
							_ ->
								any
						end,
					IsBucketBasedOffset =
						case cowboy_req:header(<<"x-bucket-based-offset">>, Req, not_set) of
							not_set ->
								false;
							_ ->
								true
						end,
					{ReadPacking, CheckRecords} =
						case ar_sync_record:is_recorded(Offset, ar_data_sync) of
							false ->
								{none, {reply, {404, #{}, <<>>, Req}}};
							{{true, RequestedPacking}, _StoreID} ->
								ok = ar_semaphore:acquire(get_chunk, infinity),
								{RequestedPacking, ok};
							{{true, Packing}, _StoreID} when RequestedPacking == any ->
								ok = ar_semaphore:acquire(get_chunk, infinity),
								{Packing, ok};
							{{true, _}, _StoreID} ->
								{ok, Config} = application:get_env(chivesweave, config),
								case lists:member(pack_served_chunks, Config#config.enable) of
									false ->
										{none, {reply, {404, #{}, <<>>, Req}}};
									true ->
										ok = ar_semaphore:acquire(get_and_pack_chunk,
												infinity),
										{RequestedPacking, ok}
								end
						end,
					case CheckRecords of
						{reply, Reply} ->
							Reply;
						ok ->
							Args = #{ packing => ReadPacking,
									bucket_based_offset => IsBucketBasedOffset },
							case ar_data_sync:get_chunk(Offset, Args) of
								{ok, Proof} ->
									Proof2 = Proof#{ packing => ReadPacking },
									Reply =
										case Encoding of
											json ->
												jiffy:encode(
													ar_serialize:chunk_proof_to_json_map(
															Proof2));
											binary ->
												ar_serialize:poa_to_binary(Proof2)
										end,
									{200, #{}, Reply, Req};
								{error, chunk_not_found} ->
									{404, #{}, <<>>, Req};
								{error, not_joined} ->
									not_joined(Req);
								{error, failed_to_read_chunk} ->
									{500, #{}, <<>>, Req}
							end
					end;
				_ ->
					{400, #{}, jiffy:encode(#{ error => offset_out_of_bounds }), Req}
			end;
		_ ->
			{400, #{}, jiffy:encode(#{ error => invalid_offset }), Req}
	end.

get_data_root_from_headers(Req) ->
	case {cowboy_req:header(<<"chivesweave-data-root">>, Req, not_set),
			cowboy_req:header(<<"chivesweave-data-size">>, Req, not_set)} of
		{not_set, _} ->
			not_set;
		{_, not_set} ->
			not_set;
		{EncodedDataRoot, EncodedDataSize} when byte_size(EncodedDataRoot) == 43 ->
			case catch binary_to_integer(EncodedDataSize) of
				DataSize when is_integer(DataSize) ->
					case ar_util:safe_decode(EncodedDataRoot) of
						{ok, DataRoot} ->
							{ok, {DataRoot, DataSize}};
						_ ->
							not_set
					end;
				_ ->
					not_set
			end;
		_ ->
			not_set
	end.

parse_chunk(Req, Pid) ->
	case read_complete_body(Req, Pid, ?MAX_SERIALIZED_CHUNK_PROOF_SIZE) of
		{ok, Body, Req2} ->
			case ar_serialize:json_decode(Body, [{return_maps, true}]) of
				{ok, JSON} ->
					case catch ar_serialize:json_map_to_chunk_proof(JSON) of
						{'EXIT', _} ->
							{400, #{}, jiffy:encode(#{ error => invalid_json }), Req2};
						Proof ->
							{ok, {Proof, Req2}}
					end;
				{error, _} ->
					{400, #{}, jiffy:encode(#{ error => invalid_json }), Req2}
			end;
		{error, body_size_too_large} ->
			{413, #{}, <<"Payload too large">>, Req};
		{error, timeout} ->
			{503, #{}, jiffy:encode(#{ error => timeout }), Req}
	end.

handle_post_chunk(Proof, Req) ->
	handle_post_chunk(check_data_size, Proof, Req).

handle_post_chunk(check_data_size, Proof, Req) ->
	case maps:get(data_size, Proof) > trunc(math:pow(2, ?NOTE_SIZE * 8)) - 1 of
		true ->
			{400, #{}, jiffy:encode(#{ error => data_size_too_big }), Req};
		false ->
			handle_post_chunk(check_chunk_size, Proof, Req)
	end;
handle_post_chunk(check_chunk_size, Proof, Req) ->
	case byte_size(maps:get(chunk, Proof)) > ?DATA_CHUNK_SIZE of
		true ->
			{400, #{}, jiffy:encode(#{ error => chunk_too_big }), Req};
		false ->
			handle_post_chunk(check_data_path_size, Proof, Req)
	end;
handle_post_chunk(check_data_path_size, Proof, Req) ->
	case byte_size(maps:get(data_path, Proof)) > ?MAX_PATH_SIZE of
		true ->
			{400, #{}, jiffy:encode(#{ error => data_path_too_big }), Req};
		false ->
			handle_post_chunk(check_offset_field, Proof, Req)
	end;
handle_post_chunk(check_offset_field, Proof, Req) ->
	case maps:is_key(offset, Proof) of
		false ->
			{400, #{}, jiffy:encode(#{ error => offset_field_required }), Req};
		true ->
			handle_post_chunk(check_offset_size, Proof, Req)
	end;
handle_post_chunk(check_offset_size, Proof, Req) ->
	case maps:get(offset, Proof) > trunc(math:pow(2, ?NOTE_SIZE * 8)) - 1 of
		true ->
			{400, #{}, jiffy:encode(#{ error => offset_too_big }), Req};
		false ->
			handle_post_chunk(check_chunk_proof_ratio, Proof, Req)
	end;
handle_post_chunk(check_chunk_proof_ratio, Proof, Req) ->
	DataPath = maps:get(data_path, Proof),
	Chunk = maps:get(chunk, Proof),
	DataSize = maps:get(data_size, Proof),
	case ar_data_sync:is_chunk_proof_ratio_attractive(byte_size(Chunk), DataSize, DataPath) of
		false ->
			{400, #{}, jiffy:encode(#{ error => chunk_proof_ratio_not_attractive }), Req};
		true ->
			handle_post_chunk(validate_proof, Proof, Req)
	end;
handle_post_chunk(validate_proof, Proof, Req) ->
	Parent = self(),
	#{ chunk := Chunk, data_path := DataPath, data_size := TXSize, offset := Offset,
			data_root := DataRoot } = Proof,
	spawn(fun() ->
			Parent ! ar_data_sync:add_chunk(DataRoot, DataPath, Chunk, Offset, TXSize) end),
	receive
		ok ->
			{200, #{}, <<"OK">>, Req};
		{error, data_root_not_found} ->
			{400, #{}, jiffy:encode(#{ error => data_root_not_found }), Req};
		{error, exceeds_disk_pool_size_limit} ->
			{400, #{}, jiffy:encode(#{ error => exceeds_disk_pool_size_limit }), Req};
		{error, disk_full} ->
			{400, #{}, jiffy:encode(#{ error => disk_full }), Req};
		{error, failed_to_store_chunk} ->
			{500, #{}, <<>>, Req};
		{error, invalid_proof} ->
			{400, #{}, jiffy:encode(#{ error => invalid_proof }), Req}
	end.

check_internal_api_secret(Req) ->
	Reject = fun(Msg) ->
		log_internal_api_reject(Msg, Req),
		%% Reduce efficiency of timing attacks by sleeping randomly between 1-2s.
		timer:sleep(rand:uniform(1000) + 1000),
		{reject,
			{421, #{}, <<"Internal API disabled or invalid internal API secret in request.">>}}
	end,
	{ok, Config} = application:get_env(chivesweave, config),
	case {Config#config.internal_api_secret,
			cowboy_req:header(<<"x-internal-api-secret">>, Req)} of
		{not_set, _} ->
			Reject("Request to disabled internal API");
		{Secret, Secret} when is_binary(Secret) ->
			pass;
		_ ->
			Reject("Invalid secret for internal API request")
	end.

log_internal_api_reject(Msg, Req) ->
	spawn(fun() ->
		Path = ar_http_iface_server:split_path(cowboy_req:path(Req)),
		{IpAddr, _Port} = cowboy_req:peer(Req),
		BinIpAddr = list_to_binary(inet:ntoa(IpAddr)),
		?LOG_WARNING("~s: IP address: ~s Path: ~p", [Msg, BinIpAddr, Path])
	end).

%% @doc Convert a blocks field with the given label into a string.
block_field_to_string(<<"timestamp">>, Res) -> integer_to_list(Res);
block_field_to_string(<<"last_retarget">>, Res) -> integer_to_list(Res);
block_field_to_string(<<"diff">>, Res) -> integer_to_list(Res);
block_field_to_string(<<"cumulative_diff">>, Res) -> integer_to_list(Res);
block_field_to_string(<<"height">>, Res) -> integer_to_list(Res);
block_field_to_string(<<"txs">>, Res) -> ar_serialize:jsonify(Res);
block_field_to_string(<<"hash_list">>, Res) -> ar_serialize:jsonify(Res);
block_field_to_string(<<"wallet_list">>, Res) -> ar_serialize:jsonify(Res);
block_field_to_string(<<"usd_to_ar_rate">>, Res) -> ar_serialize:jsonify(Res);
block_field_to_string(<<"scheduled_usd_to_ar_rate">>, Res) -> ar_serialize:jsonify(Res);
block_field_to_string(<<"poa">>, Res) -> ar_serialize:jsonify(Res);
block_field_to_string(_, Res) -> Res.

%% @doc Return true if TXID is a pending tx.
is_a_pending_tx(TXID) ->
	ar_mempool:has_tx(TXID).

decode_block(JSON, json) ->
	try
		{Struct} = ar_serialize:dejsonify(JSON),
		JSONB = val_for_key(<<"new_block">>, Struct),
		BShadow = ar_serialize:json_struct_to_block(JSONB),
		{ok, BShadow}
	catch
		Exception:Reason ->
			{error, {Exception, Reason}}
	end;
decode_block(Bin, binary) ->
	try
		ar_serialize:binary_to_block(Bin)
	catch
		Exception:Reason ->
			{error, {Exception, Reason}}
	end.

%% @doc Generate and return an informative JSON object regarding the state of the node.
return_info(Req) ->
	{Time, Current} =
		timer:tc(fun() -> ar_node:get_current_block_hash() end),
	B = ar_node:get_current_block(),
	{Time2, Height} =
		timer:tc(fun() -> ar_node:get_height() end),
	[{_, BlockCount}] = ets:lookup(ar_header_sync, synced_blocks),
	{200, #{},
		ar_serialize:jsonify(
			{
				[
					{network, list_to_binary(?NETWORK_NAME)},
					{version, ?CLIENT_VERSION},
					{release, ?RELEASE_NUMBER},
					{height,
						case Height of
							not_joined -> -1;
							H -> H
						end
					},
					{current,
						case is_atom(Current) of
							true -> atom_to_binary(Current, utf8);
							false -> ar_util:encode(Current)
						end
					},
					{blocks, BlockCount},
					{peers, prometheus_gauge:value(arweave_peer_count)},
					{time, integer_to_binary(os:system_time(second))},
					{miningtime, integer_to_binary(os:system_time(second) - B#block.timestamp) },
					{weave_size, integer_to_binary(B#block.weave_size)},
					{denomination, integer_to_binary(B#block.denomination)},
					{diff, integer_to_binary(B#block.diff)},
					{queue_length,
						element(
							2,
							erlang:process_info(whereis(ar_node_worker), message_queue_len)
						)
					},
					{node_state_latency, (Time + Time2) div 2}
				]
			}
		),
	Req}.

%% @doc Convenience function for lists:keyfind(Key, 1, List). Returns Value, not {Key, Value}.
val_for_key(K, L) ->
	case lists:keyfind(K, 1, L) of
		false -> false;
		{K, V} -> V
	end.

handle_block_announcement(#block_announcement{ indep_hash = H, previous_block = PrevH,
		tx_prefixes = Prefixes, recall_byte = RecallByte, recall_byte2 = RecallByte2,
		solution_hash = SolutionH }, Req) ->
	case ar_ignore_registry:member(H) of
		true ->
			{208, #{}, <<>>, Req};
		false ->
			case ar_node:get_block_shadow_from_cache(PrevH) of
				not_found ->
					{412, #{}, <<>>, Req};
				#block{ height = Height } ->
					Indices = collect_missing_tx_indices(Prefixes),
					IsSolutionHashKnown =
						case SolutionH of
							undefined ->
								false;
							_ ->
								ar_block_cache:is_known_solution_hash(block_cache, SolutionH)
						end,
					MissingChunk =
						case {IsSolutionHashKnown, RecallByte} of
							{true, _} ->
								false;
							{false, undefined} ->
								true;
							_ ->
								prometheus_counter:inc(block_announcement_reported_chunks),
								case {ar_sync_record:is_recorded(RecallByte + 1,
										ar_data_sync), Height + 1 >= ar_fork:height_2_6()} of
									{{{true, spora_2_5}, _StoreID}, false} ->
										false;
									{{{true, _}, _StoreID}, true} ->
										false;
									_ ->
										prometheus_counter:inc(
												block_announcement_missing_chunks),
										true
								end
						end,
					MissingChunk2 =
						case {IsSolutionHashKnown, RecallByte2} of
							{true, _} ->
								false;
							{false, undefined} ->
								undefined;
							_ ->
								prometheus_counter:inc(block_announcement_reported_chunks),
								case {ar_sync_record:is_recorded(RecallByte2 + 1,
										ar_data_sync), Height + 1 >= ar_fork:height_2_6()} of
									{_, false} ->
										undefined;
									{{{true, _}, _}, true} ->
										false;
									_ ->
										prometheus_counter:inc(
												block_announcement_missing_chunks),
										true
								end
						end,
					prometheus_counter:inc(block_announcement_reported_transactions,
							length(Prefixes)),
					prometheus_counter:inc(block_announcement_missing_transactions,
							length(Indices)),
					Response = #block_announcement_response{ missing_chunk = MissingChunk,
							missing_tx_indices = Indices, missing_chunk2 = MissingChunk2 },
					{200, #{}, ar_serialize:block_announcement_response_to_binary(Response),
							Req}
			end
	end.

collect_missing_tx_indices(Prefixes) ->
	collect_missing_tx_indices(Prefixes, [], 0).

collect_missing_tx_indices([], Indices, _N) ->
	lists:reverse(Indices);
collect_missing_tx_indices([Prefix | Prefixes], Indices, N) ->
	case ets:member(tx_prefixes, Prefix) of
		false ->
			collect_missing_tx_indices(Prefixes, [N | Indices], N + 1);
		true ->
			collect_missing_tx_indices(Prefixes, Indices, N + 1)
	end.

post_block(request, {Req, Pid, Encoding}, ReceiveTimestamp) ->
	Peer = ar_http_util:arweave_peer(Req),
	case ar_blacklist_middleware:is_peer_banned(Peer) of
		not_banned ->
			post_block(check_joined, Peer, {Req, Pid, Encoding}, ReceiveTimestamp);
		banned ->
			{403, #{}, <<"IP address blocked due to previous request.">>, Req}
	end.

post_block(check_joined, Peer, {Req, Pid, Encoding}, ReceiveTimestamp) ->
	case ar_node:is_joined() of
		true ->
			ConfirmedHeight = ar_node:get_height() - ?STORE_BLOCKS_BEHIND_CURRENT,
			case {Encoding, ConfirmedHeight >= ar_fork:height_2_6()} of
				{json, true} ->
					%% We gesticulate it explicitly here that POST /block is not
					%% supported after the 2.6 fork. However, this check is not strictly
					%% necessary because ar_serialize:json_struct_to_block/1 fails
					%% unless the block height is smaller than the fork 2.6 height.
					{400, #{}, <<>>, Req};
				_ ->
					post_block(check_block_hash_header, Peer, {Req, Pid, Encoding},
							ReceiveTimestamp)
			end;
		false ->
			%% The node is not ready to validate and accept blocks.
			%% If the network adopts this block, ar_poller will catch up.
			{503, #{}, <<"Not joined.">>, Req}
	end;
post_block(check_block_hash_header, Peer, {Req, Pid, Encoding}, ReceiveTimestamp) ->
	case cowboy_req:header(<<"chivesweave-block-hash">>, Req, not_set) of
		not_set ->
			post_block(read_body, Peer, {Req, Pid, Encoding}, ReceiveTimestamp);
		EncodedBH ->
			case ar_util:safe_decode(EncodedBH) of
				{ok, BH} when byte_size(BH) =< 48 ->
					case ar_ignore_registry:member(BH) of
						true ->
							{208, #{}, <<"Block already processed.">>, Req};
						false ->
							post_block(read_body, Peer, {Req, Pid, Encoding},
									ReceiveTimestamp)
					end;
				_ ->
					post_block(read_body, Peer, {Req, Pid, Encoding}, ReceiveTimestamp)
			end
	end;
post_block(read_body, Peer, {Req, Pid, Encoding}, ReceiveTimestamp) ->
	case read_complete_body(Req, Pid) of
		{ok, Body, Req2} ->
			case decode_block(Body, Encoding) of
				{error, _} ->
					{400, #{}, <<"Invalid block.">>, Req2};
				{ok, BShadow} ->
					post_block(check_transactions_are_present, {BShadow, Peer}, Req2,
							ReceiveTimestamp)
			end;
		{error, body_size_too_large} ->
			{413, #{}, <<"Payload too large">>, Req};
		{error, timeout} ->
			{503, #{}, jiffy:encode(#{ error => timeout }), Req}
	end;
post_block(check_transactions_are_present, {BShadow, Peer}, Req, ReceiveTimestamp) ->
	case erlang:get(post_block2) of
		true ->
			case get_missing_tx_identifiers(BShadow#block.txs) of
				[] ->
					post_block(enqueue_block, {BShadow, Peer}, Req, ReceiveTimestamp);
				{error, tx_list_too_long} ->
					{400, #{}, <<>>, Req};
				MissingTXIDs ->
					{418, #{}, encode_txids(MissingTXIDs), Req}
			end;
		_ -> % POST /block; do not reject for backwards-compatibility
			post_block(enqueue_block, {BShadow, Peer}, Req, ReceiveTimestamp)
	end;
post_block(enqueue_block, {B, Peer}, Req, ReceiveTimestamp) ->
	B2 =
		case B#block.height >= ar_fork:height_2_6() of
			true ->
				B;
			false ->
				case cowboy_req:header(<<"chivesweave-recall-byte">>, Req, not_set) of
					not_set ->
						B;
					ByteBin ->
						case catch binary_to_integer(ByteBin) of
							RecallByte when is_integer(RecallByte) ->
								B#block{ recall_byte = RecallByte };
							_ ->
								B
						end
				end
		end,
	?LOG_INFO([{event, received_block}, {block, ar_util:encode(B#block.indep_hash)}]),
	BodyReadTime = ar_http_req:body_read_time(Req),
	case ar_block_pre_validator:pre_validate(B2, Peer, ReceiveTimestamp) of
		ok ->
			ar_peers:rate_gossiped_data(Peer, block,
				erlang:convert_time_unit(BodyReadTime, native, microsecond),
				byte_size(term_to_binary(B)));
		_ ->
			ok
	end,
	{200, #{}, <<"OK">>, Req}.

encode_txids([]) ->
	<<>>;
encode_txids([TXID | TXIDs]) ->
	<< TXID/binary, (encode_txids(TXIDs))/binary >>.

get_missing_tx_identifiers(TXIDs) ->
	get_missing_tx_identifiers(TXIDs, [], 0).

get_missing_tx_identifiers([], MissingTXIDs, _N) ->
	MissingTXIDs;
get_missing_tx_identifiers([_ | _], _, N) when N == ?BLOCK_TX_COUNT_LIMIT ->
	{error, tx_list_too_long};
get_missing_tx_identifiers([#tx{} | TXIDs], MissingTXIDs, N) ->
	get_missing_tx_identifiers(TXIDs, MissingTXIDs, N + 1);
get_missing_tx_identifiers([TXID | TXIDs], MissingTXIDs, N) ->
	case ar_node_worker:is_mempool_or_block_cache_tx(TXID) of
		true ->
			get_missing_tx_identifiers(TXIDs, MissingTXIDs, N + 1);
		false ->
			get_missing_tx_identifiers(TXIDs, [TXID | MissingTXIDs], N + 1)
	end.

decode_recent_hash_list(<<>>) ->
	{ok, []};
decode_recent_hash_list(<< H:48/binary, Rest/binary >>) ->
	case decode_recent_hash_list(Rest) of
		error ->
			error;
		{ok, HL} ->
			{ok, [H | HL]}
	end;
decode_recent_hash_list(_Rest) ->
	error.

get_recent_hash_list_diff([H | HL], BlockTXPairs) ->
	case lists:dropwhile(fun({BH, _TXIDs}) -> BH /= H end, BlockTXPairs) of
		[] ->
			get_recent_hash_list_diff(HL, BlockTXPairs);
		Tail ->
			get_recent_hash_list_diff(HL, tl(Tail), H)
	end;
get_recent_hash_list_diff([], _BlockTXPairs) ->
	no_intersection.

get_recent_hash_list_diff([H | HL], [{H, _SizeTaggedTXs} | BlockTXPairs], _PrevH) ->
	get_recent_hash_list_diff(HL, BlockTXPairs, H);
get_recent_hash_list_diff(_HL, BlockTXPairs, PrevH) ->
	<< PrevH/binary, (get_recent_hash_list_diff(BlockTXPairs))/binary >>.

get_recent_hash_list_diff([{H, TXIDs} | BlockTXPairs]) ->
	Len = length(TXIDs),
	<< H:48/binary, Len:16,
			(iolist_to_binary([TXID || TXID <- TXIDs]))/binary,
			(get_recent_hash_list_diff(BlockTXPairs))/binary >>;
get_recent_hash_list_diff([]) ->
	<<>>.

get_total_supply(RootHash, Cursor, Sum, Denomination) ->
	{ok, {NextCursor, Range}} = ar_wallets:get_chunk(RootHash, Cursor),
	RangeSum = get_balance_sum(Range, Denomination),
	case NextCursor of
		last ->
			Sum + RangeSum;
		_ ->
			get_total_supply(RootHash, NextCursor, Sum + RangeSum, Denomination)
	end.

get_balance_sum([{_, {Balance, _LastTX}} | Range], BlockDenomination) ->
	ar_pricing:redenominate(Balance, 1, BlockDenomination)
			+ get_balance_sum(Range, BlockDenomination);
get_balance_sum([{_, {Balance, _LastTX, Denomination, _MiningPermission}} | Range],
		BlockDenomination) ->
	ar_pricing:redenominate(Balance, Denomination, BlockDenomination)
			+ get_balance_sum(Range, BlockDenomination);
get_balance_sum([], _BlockDenomination) ->
	0.

%% Return the block hash list associated with a block.
process_request(get_block, [Type, ID, <<"hash_list">>], Req) ->
	case find_block(Type, ID) of
		{error, height_not_integer} ->
			{400, #{}, jiffy:encode(#{ error => size_must_be_an_integer }), Req};
		unavailable ->
			{404, #{}, <<"Not Found.">>, Req};
		B ->
			ok = ar_semaphore:acquire(get_block_index, infinity),
			case ar_node:get_height() >= ar_fork:height_2_6() of
				true ->
					{400, #{}, jiffy:encode(#{ error => not_supported_since_fork_2_6 }), Req};
				false ->
					CurrentBI = ar_node:get_block_index(),
					HL = ar_block:generate_hash_list_for_block(B#block.indep_hash, CurrentBI),
					{200, #{}, ar_serialize:jsonify(lists:map(fun ar_util:encode/1, HL)), Req}
			end
	end;
%% @doc Return the wallet list associated with a block.
process_request(get_block, [Type, ID, <<"wallet_list">>], Req) ->
	case find_block(Type, ID) of
		{error, height_not_integer} ->
			{400, #{}, jiffy:encode(#{ error => size_must_be_an_integer }), Req};
		unavailable ->
			{404, #{}, <<"Not Found.">>, Req};
		B ->
			{ok, Config} = application:get_env(chivesweave, config),
			case {B#block.height >= ar_fork:height_2_2(),
					lists:member(serve_wallet_lists, Config#config.enable)} of
				{true, false} ->
					{400, #{},
						jiffy:encode(#{ error => does_not_serve_blocks_after_2_2_fork }),
						Req};
				{true, _} ->
					ok = ar_semaphore:acquire(get_wallet_list, infinity),
					case ar_storage:read_wallet_list(B#block.wallet_list) of
						{ok, Tree} ->
							{200, #{}, ar_serialize:jsonify(
								ar_serialize:wallet_list_to_json_struct(
									B#block.reward_addr, false, Tree
								)), Req};
						_ ->
							{404, #{}, <<"Block not found.">>, Req}
					end;
				_ ->
					WLFilepath = ar_storage:wallet_list_filepath(B#block.wallet_list),
					case filelib:is_file(WLFilepath) of
						true ->
							{200, #{}, sendfile(WLFilepath), Req};
						false ->
							{404, #{}, <<"Block not found.">>, Req}
					end
			end
	end;

%% Return a requested field of a given block.
%% GET request to endpoint /block/hash/{hash|height}/{field}.
%%
%% field :: nonce | previous_block | timestamp | last_retarget | diff | height | hash |
%%			indep_hash | txs | hash_list | wallet_list | reward_addr | tags | reward_pool
process_request(get_block, [Type, ID, Field], Req) ->
	{ok, Config} = application:get_env(chivesweave, config),
	case lists:member(subfield_queries, Config#config.enable) of
		true ->
			case find_block(Type, ID) of
				{error, height_not_integer} ->
					{400, #{}, jiffy:encode(#{ error => size_must_be_an_integer }), Req};
				unavailable ->
					{404, #{}, <<"Not Found.">>, Req};
				B ->
					{BLOCKJSON} = ar_serialize:block_to_json_struct(B),
					case catch list_to_existing_atom(binary_to_list(Field)) of
						{'EXIT', _} ->
							{404, #{}, <<"Not Found.">>, Req};
						Atom ->
							case lists:keyfind(Atom, 1, BLOCKJSON) of
								{_, Res} ->
									Result = block_field_to_string(Field, Res),
									{200, #{}, Result, Req};
								_ ->
									{404, #{}, <<"Not Found.">>, Req}
							end
					end
			end;
		_ ->
			{421, #{}, <<"Subfield block querying is disabled on this node.">>, Req}
	end.

handle_get_block_wallet_balance(EncodedHeight, EncodedAddr, Req) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			CurrentHeight = ar_node:get_height(),
			try binary_to_integer(EncodedHeight) of
				Height when Height < 0 ->
					{400, #{}, jiffy:encode(#{ error => invalid_height }), Req};
				Height when Height > CurrentHeight ->
					{404, #{}, jiffy:encode(#{ error => block_not_found }), Req};
				Height ->
					case ar_block_index:get_element_by_height(Height) of
						not_found ->
							{404, #{}, jiffy:encode(#{ error => block_not_found }), Req};
						{H, _, _} ->
							B =
								case ar_block_cache:get(block_cache, H) of
									not_found ->
										ar_storage:read_block(H);
									B2 ->
										B2
								end,
							case B of
								unavailable ->
									{404, #{}, jiffy:encode(#{ error => block_not_found }),
											Req};
								#block{ wallet_list = RootHash } ->
									case ar_util:safe_decode(EncodedAddr) of
										{ok, Addr} ->
											handle_get_block_wallet_balance2(Addr, RootHash,
													Req);
										{error, invalid} ->
											{400, #{}, jiffy:encode(#{
													error => invalid_address }), Req}
									end
							end
					end
			catch _:_ ->
				{400, #{}, jiffy:encode(#{ error => invalid_height }), Req}
			end
	end.

handle_get_block_wallet_balance2(Addr, RootHash, Req) ->
	case ar_wallets:get_balance(RootHash, Addr) of
		{error, not_found} ->
			handle_get_block_wallet_balance3(Addr, RootHash, Req);
		Balance when is_integer(Balance) ->
			{200, #{}, integer_to_binary(Balance), Req};
		_Error ->
			{500, #{}, <<>>, Req}
	end.

handle_get_block_wallet_balance3(Addr, RootHash, Req) ->
	case ar_storage:read_account(Addr, RootHash) of
		not_found ->
			{404, #{}, jiffy:encode(#{ error => account_data_not_found }), Req};
		{Balance, _LastTX} ->
			{200, #{}, integer_to_binary(Balance), Req};
		{Balance, _LastTX, _Denomination, _MiningPermission} ->
			{200, #{}, integer_to_binary(Balance), Req}
	end.

process_get_wallet_list_chunk(EncodedRootHash, EncodedCursor, Req) ->
	DecodeCursorResult =
		case EncodedCursor of
			first ->
				{ok, first};
			_ ->
				ar_util:safe_decode(EncodedCursor)
		end,
	case {ar_util:safe_decode(EncodedRootHash), DecodeCursorResult} of
		{{error, invalid}, _} ->
			{400, #{}, <<"Invalid root hash.">>, Req};
		{_, {error, invalid}} ->
			{400, #{}, <<"Invalid root hash.">>, Req};
		{{ok, RootHash}, {ok, Cursor}} ->
			case ar_wallets:get_chunk(RootHash, Cursor) of
				{ok, {NextCursor, Wallets}} ->
					SerializeFn = case cowboy_req:header(<<"content-type">>, Req) of
						<<"application/json">> -> fun wallet_list_chunk_to_json/1;
						<<"application/etf">> -> fun erlang:term_to_binary/1;
						_ -> fun erlang:term_to_binary/1
					end,
					Reply = SerializeFn(#{ next_cursor => NextCursor, wallets => Wallets }),
					{200, #{}, Reply, Req};
				{error, root_hash_not_found} ->
					{404, #{}, <<"Root hash not found.">>, Req}
			end
	end.

wallet_list_chunk_to_json(#{ next_cursor := NextCursor, wallets := Wallets }) ->
	SerializedWallets =
		lists:map(
			fun({Addr, Value}) ->
				ar_serialize:wallet_to_json_struct(Addr, Value)
			end,
			Wallets
		),
	case NextCursor of
		last ->
			jiffy:encode(#{ wallets => SerializedWallets });
		Cursor when is_binary(Cursor) ->
			jiffy:encode(#{
				next_cursor => ar_util:encode(Cursor),
				wallets => SerializedWallets
			})
	end.

get_reward_sum(Addr, L, Denomination) ->
	get_reward_sum(Addr, L, Denomination, ?REWARD_HISTORY_BLOCKS, 0).

get_reward_sum(_Addr, _L, _Denomination, 0, Sum) ->
	Sum;
get_reward_sum(_Addr, [], _Denomination, _N, Sum) ->
	Sum;
get_reward_sum(Addr, [{Addr, _HashRate, Reward, RDenomination} | L], Denomination, N, Sum) ->
	Reward2 = ar_pricing:redenominate(Reward, RDenomination, Denomination),
	get_reward_sum(Addr, L, Denomination, N - 1, Sum + Reward2);
get_reward_sum(Addr, [_ | L], Denomination, N, Sum) ->
	get_reward_sum(Addr, L, Denomination, N - 1, Sum).

%% @doc Find a block, given a type and a specifier.
find_block(<<"height">>, RawHeight) ->
	case catch binary_to_integer(RawHeight) of
		{'EXIT', _} ->
			{error, height_not_integer};
		Height ->
			case ar_block_index:get_element_by_height(Height) of
				not_found ->
					unavailable;
				{H, _, _} ->
					ar_storage:read_block(H)
			end
	end;
find_block(<<"hash">>, ID) ->
	case ar_util:safe_decode(ID) of
		{ok, H} ->
			ar_storage:read_block(H);
		_ ->
			unavailable
	end.

is_tx_already_processed(TXID) ->
	case ar_ignore_registry:member(TXID) of
		true ->
			true;
		false ->
			ar_mempool:has_tx(TXID)
	end.

post_tx_parse_id({Req, Pid, Encoding}) ->
	post_tx_parse_id(check_header, {Req, Pid, Encoding}).

post_tx_parse_id(check_header, {Req, Pid, Encoding}) ->
	case cowboy_req:header(<<"chivesweave-tx-id">>, Req, not_set) of
		not_set ->
			post_tx_parse_id(read_body, {not_set, Req, Pid, Encoding});
		EncodedTXID ->
			case ar_util:safe_decode(EncodedTXID) of
				{ok, TXID} when byte_size(TXID) =< 32 ->
					post_tx_parse_id(check_ignore_list, {TXID, Req, Pid, Encoding});
				_ ->
					{error, invalid_hash, Req}
			end
	end;
post_tx_parse_id(check_ignore_list, {TXID, Req, Pid, Encoding}) ->
	case is_tx_already_processed(TXID) of
		true ->
			{error, tx_already_processed, TXID, Req};
		false ->
			ar_ignore_registry:add_temporary(TXID, 5000),
			post_tx_parse_id(read_body, {TXID, Req, Pid, Encoding})
	end;
post_tx_parse_id(read_body, {TXID, Req, Pid, Encoding}) ->
	case read_complete_body(Req, Pid) of
		{ok, Body, Req2} ->
			case Encoding of
				json ->
					post_tx_parse_id(parse_json, {TXID, Req2, Body});
				binary ->
					post_tx_parse_id(parse_binary, {TXID, Req2, Body})
			end;
		{error, body_size_too_large} ->
			{error, body_size_too_large, Req};
		{error, timeout} ->
			{error, timeout}
	end;
post_tx_parse_id(parse_json, {TXID, Req, Body}) ->
	case catch ar_serialize:json_struct_to_tx(Body) of
		{'EXIT', _} ->
			case TXID of
				not_set ->
					noop;
				_ ->
					ar_ignore_registry:remove_temporary(TXID)
			end,
			{error, invalid_json, Req};
		{error, invalid_signature_type} ->
            case TXID of
                not_set ->
                    noop;
                _ ->
                    ar_ignore_registry:remove_temporary(TXID),
					ar_tx_db:put_error_codes(TXID, [<<"invalid_signature_type">>])
            end,
            {error, invalid_signature_type, Req};
		{error, _} ->
			case TXID of
				not_set ->
					noop;
				_ ->
					ar_ignore_registry:remove_temporary(TXID)
			end,
			{error, invalid_json, Req};
		TX ->
			post_tx_parse_id(verify_id_match, {TXID, Req, TX})
	end;
post_tx_parse_id(parse_binary, {TXID, Req, Body}) ->
	case catch ar_serialize:binary_to_tx(Body) of
		{'EXIT', _} ->
			case TXID of
				not_set ->
					noop;
				_ ->
					ar_ignore_registry:remove_temporary(TXID)
			end,
			{error, invalid_json, Req};
		{error, _} ->
			case TXID of
				not_set ->
					noop;
				_ ->
					ar_ignore_registry:remove_temporary(TXID)
			end,
			{error, invalid_json, Req};
		{ok, TX} ->
			post_tx_parse_id(verify_id_match, {TXID, Req, TX})
	end;
post_tx_parse_id(verify_id_match, {MaybeTXID, Req, TX}) ->
	TXID = TX#tx.id,
	case MaybeTXID of
		TXID ->
			{ok, TX, Req};
		MaybeNotSet ->
			case MaybeNotSet of
				not_set ->
					noop;
				MismatchingTXID ->
					ar_ignore_registry:remove_temporary(MismatchingTXID)
			end,
			case byte_size(TXID) > 32 of
				true ->
					{error, invalid_hash, Req};
				false ->
					case is_tx_already_processed(TXID) of
						true ->
							{error, tx_already_processed, TXID, Req};
						false ->
							ar_ignore_registry:add_temporary(TXID, 5000),
							{ok, TX, Req}
					end
			end
	end.

handle_post_vdf(Req, Pid) ->
	Peer = ar_http_util:arweave_peer(Req),
	case ets:member(ar_peers, {vdf_server_peer, Peer}) of
		false ->
			{400, #{}, <<>>, Req};
		true ->
			handle_post_vdf2(Req, Pid, Peer)
	end.

handle_post_vdf2(Req, Pid, Peer) ->
	case ar_config:pull_from_remote_vdf_server() of
		true ->
			%% We are pulling the updates - tell the server not to push them.
			Response = #nonce_limiter_update_response{ postpone = 120 },
			Bin = ar_serialize:nonce_limiter_update_response_to_binary(Response),
			{202, #{}, Bin, Req};
		false ->
			handle_post_vdf3(Req, Pid, Peer)
	end.

handle_post_vdf3(Req, Pid, Peer) ->
	case read_complete_body(Req, Pid) of
		{ok, Body, Req2} ->
			{ok, Update} = ar_serialize:binary_to_nonce_limiter_update(Body),
			case ar_nonce_limiter:apply_external_update(Update, Peer) of
				ok ->
					{200, #{}, <<>>, Req2};
				#nonce_limiter_update_response{} = Response ->
					Bin = ar_serialize:nonce_limiter_update_response_to_binary(Response),
					{202, #{}, Bin, Req2}
			end;
		{error, body_size_too_large} ->
			{413, #{}, <<"Payload too large">>, Req};
		{error, timeout} ->
			{503, #{}, jiffy:encode(#{ error => timeout }), Req}
	end.

handle_get_vdf(Req, Call) ->
	Peer = ar_http_util:arweave_peer(Req),
	case ets:lookup(ar_peers, {vdf_client_peer, Peer}) of
		[] ->
			{400, #{}, jiffy:encode(#{ error => not_our_vdf_client }), Req};
		[{_, _RawPeer}] ->
			handle_get_vdf2(Req, Call)
	end.

handle_get_vdf2(Req, Call) ->
	case gen_server:call(ar_nonce_limiter_server, Call) of
		not_found ->
			{404, #{}, <<>>, Req};
		Update ->
			{200, #{}, ar_serialize:nonce_limiter_update_to_binary(Update), Req}
	end.

read_complete_body(Req, Pid) ->
	read_complete_body(Req, Pid, ?MAX_BODY_SIZE).

read_complete_body(Req, Pid, SizeLimit) ->
	Pid ! {read_complete_body, self(), Req, SizeLimit},
	receive
		{read_complete_body, {'EXIT', timeout}} ->
			?LOG_WARNING([{event, body_read_cowboy_timeout}, {method, cowboy_req:method(Req)},
					{path, cowboy_req:path(Req)}]),
			{error, timeout};
		{read_complete_body, Term} ->
			Term
	end.

read_body_chunk(Req, Pid, Size, Timeout) ->
	Pid ! {read_body_chunk, self(), Req, Size, Timeout},
	receive
		{read_body_chunk, {'EXIT', timeout}} ->
			Peer = ar_http_util:arweave_peer(Req),
			?LOG_DEBUG([{event, body_read_cowboy_timeout}, {method, cowboy_req:method(Req)},
					{path, cowboy_req:path(Req)}, {peer, ar_util:format_peer(Peer)}]),
			{error, timeout};
		{read_body_chunk, Term} ->
			Term
	after Timeout ->
		Peer = ar_http_util:arweave_peer(Req),
		?LOG_DEBUG([{event, body_read_timeout}, {method, cowboy_req:method(Req)},
				{path, cowboy_req:path(Req)}, {peer, ar_util:format_peer(Peer)}]),
		{error, timeout}
	end.
