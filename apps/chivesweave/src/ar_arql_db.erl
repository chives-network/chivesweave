-module(ar_arql_db).
-behaviour(gen_server).

-export([start_link/0, select_tx_by_id/1, select_txs_by/1, select_block_by_tx_id/1,
		select_tags_by_tx_id/1, eval_legacy_arql/1, insert_full_block/1, insert_full_block/2,
		insert_block/1, insert_tx/4, insert_tx/5, insert_tx/2,
		select_address_range/2, select_address_total/0,
		select_address_referee_range/3, select_address_referee_total/1,
		select_address_agent_range/2, select_address_agent_total/0,
		select_address_profile_range/2, select_address_profile_total/0, select_address_profile_my/1,
		select_transaction_range/2, select_transaction_total/0, 
		select_transaction_range_filter/3, select_transaction_total_filter/1, 
		select_transaction_range_filetype_address/4, select_transaction_total_filetype_address/2, 
		select_transaction_range_folder_address/4, select_transaction_total_folder_address/2, 
		select_transaction_range_label_address/4, select_transaction_total_label_address/2, 
		select_transaction_range_star_address/4, select_transaction_total_star_address/2, 
		select_transaction_range_filter_address_filename/5, select_transaction_total_filter_address_filename/3, 
		select_transaction_range_filter_filename/4, select_transaction_total_filter_filename/2,
		select_transaction_range_bundletxparse/3, select_transaction_total_bundletxparse/1,
		select_transaction_group_label_address/1,
		select_folder_address/1,
		update_tx_label/5, update_tx_folder/5, update_tx_star/5, update_tx_public/5, update_tx_bundletxparse/2,
		update_address_referee/2, update_address_agent/3, update_address_profile/5, 
		update_address_blockinfo/4
		]).

-export([init/1, handle_call/3, handle_cast/2, terminate/2]).

-include_lib("chivesweave/include/ar.hrl").
-include_lib("chivesweave/include/ar_config.hrl").

%% Timeout passed to gen_server:call when running SELECTs.
%% Set to 5s.
-define(SELECT_TIMEOUT, 5000).

%% Time to wait for the NIF thread to send back the query results.
%% Set to 4.5s.
-define(DRIVER_TIMEOUT, 4500).

%% Time to wait until an operation (bind, step, etc) required for inserting an entity is complete.
-define(INSERT_STEP_TIMEOUT, 60000).

%% Time to wait until an operation (bind, step, etc) required for inserting an entity is complete.
-define(UPDATE_STEP_TIMEOUT, 60000).

%% The migration name should follow the format YYYYMMDDHHMMSS_human_readable_name
%% where the date is picked at the time of naming the migration.
-define(CREATE_MIGRATION_TABLE_SQL, "
CREATE TABLE migration (
	name TEXT PRIMARY KEY,
	date TEXT
);

CREATE INDEX idx_migration_name ON migration (name);
CREATE INDEX idx_migration_date ON migration (date);
").

-define(CREATE_TABLES_SQL, "
CREATE TABLE address (
	address TEXT PRIMARY KEY,
	balance INTEGER,
	txs INTEGER,
	sent INTEGER,
	received INTEGER,

	lastblock INTEGER,
	timestamp INTEGER,
	profile TEXT,
	chivesDrive INTEGER DEFAULT 0,
	chivesEmail INTEGER DEFAULT 0,
	
	chivesBlog INTEGER DEFAULT 0,
	chivesMessage INTEGER DEFAULT 0,
	chivesForum INTEGER DEFAULT 0,
	chivesDb INTEGER DEFAULT 0,
	agent INTEGER DEFAULT 0,

	referee TEXT,
	last_tx_action TEXT
);

CREATE TABLE block (
	indep_hash TEXT PRIMARY KEY,
	previous_block TEXT,
	height INTEGER,
	timestamp INTEGER
);

CREATE TABLE tx (
	id TEXT PRIMARY KEY,
	block_indep_hash TEXT,
	last_tx TEXT,
	owner TEXT,
	from_address TEXT,
	target TEXT,
	quantity INTEGER,
	signature TEXT,
	reward INTEGER,
	timestamp INTEGER,
	block_height INTEGER,
	data_size INTEGER,
	bundleid TEXT,
	item_name TEXT,
	item_type TEXT,
	item_parent TEXT,
	content_type TEXT,
	item_hash TEXT,
	item_summary TEXT,
	item_star TEXT,
	item_label TEXT,
	item_download TEXT,
	item_language TEXT,
	item_pages TEXT,
	is_encrypt TEXT,
	is_public TEXT,
	entity_type TEXT,
	app_name TEXT,
	app_version TEXT,
	app_instance TEXT,
	item_node_label TEXT,
	item_node_group TEXT,
	item_node_star TEXT,
	item_node_hot TEXT,
	item_node_delete TEXT,
	last_tx_action TEXT,
	bundleTxParse INTEGER
);

CREATE TABLE tag (
	tx_id TEXT,
	name TEXT,
	value TEXT
);
").

-define(CREATE_INDEXES_SQL, "
CREATE INDEX idx_block_height ON block (height);
CREATE INDEX idx_block_timestamp ON block (timestamp);

CREATE INDEX idx_tx_block_indep_hash ON tx (block_indep_hash);
CREATE INDEX idx_tx_from_address ON tx (from_address);
CREATE INDEX idx_tx_target ON tx (target);
CREATE INDEX idx_tx_timestamp ON tx (timestamp);
CREATE INDEX idx_tx_block_height ON tx (block_height);
CREATE INDEX idx_tx_bundleid ON tx (bundleid);
CREATE INDEX idx_tx_item_name ON tx (item_name);
CREATE INDEX idx_tx_item_type ON tx (item_type);
CREATE INDEX idx_tx_item_parent ON tx (item_parent);
CREATE INDEX idx_tx_content_type ON tx (content_type);
CREATE INDEX idx_tx_item_hash ON tx (item_hash);
CREATE INDEX idx_tx_is_encrypt ON tx (is_encrypt);
CREATE INDEX idx_tx_is_public ON tx (is_public);
CREATE INDEX idx_tx_entity_type ON tx (entity_type);
CREATE INDEX idx_tx_app_name ON tx (app_name);
CREATE INDEX idx_tx_app_version ON tx (app_version);
CREATE INDEX idx_tx_app_instance ON tx (app_instance);

CREATE INDEX idx_tx_item_label ON tx (item_label);
CREATE INDEX idx_tx_item_star ON tx (item_star);
CREATE INDEX idx_tx_item_language ON tx (item_language);
CREATE INDEX idx_tx_item_node_label ON tx (item_node_label);
CREATE INDEX idx_tx_item_node_group ON tx (item_node_group);
CREATE INDEX idx_tx_item_node_star ON tx (item_node_star);
CREATE INDEX idx_tx_item_node_hot ON tx (item_node_hot);
CREATE INDEX idx_tx_item_node_delete ON tx (item_node_delete);
CREATE INDEX idx_tx_bundleTxParse ON tx (bundleTxParse);
CREATE INDEX idx_tx_last_tx_action ON tx (last_tx_action);

CREATE INDEX idx_tag_tx_id ON tag (tx_id);
CREATE INDEX idx_tag_name ON tag (name);
CREATE INDEX idx_tag_name_value ON tag (name, value);

CREATE INDEX idx_address_lastblock ON address (lastblock);
CREATE INDEX idx_address_timestamp ON address (timestamp);
CREATE INDEX idx_address_profile ON address (profile);
CREATE INDEX idx_address_chivesDrive ON address (chivesDrive);
CREATE INDEX idx_address_chivesEmail ON address (chivesEmail);
CREATE INDEX idx_address_chivesBlog ON address (chivesBlog);
CREATE INDEX idx_address_chivesMessage ON address (chivesMessage);
CREATE INDEX idx_address_chivesForum ON address (chivesForum);
CREATE INDEX idx_address_chivesDb ON address (chivesDb);
CREATE INDEX idx_address_agent ON address (agent);
CREATE INDEX idx_address_referee ON address (referee);
CREATE INDEX idx_address_last_tx_action ON address (last_tx_action);
").

-define(DROP_INDEXES_SQL, "
DROP INDEX idx_block_height;
DROP INDEX idx_block_timestamp;

DROP INDEX idx_tx_block_indep_hash;
DROP INDEX idx_tx_from_address;
DROP INDEX idx_tx_timestamp;
DROP INDEX idx_tx_block_height;
DROP INDEX idx_tx_bundleid;
DROP INDEX idx_tx_item_name;
DROP INDEX idx_tx_content_type;
DROP INDEX idx_tx_app_name;
DROP INDEX idx_tx_app_instance;

DROP INDEX idx_tx_item_label;
DROP INDEX idx_tx_item_star;
DROP INDEX idx_tx_item_language;
DROP INDEX idx_tx_item_node_label;
DROP INDEX idx_tx_item_node_group;
DROP INDEX idx_tx_item_node_star;
DROP INDEX idx_tx_item_node_hot;
DROP INDEX idx_tx_item_node_delete;
DROP INDEX idx_tx_bundleTxParse;
DROP INDEX idx_tx_last_tx_action;

DROP INDEX idx_tag_tx_id;
DROP INDEX idx_tag_name;
DROP INDEX idx_tag_name_value;

DROP INDEX idx_address_lastblock;
DROP INDEX idx_address_timestamp;
DROP INDEX idx_address_profile;
DROP INDEX idx_address_chivesDrive;
DROP INDEX idx_address_chivesEmail;
DROP INDEX idx_address_chivesBlog;
DROP INDEX idx_address_chivesMessage;
DROP INDEX idx_address_chivesForum;
DROP INDEX idx_address_chivesDb;
DROP INDEX idx_address_agent;
DROP INDEX idx_address_referee;
DROP INDEX idx_address_last_tx_action;

").

-define(INSERT_BLOCK_SQL, "INSERT OR REPLACE INTO block VALUES (?, ?, ?, ?)").
-define(INSERT_TX_SQL, "INSERT OR IGNORE INTO tx VALUES (?, ?, ?, ?, ?,  ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?,  ?, ?)").
-define(INSERT_TAG_SQL, "INSERT OR REPLACE INTO tag VALUES (?, ?, ?)").
-define(SELECT_TX_BY_ID_SQL, "SELECT * FROM tx WHERE id = ?").

-define(SELECT_BLOCK_BY_TX_ID_SQL, "
	SELECT block.* FROM block
	JOIN tx on tx.block_indep_hash = block.indep_hash
	WHERE tx.id = ?
").

-define(SELECT_TAGS_BY_TX_ID_SQL, "SELECT * FROM tag WHERE tx_id = ?").

-define(INSERT_ADDRESS_SQL, "INSERT OR IGNORE INTO address (address, balance, lastblock, timestamp) VALUES (?, ?, ?, ?)").
-define(SELECT_ADDRESS_RANGE_SQL, "SELECT * FROM address order by balance desc LIMIT ? OFFSET ?").
-define(SELECT_ADDRESS_TOTAL, "SELECT COUNT(*) AS NUM FROM address").

-define(SELECT_ADDRESS_REFEREE_RANGE_SQL, "SELECT * FROM address where referee = ? order by balance desc LIMIT ? OFFSET ?").
-define(SELECT_ADDRESS_REFEREE_TOTAL, "SELECT COUNT(*) AS NUM FROM address where referee = ? ").
-define(UPDATE_ADDRESS_REFEREE_SQL, "update address set referee = ? where address = ? and referee is null").

-define(SELECT_ADDRESS_AGENT_RANGE_SQL, "SELECT * FROM address where Agent != '0' order by balance desc LIMIT ? OFFSET ?").
-define(SELECT_ADDRESS_AGENT_TOTAL, "SELECT COUNT(*) AS NUM FROM address where Agent != '0' ").
-define(UPDATE_ADDRESS_AGENT_SQL, "update address set Agent = ?, timestamp = ? where address = ? and Agent = '0'").

-define(SELECT_ADDRESS_PROFILE_MY_SQL, "SELECT * FROM address where address = ?").
-define(SELECT_ADDRESS_PROFILE_RANGE_SQL, "SELECT * FROM address where profile != null and profile != '' order by balance desc LIMIT ? OFFSET ?").
-define(SELECT_ADDRESS_PROFILE_TOTAL, "SELECT COUNT(*) AS NUM FROM address where profile != null and profile != '' ").
-define(UPDATE_ADDRESS_PROFILE_SQL, "update address set profile = ?, timestamp = ?, last_tx_action = ? where (last_tx_action is null and address = ?) or (address != last_tx_action and address = ? and last_tx_action = ?) or (address != last_tx_action and address = ? and last_tx_action = ?)").

-define(UPDATE_ADDRESS_BLOCKINFO_SQL, "update address set balance = ?, lastblock = ?, timestamp = ? where address = ? and timestamp <= ?").

-define(SELECT_TRANSACTION_RANGE_SQL, "SELECT * FROM tx where is_encrypt = '' order by timestamp desc LIMIT ? OFFSET ?").
-define(SELECT_TRANSACTION_RANGE_FILTER_SQL, "SELECT * FROM tx where item_type = ? and is_encrypt = '' order by timestamp desc LIMIT ? OFFSET ?").

-define(SELECT_TRANSACTION_TOTAL, "SELECT COUNT(*) AS NUM FROM tx where is_encrypt = '' and entity_type = 'File'").
-define(SELECT_TRANSACTION_TOTAL_FILTER, "SELECT COUNT(*) AS NUM FROM tx where item_type = ? and is_encrypt = '' and entity_type = 'File'").

-define(SELECT_TRANSACTION_RANGE_FILTER_ADDRESS_SQL, "SELECT * FROM tx where item_type = ? and is_encrypt = '' and entity_type = 'File' and from_address = ? order by timestamp desc LIMIT ? OFFSET ?").
-define(SELECT_TRANSACTION_TOTAL_FILTER_ADDRESS, "SELECT COUNT(*) AS NUM FROM tx where item_type = ? and is_encrypt = '' and entity_type = 'File' and from_address = ?").

-define(SELECT_TRANSACTION_RANGE_FILTER_ADDRESS_FILENAME_SQL, "SELECT * FROM tx where item_type = ? and is_encrypt = '' and entity_type = 'File' and from_address = ? and item_name like ? order by timestamp desc LIMIT ? OFFSET ?").
-define(SELECT_TRANSACTION_TOTAL_FILTER_ADDRESS_FILENAME, "SELECT COUNT(*) AS NUM FROM tx where item_type = ? and is_encrypt = '' and entity_type = 'File' and from_address = ? and item_name like ?").

-define(SELECT_TRANSACTION_RANGE_FILTER_FILENAME_SQL, "SELECT * FROM tx where item_type = ? and is_encrypt = '' and entity_type = 'File' and item_name like ? order by timestamp desc LIMIT ? OFFSET ?").
-define(SELECT_TRANSACTION_TOTAL_FILTER_FILENAME, "SELECT COUNT(*) AS NUM FROM tx where item_type = ? and is_encrypt = '' and entity_type = 'File' and item_name like ?").

-define(SELECT_TRANSACTION_RANGE_BUNDLETXPARSE_SQL, "SELECT * FROM tx where bundletxparse = ? and entity_type = 'Bundle' order by timestamp asc LIMIT ? OFFSET ?").
-define(SELECT_TRANSACTION_TOTAL_BUNDLETXPARSE, "SELECT COUNT(*) AS NUM FROM tx where bundletxparse = ? and entity_type = 'Bundle'").

%% Explain: where (id = last_tx_action and id = ?) or (id != last_tx_action and id = ? and last_tx_action = ?)
%% Case 1: Frist File Create
%% Case 2: Tx By Tx, make a change log chain
%% Case 3: In one block, have multi operations on one file
-define(UPDATE_TX_LABEL_SQL, "update tx set item_label = ?, timestamp = ?, last_tx_action = ? where (id = last_tx_action and id = ?) or (id != last_tx_action and id = ? and last_tx_action = ?) or (id != last_tx_action and id = ? and last_tx_action = ?)").
-define(UPDATE_TX_STAR_SQL, "update tx set item_star = ?, timestamp = ?, last_tx_action = ? where (id = last_tx_action and id = ?) or (id != last_tx_action and id = ? and last_tx_action = ?) or (id != last_tx_action and id = ? and last_tx_action = ?)").
-define(UPDATE_TX_FOLDER_SQL, "update tx set item_parent = ?, timestamp = ?, last_tx_action = ? where (id = last_tx_action and id = ?) or (id != last_tx_action and id = ? and last_tx_action = ?) or (id != last_tx_action and id = ? and last_tx_action = ?)").
-define(UPDATE_TX_PUBLIC_SQL, "update tx set is_public = ?, timestamp = ?, last_tx_action = ? where (id = last_tx_action and id = ?) or (id != last_tx_action and id = ? and last_tx_action = ?) or (id != last_tx_action and id = ? and last_tx_action = ?)").
-define(UPDATE_TX_BUNDLETXPARSE_SQL, "update tx set bundleTxParse = ? where id = ?").

-define(SELECT_TRANSACTION_RANGE_FOLDER_ADDRESS_SQL, "SELECT * FROM tx where item_parent = ? and is_encrypt = '' and (entity_type = 'File' or entity_type = 'Folder') and from_address = ? order by entity_type desc, timestamp desc LIMIT ? OFFSET ?").
-define(SELECT_TRANSACTION_TOTAL_FOLDER_ADDRESS, "SELECT COUNT(*) AS NUM FROM tx where item_parent = ? and is_encrypt = '' and (entity_type = 'File' or entity_type = 'Folder') and from_address = ?").

-define(SELECT_TRANSACTION_RANGE_LABEL_ADDRESS_SQL, "SELECT * FROM tx where item_label = ? and is_encrypt = '' and entity_type = 'File' and from_address = ? order by timestamp desc LIMIT ? OFFSET ?").
-define(SELECT_TRANSACTION_TOTAL_LABEL_ADDRESS, "SELECT COUNT(*) AS NUM FROM tx where item_label = ? and is_encrypt = '' and entity_type = 'File' and from_address = ?").

-define(SELECT_TRANSACTION_RANGE_STAR_ADDRESS_SQL, "SELECT * FROM tx where item_star = ? and is_encrypt = '' and entity_type = 'File' and from_address = ? order by timestamp desc LIMIT ? OFFSET ?").
-define(SELECT_TRANSACTION_TOTAL_STAR_ADDRESS, "SELECT COUNT(*) AS NUM FROM tx where item_star = ? and is_encrypt = '' and entity_type = 'File' and from_address = ?").

-define(SELECT_TRANSACTION_GROUP_LABEL_ADDRESS, "SELECT item_label, COUNT(*) AS NUM FROM tx where is_encrypt = '' and entity_type = 'File' and from_address = ? group by item_label").

-define(SELECT_FOLDER_ADDRESS, "SELECT * FROM tx where is_encrypt = '' and entity_type = 'Folder' and from_address = ? order by timestamp desc").

%%%===================================================================
%%% Public API.
%%%===================================================================

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

select_tx_by_id(ID) ->
	gen_server:call(?MODULE, {select_tx_by_id, ID}, ?SELECT_TIMEOUT).

select_txs_by(Opts) ->
	gen_server:call(?MODULE, {select_txs_by, Opts}, ?SELECT_TIMEOUT).

select_block_by_tx_id(TXID) ->
	gen_server:call(?MODULE, {select_block_by_tx_id, TXID}, ?SELECT_TIMEOUT).

select_tags_by_tx_id(TXID) ->
	gen_server:call(?MODULE, {select_tags_by_tx_id, TXID}, ?SELECT_TIMEOUT).

select_address_range(LIMIT, OFFSET) ->
	gen_server:call(?MODULE, {select_address_range, LIMIT, OFFSET}, ?SELECT_TIMEOUT).

select_address_total() ->
	gen_server:call(?MODULE, {select_address_total}, ?SELECT_TIMEOUT).

select_address_referee_range(ADDRESS, LIMIT, OFFSET) ->
	gen_server:call(?MODULE, {select_address_referee_range, ADDRESS, LIMIT, OFFSET}, ?SELECT_TIMEOUT).

select_address_referee_total(ADDRESS) ->
	gen_server:call(?MODULE, {select_address_referee_total, ADDRESS}, ?SELECT_TIMEOUT).

select_address_agent_range(LIMIT, OFFSET) ->
	gen_server:call(?MODULE, {select_address_agent_range, LIMIT, OFFSET}, ?SELECT_TIMEOUT).

select_address_agent_total() ->
	gen_server:call(?MODULE, {select_address_agent_total}, ?SELECT_TIMEOUT).

select_address_profile_range(LIMIT, OFFSET) ->
	gen_server:call(?MODULE, {select_address_profile_range, LIMIT, OFFSET}, ?SELECT_TIMEOUT).

select_address_profile_my(ADDRESS) ->
	gen_server:call(?MODULE, {select_address_profile_my, ADDRESS}, ?SELECT_TIMEOUT).

select_address_profile_total() ->
	gen_server:call(?MODULE, {select_address_profile_total}, ?SELECT_TIMEOUT).

select_transaction_range(LIMIT, OFFSET) ->
	gen_server:call(?MODULE, {select_transaction_range, LIMIT, OFFSET}, ?SELECT_TIMEOUT).

select_transaction_total() ->
	gen_server:call(?MODULE, {select_transaction_total}, ?SELECT_TIMEOUT).

select_transaction_range_filter(CONTENT_TYPE, LIMIT, OFFSET) ->
	gen_server:call(?MODULE, {select_transaction_range_filter, CONTENT_TYPE, LIMIT, OFFSET}, ?SELECT_TIMEOUT).

select_transaction_total_filter(CONTENT_TYPE) ->
	gen_server:call(?MODULE, {select_transaction_total_filter, CONTENT_TYPE}, ?SELECT_TIMEOUT).

select_transaction_range_filetype_address(CONTENT_TYPE, FROM_ADDRESS, LIMIT, OFFSET) ->
	gen_server:call(?MODULE, {select_transaction_range_filetype_address, CONTENT_TYPE, FROM_ADDRESS, LIMIT, OFFSET}, ?SELECT_TIMEOUT).

select_transaction_total_filetype_address(CONTENT_TYPE, FROM_ADDRESS) ->
	gen_server:call(?MODULE, {select_transaction_total_filetype_address, CONTENT_TYPE, FROM_ADDRESS}, ?SELECT_TIMEOUT).

select_transaction_range_folder_address(FOLDER, FROM_ADDRESS, LIMIT, OFFSET) ->
	gen_server:call(?MODULE, {select_transaction_range_folder_address, FOLDER, FROM_ADDRESS, LIMIT, OFFSET}, ?SELECT_TIMEOUT).

select_transaction_total_folder_address(FOLDER, FROM_ADDRESS) ->
	gen_server:call(?MODULE, {select_transaction_total_folder_address, FOLDER, FROM_ADDRESS}, ?SELECT_TIMEOUT).

select_transaction_range_label_address(LABEL, FROM_ADDRESS, LIMIT, OFFSET) ->
	gen_server:call(?MODULE, {select_transaction_range_label_address, LABEL, FROM_ADDRESS, LIMIT, OFFSET}, ?SELECT_TIMEOUT).

select_transaction_group_label_address(FROM_ADDRESS) ->
	gen_server:call(?MODULE, {select_transaction_group_label_address, FROM_ADDRESS}, ?SELECT_TIMEOUT).

select_folder_address(FROM_ADDRESS) ->
	gen_server:call(?MODULE, {select_folder_address, FROM_ADDRESS}, ?SELECT_TIMEOUT).

select_transaction_total_label_address(LABEL, FROM_ADDRESS) ->
	gen_server:call(?MODULE, {select_transaction_total_label_address, LABEL, FROM_ADDRESS}, ?SELECT_TIMEOUT).

select_transaction_range_star_address(STAR, FROM_ADDRESS, LIMIT, OFFSET) ->
	gen_server:call(?MODULE, {select_transaction_range_star_address, STAR, FROM_ADDRESS, LIMIT, OFFSET}, ?SELECT_TIMEOUT).

select_transaction_total_star_address(STAR, FROM_ADDRESS) ->
	gen_server:call(?MODULE, {select_transaction_total_star_address, STAR, FROM_ADDRESS}, ?SELECT_TIMEOUT).

select_transaction_range_filter_address_filename(CONTENT_TYPE, FROM_ADDRESS, FILE_NAME, LIMIT, OFFSET) ->
	gen_server:call(?MODULE, {select_transaction_range_filter_address_filename, CONTENT_TYPE, FROM_ADDRESS, FILE_NAME, LIMIT, OFFSET}, ?SELECT_TIMEOUT).

select_transaction_total_filter_address_filename(CONTENT_TYPE, FROM_ADDRESS, FILE_NAME) ->
	gen_server:call(?MODULE, {select_transaction_total_filter_address_filename, CONTENT_TYPE, FROM_ADDRESS, FILE_NAME}, ?SELECT_TIMEOUT).
	
select_transaction_range_filter_filename(CONTENT_TYPE, FILE_NAME, LIMIT, OFFSET) ->
	gen_server:call(?MODULE, {select_transaction_range_filter_filename, CONTENT_TYPE, FILE_NAME, LIMIT, OFFSET}, ?SELECT_TIMEOUT).

select_transaction_total_filter_filename(CONTENT_TYPE, FILE_NAME) ->
	gen_server:call(?MODULE, {select_transaction_total_filter_filename, CONTENT_TYPE, FILE_NAME}, ?SELECT_TIMEOUT).

select_transaction_range_bundletxparse(BUNDLETXPARSE, LIMIT, OFFSET) ->
	gen_server:call(?MODULE, {select_transaction_range_bundletxparse, BUNDLETXPARSE, LIMIT, OFFSET}, ?SELECT_TIMEOUT).

select_transaction_total_bundletxparse(BUNDLETXPARSE) ->
	gen_server:call(?MODULE, {select_transaction_total_bundletxparse, BUNDLETXPARSE}, ?SELECT_TIMEOUT).

eval_legacy_arql(Query) ->
	gen_server:call(?MODULE, {eval_legacy_arql, Query}, ?SELECT_TIMEOUT).

update_tx_label(ITEM_LABEL, TIMESTAMP, CURRENT_BLOCK_HEIGHT, FILE_TXID, LAST_TX_ACTION) ->
	gen_server:cast(?MODULE, {update_tx_label, ITEM_LABEL, TIMESTAMP, CURRENT_BLOCK_HEIGHT, FILE_TXID, LAST_TX_ACTION}),
	ok.

update_tx_star(ITEM_STAR, TIMESTAMP, CURRENT_BLOCK_HEIGHT, FILE_TXID, LAST_TX_ACTION) ->
	gen_server:cast(?MODULE, {update_tx_star, ITEM_STAR, TIMESTAMP, CURRENT_BLOCK_HEIGHT, FILE_TXID, LAST_TX_ACTION}),
	ok.

update_tx_folder(ITEM_PARENT, TIMESTAMP, CURRENT_BLOCK_HEIGHT, FILE_TXID, LAST_TX_ACTION) ->
	gen_server:cast(?MODULE, {update_tx_folder, ITEM_PARENT, TIMESTAMP, CURRENT_BLOCK_HEIGHT, FILE_TXID, LAST_TX_ACTION}),
	ok.

update_tx_public(IS_PUBLIC, TIMESTAMP, CURRENT_BLOCK_HEIGHT, FILE_TXID, LAST_TX_ACTION) ->
	gen_server:cast(?MODULE, {update_tx_public, IS_PUBLIC, TIMESTAMP, CURRENT_BLOCK_HEIGHT, FILE_TXID, LAST_TX_ACTION}),
	ok.

update_tx_bundletxparse(BundleTxParse, TXID) ->
	gen_server:cast(?MODULE, {update_tx_bundletxparse, BundleTxParse, TXID}),
	ok.

update_address_referee(REFEREE, ADDRESS) ->
	gen_server:cast(?MODULE, {update_address_referee, REFEREE, ADDRESS}),
	ok.

update_address_agent(AGENT, ADDRESS, TIMESTAMP) ->
	gen_server:cast(?MODULE, {update_address_agent, AGENT, ADDRESS, TIMESTAMP}),
	ok.

update_address_profile(PROFILE, TIMESTAMP, CURRENT_BLOCK_HEIGHT, ADDRESS, LAST_TX_ACTION) ->
	gen_server:cast(?MODULE, {update_address_profile, PROFILE, TIMESTAMP, CURRENT_BLOCK_HEIGHT, ADDRESS, LAST_TX_ACTION}),
	ok.

update_address_blockinfo(BALANCE, BLOCKHEIGHT, TIMESTAMP, ADDRESS) ->
	gen_server:cast(?MODULE, {update_address_blockinfo, BALANCE, BLOCKHEIGHT, TIMESTAMP, ADDRESS}),
	ok.

insert_full_block(FullBlock) ->
	insert_full_block(FullBlock, store_tags).

insert_full_block(#block {} = FullBlock, StoreTags) ->
	{BlockFields, TxFieldsList} = full_block_to_fields(FullBlock),
	TagFieldsList = case StoreTags of
		store_tags ->
			block_to_tag_fields_list(FullBlock);
		_ ->
			[]
	end,
	Call = {insert_full_block, BlockFields, TxFieldsList, TagFieldsList},
	case catch gen_server:call(?MODULE, Call, 30000) of
		{'EXIT', {timeout, {gen_server, call, _}}} ->
			{error, sqlite_timeout};
		Reply ->
			Reply
	end.

insert_block(B) ->
	BlockFields = block_to_fields(B),
	gen_server:cast(?MODULE, {insert_block, BlockFields}),
	ok.

insert_tx(BH, TX, Timestamp, Height) ->
	insert_tx(BH, TX, store_tags, Timestamp, Height).

insert_tx(BH, TX, StoreTags, Timestamp, Height) ->
	TXFields = tx_to_fields(BH, TX, Timestamp, Height),
	TagFieldsList = case StoreTags of
		store_tags ->
			tx_to_tag_fields_list(TX);
		_ ->
			[]
	end,
	gen_server:cast(?MODULE, {insert_tx, TXFields, TagFieldsList}),
	ok.

insert_tx(TXFields, TagFieldsList) ->
	gen_server:cast(?MODULE, {insert_tx, TXFields, TagFieldsList}),
	ok.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	{ok, Config} = application:get_env(chivesweave, config),
	DataDir = Config#config.data_dir,
	?LOG_INFO([{ar_arql_db, init}, {data_dir, DataDir}]),
	%% Very occasionally the port fails to be reopened immediately after
	%% a crash so we give it a little time here.
	timer:sleep(1000),
	DbPath = filename:join([DataDir, ?SQLITE3_DIR, "arql.db"]),
	ok = filelib:ensure_dir(DbPath),
	ok = ar_sqlite3:start_link(),
	{ok, Conn} = ar_sqlite3:open(DbPath, ?DRIVER_TIMEOUT),
	ok = ensure_meta_table_created(Conn),
	ok = ensure_schema_created(Conn),
	{ok, InsertBlockStmt} = ar_sqlite3:prepare(Conn, ?INSERT_BLOCK_SQL, ?DRIVER_TIMEOUT),
	{ok, InsertTxStmt} = ar_sqlite3:prepare(Conn, ?INSERT_TX_SQL, ?DRIVER_TIMEOUT),
	{ok, InsertTagStmt} = ar_sqlite3:prepare(Conn, ?INSERT_TAG_SQL, ?DRIVER_TIMEOUT),
	{ok, SelectTxByIdStmt} = ar_sqlite3:prepare(Conn, ?SELECT_TX_BY_ID_SQL, ?DRIVER_TIMEOUT),
	{ok, SelectBlockByTxIdStmt} = ar_sqlite3:prepare(Conn, ?SELECT_BLOCK_BY_TX_ID_SQL, ?DRIVER_TIMEOUT),
	{ok, SelectTagsByTxIdStmt} = ar_sqlite3:prepare(Conn, ?SELECT_TAGS_BY_TX_ID_SQL, ?DRIVER_TIMEOUT),
	{ok, InsertAddressStmt} = ar_sqlite3:prepare(Conn, ?INSERT_ADDRESS_SQL, ?DRIVER_TIMEOUT),
	{ok, SelectAddressRangeStmt} = ar_sqlite3:prepare(Conn, ?SELECT_ADDRESS_RANGE_SQL, ?DRIVER_TIMEOUT),
	{ok, SelectAddressTotalStmt} = ar_sqlite3:prepare(Conn, ?SELECT_ADDRESS_TOTAL, ?DRIVER_TIMEOUT),
	{ok, SelectAddressRefereeRangeStmt} = ar_sqlite3:prepare(Conn, ?SELECT_ADDRESS_REFEREE_RANGE_SQL, ?DRIVER_TIMEOUT),
	{ok, SelectAddressRefereeTotalStmt} = ar_sqlite3:prepare(Conn, ?SELECT_ADDRESS_REFEREE_TOTAL, ?DRIVER_TIMEOUT),
	{ok, SelectAddressAgentRangeStmt} = ar_sqlite3:prepare(Conn, ?SELECT_ADDRESS_AGENT_RANGE_SQL, ?DRIVER_TIMEOUT),
	{ok, SelectAddressAgentTotalStmt} = ar_sqlite3:prepare(Conn, ?SELECT_ADDRESS_AGENT_TOTAL, ?DRIVER_TIMEOUT),
	{ok, SelectAddressProfileRangeStmt} = ar_sqlite3:prepare(Conn, ?SELECT_ADDRESS_PROFILE_RANGE_SQL, ?DRIVER_TIMEOUT),
	{ok, SelectAddressProfileMyStmt} = ar_sqlite3:prepare(Conn, ?SELECT_ADDRESS_PROFILE_MY_SQL, ?DRIVER_TIMEOUT),
	{ok, SelectAddressProfileTotalStmt} = ar_sqlite3:prepare(Conn, ?SELECT_ADDRESS_PROFILE_TOTAL, ?DRIVER_TIMEOUT),
	{ok, SelectTransactionRangeStmt} = ar_sqlite3:prepare(Conn, ?SELECT_TRANSACTION_RANGE_SQL, ?DRIVER_TIMEOUT),
	{ok, SelectTransactionTotalStmt} = ar_sqlite3:prepare(Conn, ?SELECT_TRANSACTION_TOTAL, ?DRIVER_TIMEOUT),
	{ok, SelectTransactionRangeFilterStmt} = ar_sqlite3:prepare(Conn, ?SELECT_TRANSACTION_RANGE_FILTER_SQL, ?DRIVER_TIMEOUT),
	{ok, SelectTransactionTotalFilterStmt} = ar_sqlite3:prepare(Conn, ?SELECT_TRANSACTION_TOTAL_FILTER, ?DRIVER_TIMEOUT),
	{ok, SelectTransactionRangeFilterAddressStmt} = ar_sqlite3:prepare(Conn, ?SELECT_TRANSACTION_RANGE_FILTER_ADDRESS_SQL, ?DRIVER_TIMEOUT),
	{ok, SelectTransactionTotalFilterAddressStmt} = ar_sqlite3:prepare(Conn, ?SELECT_TRANSACTION_TOTAL_FILTER_ADDRESS, ?DRIVER_TIMEOUT),
	{ok, SelectTransactionRangeFolderAddressStmt} = ar_sqlite3:prepare(Conn, ?SELECT_TRANSACTION_RANGE_FOLDER_ADDRESS_SQL, ?DRIVER_TIMEOUT),
	{ok, SelectTransactionTotalFolderAddressStmt} = ar_sqlite3:prepare(Conn, ?SELECT_TRANSACTION_TOTAL_FOLDER_ADDRESS, ?DRIVER_TIMEOUT),
	{ok, SelectTransactionRangeLabelAddressStmt} = ar_sqlite3:prepare(Conn, ?SELECT_TRANSACTION_RANGE_LABEL_ADDRESS_SQL, ?DRIVER_TIMEOUT),
	{ok, SelectTransactionTotalLabelAddressStmt} = ar_sqlite3:prepare(Conn, ?SELECT_TRANSACTION_TOTAL_LABEL_ADDRESS, ?DRIVER_TIMEOUT),
	{ok, SelectTransactionRangeStarAddressStmt} = ar_sqlite3:prepare(Conn, ?SELECT_TRANSACTION_RANGE_STAR_ADDRESS_SQL, ?DRIVER_TIMEOUT),
	{ok, SelectTransactionTotalStarAddressStmt} = ar_sqlite3:prepare(Conn, ?SELECT_TRANSACTION_TOTAL_STAR_ADDRESS, ?DRIVER_TIMEOUT),
	{ok, SelectTransactionRangeFilterAddressFileNameStmt} = ar_sqlite3:prepare(Conn, ?SELECT_TRANSACTION_RANGE_FILTER_ADDRESS_FILENAME_SQL, ?DRIVER_TIMEOUT),
	{ok, SelectTransactionTotalFilterAddressFileNameStmt} = ar_sqlite3:prepare(Conn, ?SELECT_TRANSACTION_TOTAL_FILTER_ADDRESS_FILENAME, ?DRIVER_TIMEOUT),
	{ok, SelectTransactionRangeFilterFileNameStmt} = ar_sqlite3:prepare(Conn, ?SELECT_TRANSACTION_RANGE_FILTER_FILENAME_SQL, ?DRIVER_TIMEOUT),
	{ok, SelectTransactionTotalFilterFileNameStmt} = ar_sqlite3:prepare(Conn, ?SELECT_TRANSACTION_TOTAL_FILTER_FILENAME, ?DRIVER_TIMEOUT),
	{ok, SelectTransactionRangeBundleTxParseStmt} = ar_sqlite3:prepare(Conn, ?SELECT_TRANSACTION_RANGE_BUNDLETXPARSE_SQL, ?DRIVER_TIMEOUT),
	{ok, SelectTransactionTotalBundleTxParseStmt} = ar_sqlite3:prepare(Conn, ?SELECT_TRANSACTION_TOTAL_BUNDLETXPARSE, ?DRIVER_TIMEOUT),
	{ok, UpdateTxLabelStmt} = ar_sqlite3:prepare(Conn, ?UPDATE_TX_LABEL_SQL, ?DRIVER_TIMEOUT),
	{ok, UpdateTxStarStmt} = ar_sqlite3:prepare(Conn, ?UPDATE_TX_STAR_SQL, ?DRIVER_TIMEOUT),
	{ok, UpdateTxFolderStmt} = ar_sqlite3:prepare(Conn, ?UPDATE_TX_FOLDER_SQL, ?DRIVER_TIMEOUT),
	{ok, UpdateTxPublicStmt} = ar_sqlite3:prepare(Conn, ?UPDATE_TX_PUBLIC_SQL, ?DRIVER_TIMEOUT),
	{ok, UpdateTxBundleTxParseStmt} = ar_sqlite3:prepare(Conn, ?UPDATE_TX_BUNDLETXPARSE_SQL, ?DRIVER_TIMEOUT),
	{ok, UpdateAddressRefereeStmt} = ar_sqlite3:prepare(Conn, ?UPDATE_ADDRESS_REFEREE_SQL, ?DRIVER_TIMEOUT),
	{ok, UpdateAddressAgentStmt} = ar_sqlite3:prepare(Conn, ?UPDATE_ADDRESS_AGENT_SQL, ?DRIVER_TIMEOUT),
	{ok, UpdateAddressProfileStmt} = ar_sqlite3:prepare(Conn, ?UPDATE_ADDRESS_PROFILE_SQL, ?DRIVER_TIMEOUT),
	{ok, UpdateAddressBlockinfoStmt} = ar_sqlite3:prepare(Conn, ?UPDATE_ADDRESS_BLOCKINFO_SQL, ?DRIVER_TIMEOUT),
	{ok, SelectTransactionGroupLabelAddressStmt} = ar_sqlite3:prepare(Conn, ?SELECT_TRANSACTION_GROUP_LABEL_ADDRESS, ?DRIVER_TIMEOUT),
	{ok, SelectFolderAddressStmt} = ar_sqlite3:prepare(Conn, ?SELECT_FOLDER_ADDRESS, ?DRIVER_TIMEOUT),
	{ok, #{
		data_dir => DataDir,
		conn => Conn,
		insert_block_stmt => InsertBlockStmt,
		insert_tx_stmt => InsertTxStmt,
		insert_tag_stmt => InsertTagStmt,
		select_tx_by_id_stmt => SelectTxByIdStmt,
		select_block_by_tx_id_stmt => SelectBlockByTxIdStmt,
		select_tags_by_tx_id_stmt => SelectTagsByTxIdStmt,
		insert_address_stmt => InsertAddressStmt,
		select_address_range_stmt => SelectAddressRangeStmt,
		select_address_total_stmt => SelectAddressTotalStmt,
		select_address_referee_range_stmt => SelectAddressRefereeRangeStmt,
		select_address_referee_total_stmt => SelectAddressRefereeTotalStmt,
		select_address_agent_range_stmt => SelectAddressAgentRangeStmt,
		select_address_agent_total_stmt => SelectAddressAgentTotalStmt,
		select_address_profile_range_stmt => SelectAddressProfileRangeStmt,
		select_address_profile_my_stmt => SelectAddressProfileMyStmt,
		select_address_profile_total_stmt => SelectAddressProfileTotalStmt,
		select_transaction_range_stmt => SelectTransactionRangeStmt,
		select_transaction_total_stmt => SelectTransactionTotalStmt,
		select_transaction_range_filter_stmt => SelectTransactionRangeFilterStmt,
		select_transaction_total_filter_stmt => SelectTransactionTotalFilterStmt,
		select_transaction_range_filter_address_stmt => SelectTransactionRangeFilterAddressStmt,
		select_transaction_total_filter_address_stmt => SelectTransactionTotalFilterAddressStmt,
		select_transaction_range_folder_address_stmt => SelectTransactionRangeFolderAddressStmt,
		select_transaction_total_folder_address_stmt => SelectTransactionTotalFolderAddressStmt,
		select_transaction_range_label_address_stmt => SelectTransactionRangeLabelAddressStmt,
		select_transaction_total_label_address_stmt => SelectTransactionTotalLabelAddressStmt,
		select_transaction_range_star_address_stmt => SelectTransactionRangeStarAddressStmt,
		select_transaction_total_star_address_stmt => SelectTransactionTotalStarAddressStmt,
		select_transaction_range_filter_address_filename_stmt => SelectTransactionRangeFilterAddressFileNameStmt,
		select_transaction_total_filter_address_filename_stmt => SelectTransactionTotalFilterAddressFileNameStmt,
		select_transaction_range_filter_filename_stmt => SelectTransactionRangeFilterFileNameStmt,
		select_transaction_total_filter_filename_stmt => SelectTransactionTotalFilterFileNameStmt,
		select_transaction_range_bundletxparse_stmt => SelectTransactionRangeBundleTxParseStmt,
		select_transaction_total_bundletxparse_stmt => SelectTransactionTotalBundleTxParseStmt,
		update_tx_label_stmt => UpdateTxLabelStmt,
		update_tx_star_stmt => UpdateTxStarStmt,
		update_tx_folder_stmt => UpdateTxFolderStmt,
		update_tx_public_stmt => UpdateTxPublicStmt,
		update_tx_bundletxparse_stmt => UpdateTxBundleTxParseStmt,
		update_address_referee_stmt => UpdateAddressRefereeStmt,
		update_address_agent_stmt => UpdateAddressAgentStmt,
		update_address_profile_stmt => UpdateAddressProfileStmt,
		update_address_blockinfo_stmt => UpdateAddressBlockinfoStmt,
		select_transaction_group_label_address_stmt => SelectTransactionGroupLabelAddressStmt,
		select_folder_address_stmt => SelectFolderAddressStmt
	}}.

handle_call({insert_full_block, BlockFields, TxFieldsList, TagFieldsList}, _From, State) ->
	#{
		conn := Conn,
		insert_block_stmt := InsertBlockStmt,
		insert_tx_stmt := InsertTxStmt,
		insert_tag_stmt := InsertTagStmt,
		insert_address_stmt := InsertAddressStmt,
		update_address_blockinfo_stmt := UpdateAddressBlockinfoStmt
	} = State,
	{Time, ok} = timer:tc(fun() ->
		ok = ar_sqlite3:exec(Conn, "BEGIN TRANSACTION", ?INSERT_STEP_TIMEOUT),
		ok = ar_sqlite3:bind(InsertBlockStmt, BlockFields, ?INSERT_STEP_TIMEOUT),
		done = ar_sqlite3:step(InsertBlockStmt, ?INSERT_STEP_TIMEOUT),
		ok = ar_sqlite3:reset(InsertBlockStmt, ?INSERT_STEP_TIMEOUT),
		
		lists:foreach(
			fun(TxFields) ->
				ok = ar_sqlite3:bind(InsertTxStmt, TxFields, ?INSERT_STEP_TIMEOUT),
				done = ar_sqlite3:step(InsertTxStmt, ?INSERT_STEP_TIMEOUT),
				ok = ar_sqlite3:reset(InsertTxStmt, ?INSERT_STEP_TIMEOUT),
				
				% FromAddress Balance Record
				FromAddress = lists:nth(5, TxFields),
				case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(FromAddress) of
					{ok, FromAddressOK} ->
						case ar_node:get_balance(FromAddressOK) of
							node_unavailable ->
								ok;
							FromAddressBalance ->
								FromAddressFields = [FromAddress, integer_to_binary(FromAddressBalance div 100000000), lists:nth(3, BlockFields), lists:nth(4, BlockFields)],
								% ?LOG_INFO([{fromAddressFields___________________________________________________________________, FromAddressFields}]),
								ok = ar_sqlite3:bind(InsertAddressStmt, FromAddressFields, ?INSERT_STEP_TIMEOUT),
								done = ar_sqlite3:step(InsertAddressStmt, ?INSERT_STEP_TIMEOUT),
								ok = ar_sqlite3:reset(InsertAddressStmt, ?INSERT_STEP_TIMEOUT),
								UpdateAddressFields = [integer_to_binary(FromAddressBalance div 100000000), lists:nth(3, BlockFields), lists:nth(4, BlockFields), FromAddress, lists:nth(4, BlockFields)],
								ok = ar_sqlite3:bind(UpdateAddressBlockinfoStmt, UpdateAddressFields, ?INSERT_STEP_TIMEOUT),
								done = ar_sqlite3:step(UpdateAddressBlockinfoStmt, ?INSERT_STEP_TIMEOUT),
								ok = ar_sqlite3:reset(UpdateAddressBlockinfoStmt, ?INSERT_STEP_TIMEOUT)
						end
				end,

				% ToAddress Balance Record
				TargetAddress = lists:nth(6, TxFields),
				TargetAddressLength = byte_size(TargetAddress),
				% ?LOG_INFO([{targetAddressLength___________________________________________________________________, TargetAddressLength}]),
				case TargetAddressLength == 43 of
					true ->
						case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(TargetAddress) of
							{ok, TargetAddressOK} ->
								case ar_node:get_balance(TargetAddressOK) of
									node_unavailable ->
										ok;
									TargetAddressBalance ->
										TargetAddressFields = [TargetAddress, integer_to_binary(TargetAddressBalance div 100000000), lists:nth(3, BlockFields), lists:nth(4, BlockFields)],
										% ?LOG_INFO([{targetAddressFields___________________________________________________________________, TargetAddressFields}]),
										ok = ar_sqlite3:bind(InsertAddressStmt, TargetAddressFields, ?INSERT_STEP_TIMEOUT),
										done = ar_sqlite3:step(InsertAddressStmt, ?INSERT_STEP_TIMEOUT),
										ok = ar_sqlite3:reset(InsertAddressStmt, ?INSERT_STEP_TIMEOUT)
								end
						end;
					false ->
						ok
				end
				
			end,
			TxFieldsList
		),
		lists:foreach(
			fun(TagFields) ->
				ok = ar_sqlite3:bind(InsertTagStmt, TagFields, ?INSERT_STEP_TIMEOUT),
				done = ar_sqlite3:step(InsertTagStmt, ?INSERT_STEP_TIMEOUT),
				ok = ar_sqlite3:reset(InsertTagStmt, ?INSERT_STEP_TIMEOUT)
			end,
			TagFieldsList
		),
		ok = ar_sqlite3:exec(Conn, "COMMIT TRANSACTION", ?INSERT_STEP_TIMEOUT),
		ok
	end),
	record_query_time(insert_full_block, Time),
	{reply, ok, State};

handle_call({select_tx_by_id, ID}, _, State) ->
	#{ select_tx_by_id_stmt := Stmt } = State,
	ok = ar_sqlite3:bind(Stmt, [ID], ?DRIVER_TIMEOUT),
	{Time, Reply} = timer:tc(fun() ->
		case ar_sqlite3:step(Stmt, ?DRIVER_TIMEOUT) of
			{row, Row} -> {ok, tx_map(Row)};
			done -> not_found
		end
	end),
	ok = ar_sqlite3:reset(Stmt, ?DRIVER_TIMEOUT),
	record_query_time(select_tx_by_id, Time),
	{reply, Reply, State};

handle_call({select_txs_by, Opts}, _, #{ conn := Conn } = State) ->
	{WhereClause, Params} = select_txs_by_where_clause(Opts),
	SQL = lists:concat([
		"SELECT tx.* FROM tx ",
		"JOIN block on tx.block_indep_hash = block.indep_hash ",
		"WHERE ", WhereClause,
		" ORDER BY block.height DESC, tx.id DESC"
	]),
	{Time, Reply} = timer:tc(fun() ->
		case sql_fetchall(Conn, SQL, Params, ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:map(fun tx_map/1, Rows)
		end
	end),
	record_query_time(select_txs_by, Time),
	{reply, Reply, State};

handle_call({select_block_by_tx_id, TXID}, _, State) ->
	#{ select_block_by_tx_id_stmt := Stmt } = State,
	ok = ar_sqlite3:bind(Stmt, [TXID], ?DRIVER_TIMEOUT),
	{Time, Reply} = timer:tc(fun() ->
		case ar_sqlite3:step(Stmt, ?DRIVER_TIMEOUT) of
			{row, Row} -> {ok, block_map(Row)};
			done -> not_found
		end
	end),
	ar_sqlite3:reset(Stmt, ?DRIVER_TIMEOUT),
	record_query_time(select_block_by_tx_id, Time),
	{reply, Reply, State};

handle_call({select_tags_by_tx_id, TXID}, _, State) ->
	#{ select_tags_by_tx_id_stmt := Stmt } = State,
	{Time, Reply} = timer:tc(fun() ->
		case stmt_fetchall(Stmt, [TXID], ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:map(fun tags_map/1, Rows)
		end
	end),
	record_query_time(select_tags_by_tx_id, Time),
	{reply, Reply, State};

handle_call({select_address_range, LIMIT, OFFSET}, _, State) ->
	#{ select_address_range_stmt := Stmt } = State,
	{Time, Reply} = timer:tc(fun() ->
		case stmt_fetchall(Stmt, [LIMIT, OFFSET], ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:map(fun address_map/1, Rows)
		end
	end),
	record_query_time(select_address_range, Time),
	{reply, Reply, State};

handle_call({select_address_total}, _, State) ->
	#{ select_address_total_stmt := Stmt } = State,
	{Time, Reply} = timer:tc(fun() ->
		case stmt_fetchall(Stmt, [], ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:nth(1, lists:nth(1, Rows))
		end
	end),
	record_query_time(select_address_total, Time),
	{reply, Reply, State};

handle_call({select_address_referee_range, ADDRESS, LIMIT, OFFSET}, _, State) ->
	#{ select_address_referee_range_stmt := Stmt } = State,
	{Time, Reply} = timer:tc(fun() ->
		case stmt_fetchall(Stmt, [ADDRESS, LIMIT, OFFSET], ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:map(fun address_map/1, Rows)
		end
	end),
	record_query_time(select_address_referee_range, Time),
	{reply, Reply, State};

handle_call({select_address_referee_total, ADDRESS}, _, State) ->
	#{ select_address_referee_total_stmt := Stmt } = State,
	{Time, Reply} = timer:tc(fun() ->
		case stmt_fetchall(Stmt, [ADDRESS], ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:nth(1, lists:nth(1, Rows))
		end
	end),
	record_query_time(select_address_referee_total, Time),
	{reply, Reply, State};

handle_call({select_address_agent_range, LIMIT, OFFSET}, _, State) ->
	#{ select_address_agent_range_stmt := Stmt } = State,
	{Time, Reply} = timer:tc(fun() ->
		case stmt_fetchall(Stmt, [LIMIT, OFFSET], ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:map(fun address_map/1, Rows)
		end
	end),
	record_query_time(select_address_agent_range, Time),
	{reply, Reply, State};

handle_call({select_address_agent_total}, _, State) ->
	#{ select_address_agent_total_stmt := Stmt } = State,
	{Time, Reply} = timer:tc(fun() ->
		case stmt_fetchall(Stmt, [], ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:nth(1, lists:nth(1, Rows))
		end
	end),
	record_query_time(select_address_agent_total, Time),
	{reply, Reply, State};

handle_call({select_address_profile_my, ADDRESS}, _, State) ->
	#{ select_address_profile_my_stmt := Stmt } = State,
	{Time, Reply} = timer:tc(fun() ->
		case stmt_fetchall(Stmt, [ADDRESS], ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:map(fun address_map/1, Rows)
		end
	end),
	record_query_time(select_address_profile_my, Time),
	{reply, Reply, State};

handle_call({select_address_profile_range, LIMIT, OFFSET}, _, State) ->
	#{ select_address_profile_range_stmt := Stmt } = State,
	{Time, Reply} = timer:tc(fun() ->
		case stmt_fetchall(Stmt, [LIMIT, OFFSET], ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:map(fun address_map/1, Rows)
		end
	end),
	record_query_time(select_address_profile_range, Time),
	{reply, Reply, State};

handle_call({select_address_profile_total}, _, State) ->
	#{ select_address_profile_total_stmt := Stmt } = State,
	{Time, Reply} = timer:tc(fun() ->
		case stmt_fetchall(Stmt, [], ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:nth(1, lists:nth(1, Rows))
		end
	end),
	record_query_time(select_address_profile_total, Time),
	{reply, Reply, State};

handle_call({select_transaction_range, LIMIT, OFFSET}, _, State) ->
	#{ select_transaction_range_stmt := Stmt } = State,
	{Time, Reply} = timer:tc(fun() ->
		case stmt_fetchall(Stmt, [LIMIT, OFFSET], ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:map(fun tx_map/1, Rows)
		end
	end),
	record_query_time(select_transaction_range, Time),
	{reply, Reply, State};

handle_call({select_transaction_total}, _, State) ->
	#{ select_transaction_total_stmt := Stmt } = State,
	{Time, Reply} = timer:tc(fun() ->
		case stmt_fetchall(Stmt, [], ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:nth(1, lists:nth(1, Rows))
		end
	end),
	record_query_time(select_transaction_total, Time),
	{reply, Reply, State};

handle_call({select_transaction_range_filter, CONTENT_TYPE, LIMIT, OFFSET}, _, State) ->
	#{ select_transaction_range_filter_stmt := Stmt } = State,
	{Time, Reply} = timer:tc(fun() ->
		case stmt_fetchall(Stmt, [CONTENT_TYPE, LIMIT, OFFSET], ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:map(fun tx_map/1, Rows)
		end
	end),
	record_query_time(select_transaction_range_filter, Time),
	{reply, Reply, State};

handle_call({select_transaction_total_filter, CONTENT_TYPE}, _, State) ->
	#{ select_transaction_total_filter_stmt := Stmt } = State,
	{Time, Reply} = timer:tc(fun() ->
		case stmt_fetchall(Stmt, [CONTENT_TYPE], ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:nth(1, lists:nth(1, Rows))
		end
	end),
	record_query_time(select_transaction_total_filter, Time),
	{reply, Reply, State};

handle_call({select_transaction_range_filetype_address, CONTENT_TYPE, FROM_ADDRESS, LIMIT, OFFSET}, _, State) ->
	#{ select_transaction_range_filter_address_stmt := Stmt } = State,
	{Time, Reply} = timer:tc(fun() ->
		case stmt_fetchall(Stmt, [CONTENT_TYPE, FROM_ADDRESS, LIMIT, OFFSET], ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:map(fun tx_map/1, Rows)
		end
	end),
	record_query_time(select_transaction_range_filetype_address, Time),
	{reply, Reply, State};

handle_call({select_transaction_total_filetype_address, CONTENT_TYPE, FROM_ADDRESS}, _, State) ->
	#{ select_transaction_total_filter_address_stmt := Stmt } = State,
	{Time, Reply} = timer:tc(fun() ->
		case stmt_fetchall(Stmt, [CONTENT_TYPE, FROM_ADDRESS], ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:nth(1, lists:nth(1, Rows))
		end
	end),
	record_query_time(select_transaction_total_filetype_address, Time),
	{reply, Reply, State};

handle_call({select_transaction_range_folder_address, FOLDER, FROM_ADDRESS, LIMIT, OFFSET}, _, State) ->
	#{ select_transaction_range_folder_address_stmt := Stmt } = State,
	{Time, Reply} = timer:tc(fun() ->
		case stmt_fetchall(Stmt, [FOLDER, FROM_ADDRESS, LIMIT, OFFSET], ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:map(fun tx_map/1, Rows)
		end
	end),
	record_query_time(select_transaction_range_folder_address, Time),
	{reply, Reply, State};

handle_call({select_transaction_total_folder_address, FOLDER, FROM_ADDRESS}, _, State) ->
	#{ select_transaction_total_folder_address_stmt := Stmt } = State,
	{Time, Reply} = timer:tc(fun() ->
		case stmt_fetchall(Stmt, [FOLDER, FROM_ADDRESS], ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:nth(1, lists:nth(1, Rows))
		end
	end),
	record_query_time(select_transaction_total_folder_address, Time),
	{reply, Reply, State};

handle_call({select_transaction_range_label_address, LABEL, FROM_ADDRESS, LIMIT, OFFSET}, _, State) ->
	#{ select_transaction_range_label_address_stmt := Stmt } = State,
	{Time, Reply} = timer:tc(fun() ->
		case stmt_fetchall(Stmt, [LABEL, FROM_ADDRESS, LIMIT, OFFSET], ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:map(fun tx_map/1, Rows)
		end
	end),
	record_query_time(select_transaction_range_label_address, Time),
	{reply, Reply, State};

handle_call({select_transaction_total_label_address, LABEL, FROM_ADDRESS}, _, State) ->
	#{ select_transaction_total_label_address_stmt := Stmt } = State,
	{Time, Reply} = timer:tc(fun() ->
		case stmt_fetchall(Stmt, [LABEL, FROM_ADDRESS], ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:nth(1, lists:nth(1, Rows))
		end
	end),
	record_query_time(select_transaction_total_label_address, Time),
	{reply, Reply, State};

handle_call({select_transaction_group_label_address, FROM_ADDRESS}, _, State) ->
	#{ select_transaction_group_label_address_stmt := Stmt } = State,
	{Time, Reply} = timer:tc(fun() ->
		case stmt_fetchall(Stmt, [FROM_ADDRESS], ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:map(fun item_label_group_map/1, Rows)
		end
	end),
	record_query_time(select_transaction_group_label_address, Time),
	{reply, Reply, State};

handle_call({select_folder_address, FROM_ADDRESS}, _, State) ->
	#{ select_folder_address_stmt := Stmt } = State,
	{Time, Reply} = timer:tc(fun() ->
		case stmt_fetchall(Stmt, [FROM_ADDRESS], ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:map(fun tx_map/1, Rows)
		end
	end),
	record_query_time(select_folder_address, Time),
	{reply, Reply, State};

handle_call({select_transaction_range_star_address, STAR, FROM_ADDRESS, LIMIT, OFFSET}, _, State) ->
	#{ select_transaction_range_star_address_stmt := Stmt } = State,
	{Time, Reply} = timer:tc(fun() ->
		case stmt_fetchall(Stmt, [STAR, FROM_ADDRESS, LIMIT, OFFSET], ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:map(fun tx_map/1, Rows)
		end
	end),
	record_query_time(select_transaction_range_star_address, Time),
	{reply, Reply, State};

handle_call({select_transaction_total_star_address, STAR, FROM_ADDRESS}, _, State) ->
	#{ select_transaction_total_star_address_stmt := Stmt } = State,
	{Time, Reply} = timer:tc(fun() ->
		case stmt_fetchall(Stmt, [STAR, FROM_ADDRESS], ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:nth(1, lists:nth(1, Rows))
		end
	end),
	record_query_time(select_transaction_total_star_address, Time),
	{reply, Reply, State};

handle_call({select_transaction_range_filter_address_filename, CONTENT_TYPE, FROM_ADDRESS, FILE_NAME, LIMIT, OFFSET}, _, State) ->
	#{ select_transaction_range_filter_address_filename_stmt := Stmt } = State,
	{Time, Reply} = timer:tc(fun() ->
		case stmt_fetchall(Stmt, [CONTENT_TYPE, FROM_ADDRESS, FILE_NAME, LIMIT, OFFSET], ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:map(fun tx_map/1, Rows)
		end
	end),
	record_query_time(select_transaction_range_filter_address_filename, Time),
	{reply, Reply, State};

handle_call({select_transaction_total_filter_address_filename, CONTENT_TYPE, FROM_ADDRESS, FILE_NAME}, _, State) ->
	#{ select_transaction_total_filter_address_filename_stmt := Stmt } = State,
	{Time, Reply} = timer:tc(fun() ->
		case stmt_fetchall(Stmt, [CONTENT_TYPE, FROM_ADDRESS, FILE_NAME], ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:nth(1, lists:nth(1, Rows))
		end
	end),
	record_query_time(select_transaction_total_filter_address_filename, Time),
	{reply, Reply, State};

handle_call({select_transaction_range_filter_filename, CONTENT_TYPE, FILE_NAME, LIMIT, OFFSET}, _, State) ->
	#{ select_transaction_range_filter_filename_stmt := Stmt } = State,
	{Time, Reply} = timer:tc(fun() ->
		case stmt_fetchall(Stmt, [CONTENT_TYPE, FILE_NAME, LIMIT, OFFSET], ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:map(fun tx_map/1, Rows)
		end
	end),
	record_query_time(select_transaction_range_filter_filename, Time),
	{reply, Reply, State};

handle_call({select_transaction_total_filter_filename, CONTENT_TYPE, FILE_NAME}, _, State) ->
	#{ select_transaction_total_filter_filename_stmt := Stmt } = State,
	{Time, Reply} = timer:tc(fun() ->
		case stmt_fetchall(Stmt, [CONTENT_TYPE, FILE_NAME], ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:nth(1, lists:nth(1, Rows))
		end
	end),
	record_query_time(select_transaction_total_filter_filename, Time),
	{reply, Reply, State};

handle_call({select_transaction_range_bundletxparse, BUNDLETXPARSE, LIMIT, OFFSET}, _, State) ->
	#{ select_transaction_range_bundletxparse_stmt := Stmt } = State,
	{Time, Reply} = timer:tc(fun() ->
		case stmt_fetchall(Stmt, [BUNDLETXPARSE, LIMIT, OFFSET], ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:map(fun tx_map/1, Rows)
		end
	end),
	record_query_time(select_transaction_range_bundletxparse, Time),
	{reply, Reply, State};

handle_call({select_transaction_total_bundletxparse, BUNDLETXPARSE}, _, State) ->
	#{ select_transaction_total_bundletxparse_stmt := Stmt } = State,
	{Time, Reply} = timer:tc(fun() ->
		case stmt_fetchall(Stmt, [BUNDLETXPARSE], ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:nth(1, lists:nth(1, Rows))
		end
	end),
	record_query_time(select_transaction_total_bundletxparse, Time),
	{reply, Reply, State};

handle_call({eval_legacy_arql, Query}, _, #{ conn := Conn } = State) ->
	{Time, {Reply, _SQL, _Params}} = timer:tc(fun() ->
		case catch eval_legacy_arql_where_clause(Query) of
			{WhereClause, Params} ->
				SQL = lists:concat([
					"SELECT tx.id FROM tx ",
					"JOIN block ON tx.block_indep_hash = block.indep_hash ",
					"WHERE ", WhereClause,
					" ORDER BY block.height DESC, tx.id DESC"
				]),
				case sql_fetchall(Conn, SQL, Params, ?DRIVER_TIMEOUT) of
					Rows when is_list(Rows) ->
						{lists:map(fun([TXID]) -> TXID end, Rows), SQL, Params}
				end;
			bad_query ->
				{bad_query, 'n/a', 'n/a'}
		end
	end),
	record_query_time(eval_legacy_arql, Time),
	{reply, Reply, State}.

handle_cast({insert_block, BlockFields}, State) ->
	#{
		conn := Conn,
		insert_block_stmt := InsertBlockStmt
	} = State,
	{Time, ok} = timer:tc(fun() ->
		ok = ar_sqlite3:exec(Conn, "BEGIN TRANSACTION", ?INSERT_STEP_TIMEOUT),
		ok = ar_sqlite3:bind(InsertBlockStmt, BlockFields, ?INSERT_STEP_TIMEOUT),
		done = ar_sqlite3:step(InsertBlockStmt, ?INSERT_STEP_TIMEOUT),
		ok = ar_sqlite3:reset(InsertBlockStmt, ?INSERT_STEP_TIMEOUT),
		ok = ar_sqlite3:exec(Conn, "COMMIT TRANSACTION", ?INSERT_STEP_TIMEOUT),
		ok
	end),
	record_query_time(insert_block, Time),
	{noreply, State};

handle_cast({update_tx_label, ITEM_LABEL, TIMESTAMP, CURRENT_BLOCK_HEIGHT, FILE_TXID, LAST_TX_ACTION}, State) ->
	#{ conn := Conn, update_tx_label_stmt := Stmt } = State,
	{Time, ok} = timer:tc(fun() ->
		ok = ar_sqlite3:exec(Conn, "BEGIN TRANSACTION", ?INSERT_STEP_TIMEOUT),

		ok = ar_sqlite3:bind(Stmt, [ITEM_LABEL, TIMESTAMP, CURRENT_BLOCK_HEIGHT, FILE_TXID, FILE_TXID, LAST_TX_ACTION, FILE_TXID, CURRENT_BLOCK_HEIGHT], ?INSERT_STEP_TIMEOUT),
		done = ar_sqlite3:step(Stmt, ?INSERT_STEP_TIMEOUT),
		ok = ar_sqlite3:reset(Stmt, ?INSERT_STEP_TIMEOUT),

		ok = ar_sqlite3:exec(Conn, "COMMIT TRANSACTION", ?INSERT_STEP_TIMEOUT),
		ok
	end),
	record_query_time(update_tx_label, Time),
	{noreply, State};

handle_cast({update_tx_star, ITEM_STAR, TIMESTAMP, CURRENT_BLOCK_HEIGHT, FILE_TXID, LAST_TX_ACTION}, State) ->
	#{ conn := Conn, update_tx_star_stmt := Stmt } = State,
	{Time, ok} = timer:tc(fun() ->
		ok = ar_sqlite3:exec(Conn, "BEGIN TRANSACTION", ?INSERT_STEP_TIMEOUT),
		?LOG_INFO([{update_tx_star______________________ITEM_STAR, ITEM_STAR}]),
		?LOG_INFO([{update_tx_star______________________TIMESTAMP, TIMESTAMP}]),
		?LOG_INFO([{update_tx_star______________________CURRENT_TXID, CURRENT_BLOCK_HEIGHT}]),
		?LOG_INFO([{update_tx_star______________________FILE_TXID, FILE_TXID}]),
		?LOG_INFO([{update_tx_star______________________LAST_TX_ACTION, LAST_TX_ACTION}]),

		ok = ar_sqlite3:bind(Stmt, [ITEM_STAR, TIMESTAMP, CURRENT_BLOCK_HEIGHT, FILE_TXID, FILE_TXID, LAST_TX_ACTION, FILE_TXID, CURRENT_BLOCK_HEIGHT], ?INSERT_STEP_TIMEOUT),
		done = ar_sqlite3:step(Stmt, ?INSERT_STEP_TIMEOUT),
		ok = ar_sqlite3:reset(Stmt, ?INSERT_STEP_TIMEOUT),

		ok = ar_sqlite3:exec(Conn, "COMMIT TRANSACTION", ?INSERT_STEP_TIMEOUT),
		ok
	end),
	record_query_time(update_tx_star, Time),
	{noreply, State};

handle_cast({update_tx_folder, ITEM_PARENT, TIMESTAMP, CURRENT_BLOCK_HEIGHT, FILE_TXID, LAST_TX_ACTION}, State) ->
	#{ conn := Conn, update_tx_folder_stmt := Stmt } = State,
	{Time, ok} = timer:tc(fun() ->
		ok = ar_sqlite3:exec(Conn, "BEGIN TRANSACTION", ?INSERT_STEP_TIMEOUT),

		ok = ar_sqlite3:bind(Stmt, [ITEM_PARENT, TIMESTAMP, CURRENT_BLOCK_HEIGHT, FILE_TXID, FILE_TXID, LAST_TX_ACTION, FILE_TXID, CURRENT_BLOCK_HEIGHT], ?INSERT_STEP_TIMEOUT),
		done = ar_sqlite3:step(Stmt, ?INSERT_STEP_TIMEOUT),
		ok = ar_sqlite3:reset(Stmt, ?INSERT_STEP_TIMEOUT),

		ok = ar_sqlite3:exec(Conn, "COMMIT TRANSACTION", ?INSERT_STEP_TIMEOUT),
		ok
	end),
	record_query_time(update_tx_folder, Time),
	{noreply, State};

handle_cast({update_tx_public, IS_PUBLIC, TIMESTAMP, CURRENT_BLOCK_HEIGHT, FILE_TXID, LAST_TX_ACTION}, State) ->
	#{ conn := Conn, update_tx_public_stmt := Stmt } = State,
	{Time, ok} = timer:tc(fun() ->
		ok = ar_sqlite3:exec(Conn, "BEGIN TRANSACTION", ?INSERT_STEP_TIMEOUT),

		ok = ar_sqlite3:bind(Stmt, [IS_PUBLIC, TIMESTAMP, CURRENT_BLOCK_HEIGHT, FILE_TXID, FILE_TXID, LAST_TX_ACTION, FILE_TXID, CURRENT_BLOCK_HEIGHT], ?INSERT_STEP_TIMEOUT),
		done = ar_sqlite3:step(Stmt, ?INSERT_STEP_TIMEOUT),
		ok = ar_sqlite3:reset(Stmt, ?INSERT_STEP_TIMEOUT),

		ok = ar_sqlite3:exec(Conn, "COMMIT TRANSACTION", ?INSERT_STEP_TIMEOUT),
		ok
	end),
	record_query_time(update_tx_public, Time),
	{noreply, State};

handle_cast({update_tx_bundletxparse, BUNDLETXPARSE, TXID}, State) ->
	#{ conn := Conn, update_tx_bundletxparse_stmt := Stmt } = State,
	{Time, ok} = timer:tc(fun() ->
		ok = ar_sqlite3:exec(Conn, "BEGIN TRANSACTION", ?INSERT_STEP_TIMEOUT),

		ok = ar_sqlite3:bind(Stmt, [BUNDLETXPARSE, TXID], ?INSERT_STEP_TIMEOUT),
		done = ar_sqlite3:step(Stmt, ?INSERT_STEP_TIMEOUT),
		ok = ar_sqlite3:reset(Stmt, ?INSERT_STEP_TIMEOUT),

		ok = ar_sqlite3:exec(Conn, "COMMIT TRANSACTION", ?INSERT_STEP_TIMEOUT),
		ok
	end),
	record_query_time(update_tx_bundletxparse, Time),
	{noreply, State};

handle_cast({update_address_referee, REFEREE, ADDRESS}, State) ->
	#{ conn := Conn, update_address_referee_stmt := Stmt } = State,
	{Time, ok} = timer:tc(fun() ->
		ok = ar_sqlite3:exec(Conn, "BEGIN TRANSACTION", ?INSERT_STEP_TIMEOUT),

		ok = ar_sqlite3:bind(Stmt, [REFEREE, ADDRESS], ?INSERT_STEP_TIMEOUT),
		done = ar_sqlite3:step(Stmt, ?INSERT_STEP_TIMEOUT),
		ok = ar_sqlite3:reset(Stmt, ?INSERT_STEP_TIMEOUT),

		ok = ar_sqlite3:exec(Conn, "COMMIT TRANSACTION", ?INSERT_STEP_TIMEOUT),
		ok
	end),
	record_query_time(update_address_referee, Time),
	{noreply, State};

handle_cast({update_address_agent, AGENT, ADDRESS, TIMESTAMP}, State) ->
	#{ conn := Conn, update_address_agent_stmt := Stmt } = State,
	{Time, ok} = timer:tc(fun() ->
		ok = ar_sqlite3:exec(Conn, "BEGIN TRANSACTION", ?INSERT_STEP_TIMEOUT),

		ok = ar_sqlite3:bind(Stmt, [AGENT, TIMESTAMP, ADDRESS], ?INSERT_STEP_TIMEOUT),
		done = ar_sqlite3:step(Stmt, ?INSERT_STEP_TIMEOUT),
		ok = ar_sqlite3:reset(Stmt, ?INSERT_STEP_TIMEOUT),

		ok = ar_sqlite3:exec(Conn, "COMMIT TRANSACTION", ?INSERT_STEP_TIMEOUT),
		ok
	end),
	record_query_time(update_address_agent, Time),
	{noreply, State};

handle_cast({update_address_profile, PROFILE, TIMESTAMP, CURRENT_BLOCK_HEIGHT, ADDRESS, LAST_TX_ACTION}, State) ->
	#{ conn := Conn, update_address_profile_stmt := Stmt } = State,
	{Time, ok} = timer:tc(fun() ->
		ok = ar_sqlite3:exec(Conn, "BEGIN TRANSACTION", ?INSERT_STEP_TIMEOUT),
		?LOG_INFO([{update_address_profile______________________________PROFILE, PROFILE}]),
		?LOG_INFO([{update_address_profile______________________________TIMESTAMP, TIMESTAMP}]),
		?LOG_INFO([{update_address_profile______________________________CURRENT_BLOCK_HEIGHT, CURRENT_BLOCK_HEIGHT}]),
		?LOG_INFO([{update_address_profile______________________________ADDRESS, ADDRESS}]),
		?LOG_INFO([{update_address_profile______________________________LAST_TX_ACTION, LAST_TX_ACTION}]),
		ok = ar_sqlite3:bind(Stmt, [PROFILE, TIMESTAMP, CURRENT_BLOCK_HEIGHT, ADDRESS, ADDRESS, LAST_TX_ACTION, ADDRESS, CURRENT_BLOCK_HEIGHT], ?INSERT_STEP_TIMEOUT),
		done = ar_sqlite3:step(Stmt, ?INSERT_STEP_TIMEOUT),
		ok = ar_sqlite3:reset(Stmt, ?INSERT_STEP_TIMEOUT),

		ok = ar_sqlite3:exec(Conn, "COMMIT TRANSACTION", ?INSERT_STEP_TIMEOUT),
		ok
	end),
	record_query_time(update_address_profile, Time),
	{noreply, State};

handle_cast({update_address_blockinfo, BALANCE, BLOCKHEIGHT, TIMESTAMP, ADDRESS}, State) ->
	#{ conn := Conn, update_address_blockinfo_stmt := Stmt } = State,
	{Time, ok} = timer:tc(fun() ->
		ok = ar_sqlite3:exec(Conn, "BEGIN TRANSACTION", ?INSERT_STEP_TIMEOUT),

		ok = ar_sqlite3:bind(Stmt, [BALANCE, BLOCKHEIGHT, TIMESTAMP, ADDRESS, TIMESTAMP], ?INSERT_STEP_TIMEOUT),
		done = ar_sqlite3:step(Stmt, ?INSERT_STEP_TIMEOUT),
		ok = ar_sqlite3:reset(Stmt, ?INSERT_STEP_TIMEOUT),

		ok = ar_sqlite3:exec(Conn, "COMMIT TRANSACTION", ?INSERT_STEP_TIMEOUT),
		ok
	end),
	record_query_time(update_address_blockinfo, Time),
	{noreply, State};

handle_cast({insert_tx, TXFields, TagFieldsList}, State) ->
	#{
		conn := Conn,
		insert_tx_stmt := InsertTxStmt,
		insert_tag_stmt := InsertTagStmt
	} = State,
	{Time, ok} = timer:tc(fun() ->
		ok = ar_sqlite3:exec(Conn, "BEGIN TRANSACTION", ?INSERT_STEP_TIMEOUT),
		ok = ar_sqlite3:bind(InsertTxStmt, TXFields, ?INSERT_STEP_TIMEOUT),
		done = ar_sqlite3:step(InsertTxStmt, ?INSERT_STEP_TIMEOUT),
		ok = ar_sqlite3:reset(InsertTxStmt, ?INSERT_STEP_TIMEOUT),
		lists:foreach(
			fun(TagFields) ->
				ok = ar_sqlite3:bind(InsertTagStmt, TagFields, ?INSERT_STEP_TIMEOUT),
				done = ar_sqlite3:step(InsertTagStmt, ?INSERT_STEP_TIMEOUT),
				ok = ar_sqlite3:reset(InsertTagStmt, ?INSERT_STEP_TIMEOUT)
			end,
			TagFieldsList
		),
		ok = ar_sqlite3:exec(Conn, "COMMIT TRANSACTION", ?INSERT_STEP_TIMEOUT),
		ok
	end),
	record_query_time(insert_tx, Time),
	{noreply, State}.

terminate(Reason, State) ->
	#{
		conn := Conn,
		insert_block_stmt := InsertBlockStmt,
		insert_tx_stmt := InsertTxStmt,
		insert_tag_stmt := InsertTagStmt,
		select_tx_by_id_stmt := SelectTxByIdStmt,
		select_block_by_tx_id_stmt := SelectBlockByTxIdStmt,
		select_tags_by_tx_id_stmt := SelectTagsByTxIdStmt,
		insert_address_stmt := InsertAddressStmt,
		select_address_range_stmt := SelectAddressRangeStmt,
		select_address_total_stmt := SelectAddressTotalStmt,
		select_address_referee_range_stmt := SelectAddressRefereeRangeStmt,
		select_address_referee_total_stmt := SelectAddressRefereeTotalStmt,
		select_address_agent_range_stmt := SelectAddressAgentRangeStmt,
		select_address_agent_total_stmt := SelectAddressAgentTotalStmt,
		select_address_profile_range_stmt := SelectAddressProfileRangeStmt,
		select_address_profile_my_stmt := SelectAddressProfileMyStmt,
		select_address_profile_total_stmt := SelectAddressProfileTotalStmt,
		select_transaction_range_stmt := SelectTransactionRangeStmt,
		select_transaction_total_stmt := SelectTransactionTotalStmt,
		select_transaction_range_filter_stmt := SelectTransactionRangeFilterStmt,
		select_transaction_total_filter_stmt := SelectTransactionTotalFilterStmt,
		select_transaction_range_filter_address_stmt := SelectTransactionRangeFilterAddressStmt,
		select_transaction_total_filter_address_stmt := SelectTransactionTotalFilterAddressStmt,
		select_transaction_range_folder_address_stmt := SelectTransactionRangeFolderAddressStmt,
		select_transaction_total_folder_address_stmt := SelectTransactionTotalFolderAddressStmt,
		select_transaction_range_label_address_stmt := SelectTransactionRangeLabelAddressStmt,
		select_transaction_total_label_address_stmt := SelectTransactionTotalLabelAddressStmt,
		select_transaction_range_star_address_stmt := SelectTransactionRangeStarAddressStmt,
		select_transaction_total_star_address_stmt := SelectTransactionTotalStarAddressStmt,
		update_tx_label_stmt := UpdateTxLabelStmt,
		update_tx_star_stmt := UpdateTxStarStmt,
		update_tx_folder_stmt := UpdateTxFolderStmt,
		update_tx_public_stmt := UpdateTxPublicStmt,
		update_tx_bundletxparse_stmt := UpdateTxBundleTxParseStmt,
		select_transaction_group_label_address_stmt := SelectTransactionGroupLabelAddressStmt,
		select_folder_address_stmt := SelectFolderAddressStmt
	} = State,
	?LOG_INFO([{ar_arql_db, terminate}, {reason, Reason}]),
	ar_sqlite3:finalize(InsertBlockStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(InsertTxStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(InsertTagStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(SelectTxByIdStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(SelectBlockByTxIdStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(SelectTagsByTxIdStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(InsertAddressStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(SelectAddressRangeStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(SelectAddressTotalStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(SelectAddressRefereeRangeStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(SelectAddressRefereeTotalStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(SelectAddressAgentRangeStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(SelectAddressAgentTotalStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(SelectAddressProfileRangeStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(SelectAddressProfileMyStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(SelectAddressProfileTotalStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(SelectTransactionRangeStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(SelectTransactionTotalStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(SelectTransactionRangeFilterStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(SelectTransactionTotalFilterStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(SelectTransactionRangeFilterAddressStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(SelectTransactionTotalFilterAddressStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(SelectTransactionRangeFolderAddressStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(SelectTransactionTotalFolderAddressStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(SelectTransactionRangeLabelAddressStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(SelectTransactionTotalLabelAddressStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(SelectTransactionRangeStarAddressStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(SelectTransactionTotalStarAddressStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(UpdateTxLabelStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(UpdateTxStarStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(UpdateTxFolderStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(UpdateTxPublicStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(UpdateTxBundleTxParseStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(SelectTransactionGroupLabelAddressStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(SelectFolderAddressStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:close(Conn, ?DRIVER_TIMEOUT).

%%%===================================================================
%%% Internal functions.
%%%===================================================================

ensure_meta_table_created(Conn) ->
	case sql_fetchone(Conn, "
		SELECT 1 FROM sqlite_master
		WHERE type = 'table' AND name = 'migration'
	", ?DRIVER_TIMEOUT) of
		{row, [1]} -> ok;
		done -> create_meta_table(Conn)
	end.

create_meta_table(Conn) ->
	?LOG_INFO([{ar_arql_db, creating_meta_table}]),
	ok = ar_sqlite3:exec(Conn, ?CREATE_MIGRATION_TABLE_SQL, ?DRIVER_TIMEOUT),
	ok.

ensure_schema_created(Conn) ->
	case sql_fetchone(Conn, "
		SELECT 1 FROM migration
		WHERE name = '20191009160000_schema_created'
	", ?DRIVER_TIMEOUT) of
		{row, [1]} -> ok;
		done -> create_schema(Conn)
	end.

create_schema(Conn) ->
	?LOG_INFO([{ar_arql_db, creating_schema}]),
	ok = ar_sqlite3:exec(Conn, "BEGIN TRANSACTION", ?DRIVER_TIMEOUT),
	ok = ar_sqlite3:exec(Conn, ?CREATE_TABLES_SQL, ?DRIVER_TIMEOUT),
	ok = ar_sqlite3:exec(Conn, ?CREATE_INDEXES_SQL, ?DRIVER_TIMEOUT),
	done = sql_fetchone(Conn, "INSERT INTO migration VALUES ('20191009160000_schema_created', ?)",
			[sql_now()], ?DRIVER_TIMEOUT),
	ok = ar_sqlite3:exec(Conn, "COMMIT TRANSACTION", ?DRIVER_TIMEOUT),
	ok.

sql_fetchone(Conn, SQL, Timeout) -> sql_fetchone(Conn, SQL, [], Timeout).
sql_fetchone(Conn, SQL, Params, Timeout) ->
	{ok, Stmt} = ar_sqlite3:prepare(Conn, SQL, Timeout),
	ok = ar_sqlite3:bind(Stmt, Params, Timeout),
	Result = ar_sqlite3:step(Stmt, Timeout),
	ok = ar_sqlite3:finalize(Stmt, Timeout),
	Result.

sql_fetchall(Conn, SQL, Params, Timeout) ->
	{ok, Stmt} = ar_sqlite3:prepare(Conn, SQL, Timeout),
	Results = stmt_fetchall(Stmt, Params, Timeout),
	ar_sqlite3:finalize(Stmt, Timeout),
	Results.

stmt_fetchall(Stmt, Params, Timeout) ->
	ok = ar_sqlite3:bind(Stmt, Params, Timeout),
	From = self(),
	Ref = make_ref(),
	spawn_link(fun() ->
		From ! {Ref, collect_sql_results(Stmt)}
	end),
	Results =
		receive
			{Ref, R} -> R
		after Timeout ->
			error(timeout)
		end,
	ar_sqlite3:reset(Stmt, Timeout),
	Results.

collect_sql_results(Stmt) -> collect_sql_results(Stmt, []).
collect_sql_results(Stmt, Acc) ->
	case ar_sqlite3:step(Stmt, infinity) of
		{row, Row} -> collect_sql_results(Stmt, [Row | Acc]);
		done -> lists:reverse(Acc)
	end.

sql_now() ->
	calendar:system_time_to_rfc3339(erlang:system_time(second), [{offset, "Z"}]).

select_txs_by_where_clause(Opts) ->
	FromQuery = proplists:get_value(from, Opts, any),
	ToQuery = proplists:get_value(to, Opts, any),
	Tags = proplists:get_value(tags, Opts, []),
	{FromWhereClause, FromParams} = select_txs_by_where_clause_from(FromQuery),
	{ToWhereClause, ToParams} = select_txs_by_where_clause_to(ToQuery),
	{TagsWhereClause, TagsParams} = select_txs_by_where_clause_tags(Tags),
	{WhereClause, WhereParams} = {
		lists:concat([
			FromWhereClause,
			" AND ",
			ToWhereClause,
			" AND ",
			TagsWhereClause
		]),
		FromParams ++ ToParams ++ TagsParams
	},
	{WhereClause, WhereParams}.

select_txs_by_where_clause_from(any) ->
	{"1", []};
select_txs_by_where_clause_from(FromQuery) ->
	{
		lists:concat([
			"tx.from_address IN (",
			lists:concat(lists:join(", ", lists:map(fun(_) -> "?" end, FromQuery))),
			")"
		]),
		FromQuery
	}.

select_txs_by_where_clause_to(any) ->
	{"1", []};
select_txs_by_where_clause_to(ToQuery) ->
	{
		lists:concat([
			"tx.target IN (",
			lists:concat(lists:join(", ", lists:map(fun(_) -> "?" end, ToQuery))),
			")"
		]),
		ToQuery
	}.

select_txs_by_where_clause_tags(Tags) ->
	lists:foldl(fun
		({Name, any}, {WhereClause, WhereParams}) ->
			{
				WhereClause ++ " AND tx.id IN (SELECT tx_id FROM tag WHERE name = ?)",
				WhereParams ++ [Name]
			};
		({Name, Value}, {WhereClause, WhereParams}) ->
			{
				WhereClause ++ " AND tx.id IN (SELECT tx_id FROM tag WHERE name = ? AND value = ?)",
				WhereParams ++ [Name, Value]
			}
	end, {"1", []}, Tags).

address_map([
	Address,
	Balance,
	Txs,
	Sent,
	Received,
	Lastblock,
	Timestamp,
	Profile,
	ChivesDrive,
	ChivesEmail,
	ChivesBlog,
	ChivesMessage,
	ChivesForum,
	ChivesDb,
	Agent,
	Referee,
	Last_tx_action
]) -> #{
	id => Address,
	balance => Balance,
	txs => Txs,
	sent => Sent,
	received => Received,
	lastblock => Lastblock,
	timestamp => Timestamp,
	profile => Profile,
	chivesDrive => ChivesDrive,
	chivesEmail => ChivesEmail,
	chivesBlog => ChivesBlog,
	chivesMessage => ChivesMessage,
	chivesForum => ChivesForum,
	chivesDb => ChivesDb,
	agent => Agent,
	referee => Referee,
	last_tx_action => Last_tx_action
}.

item_label_group_map([Item_label, Number]) -> #{
	item_label => Item_label,
	number => Number
}.

tx_map([
	Id,
	BlockIndepHash,
	LastTx,
	Owner,
	FromAddress,
	Target,
	Quantity,
	Signature,
	Reward,
	Timestamp,
	Height,
	DataSize,
	BundleId,
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
	FileEncrypt,
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
]) -> #{
	id => Id,
	block_indep_hash => BlockIndepHash,
	last_tx => LastTx,
	owner => Owner,
	from_address => FromAddress,
	target => Target,
	quantity => Quantity,
	signature => Signature,
	reward => Reward,
	timestamp => Timestamp,
	block_height => Height,
	data_size => DataSize,
	bundleid => BundleId,
	item_name => FileName,
	item_type => FileType,
	item_parent => FileParent,
	content_type => ContentType,
	item_hash => FileHash,
	item_summary => FileSummary,
	item_star => Item_star,
	item_label => Item_label,
	item_download => Item_download,
	item_language => Item_language,
	item_pages => Item_pages,
	is_encrypt => FileEncrypt,
	is_public => IsPublic,
	entity_type => EntityType,
	app_name => AppName,
	app_version => AppVersion,
	app_instance => AppInstance,
	item_node_label => Item_node_label,
	item_node_group => Item_node_group,
	item_node_star => Item_node_star,
	item_node_hot => Item_node_hot,
	item_node_delete => Item_node_delete,
	last_tx_action => Last_tx_action,
	bundleTxParse => BundleTxParse
}.

block_map([
	IndepHash,
	PreviousBlock,
	Height,
	Timestamp
]) -> #{
	indep_hash => IndepHash,
	previous_block => PreviousBlock,
	height => Height,
	timestamp => Timestamp
}.

tags_map([TxId, Name, Value]) ->
	#{
		tx_id => TxId,
		name => Name,
		value => Value
	}.

eval_legacy_arql_where_clause({equals, <<"from">>, Value})
	when is_binary(Value) ->
	{
		"tx.from_address = ?",
		[Value]
	};
eval_legacy_arql_where_clause({equals, <<"to">>, Value})
	when is_binary(Value) ->
	{
		"tx.target = ?",
		[Value]
	};
eval_legacy_arql_where_clause({equals, Key, Value})
	when is_binary(Key), is_binary(Value) ->
	{
		"tx.id IN (SELECT tx_id FROM tag WHERE name = ? and value = ?)",
		[Key, Value]
	};
eval_legacy_arql_where_clause({'and',E1,E2}) ->
	{E1WhereClause, E1Params} = eval_legacy_arql_where_clause(E1),
	{E2WhereClause, E2Params} = eval_legacy_arql_where_clause(E2),
	{
		lists:concat([
			"(",
			E1WhereClause,
			" AND ",
			E2WhereClause,
			")"
		]),
		E1Params ++ E2Params
	};
eval_legacy_arql_where_clause({'or',E1,E2}) ->
	{E1WhereClause, E1Params} = eval_legacy_arql_where_clause(E1),
	{E2WhereClause, E2Params} = eval_legacy_arql_where_clause(E2),
	{
		lists:concat([
			"(",
			E1WhereClause,
			" OR ",
			E2WhereClause,
			")"
		]),
		E1Params ++ E2Params
	};
eval_legacy_arql_where_clause(_) ->
	throw(bad_query).

full_block_to_fields(FullBlock) ->
	BlockFields = block_to_fields(FullBlock),
	BlockIndepHash = lists:nth(1, BlockFields),
	TxFieldsList = lists:map(
		fun(TX) -> 
			TagsMap = lists:map(
									fun({Name, Value}) ->
										{Name, Value}
									end,
									TX#tx.tags),
			FileName = ar_storage:find_value_in_tags(<<"File-Name">>, TagsMap),
			ContentType = ar_storage:find_value_in_tags(<<"Content-Type">>, TagsMap),
			FileType = ar_storage:contentTypeToFileType(ContentType),
			FileParent = ar_storage:find_value_in_tags(<<"File-Parent">>, TagsMap),
			FileHash = ar_storage:find_value_in_tags(<<"File-Hash">>, TagsMap),
			FileSummary = ar_storage:find_value_in_tags(<<"File-Summary">>, TagsMap),
			CipherALG = ar_storage:find_value_in_tags(<<"Cipher-ALG">>, TagsMap),
			IsPublic = ar_storage:find_value_in_tags(<<"File-Public">>, TagsMap),
			% EntityType = ar_storage:find_value_in_tags(<<"Entity-Type">>, TagsMap),
			AppName = ar_storage:find_value_in_tags(<<"App-Name">>, TagsMap),
			AppVersion = ar_storage:find_value_in_tags(<<"App-Version">>, TagsMap),
			AppInstance = ar_storage:find_value_in_tags(<<"App-Instance">>, TagsMap),
			BundleFormat = ar_storage:find_value_in_tags(<<"Bundle-Format">>, TagsMap),
			EntityType = case BundleFormat of 
							<<"binary">> -> 
								<<"Bundle">>;
							_ -> 
								EntityTypeItem = ar_storage:find_value_in_tags(<<"Entity-Type">>, TagsMap),
								case byte_size(EntityTypeItem) > 0 of
									true ->
										EntityTypeItem;
									false ->
										case byte_size(FileName) > 0 of
											true ->
												"File";
											false ->
												"Tx"
										end
								end
						end,
			?LOG_INFO([{contentType__________________________________, ContentType}]),
			?LOG_INFO([{bundleFormat__________________________________, EntityType}]),
			Bundleid = <<"">>,
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
			Last_tx_action = integer_to_binary(lists:nth(3, BlockFields)),
			BundleTxParse = <<"">>,
			[
				ar_util:encode(TX#tx.id),
				BlockIndepHash,
				ar_util:encode(TX#tx.last_tx),
				ar_util:encode(TX#tx.owner),
				ar_util:encode(ar_wallet:to_address(TX#tx.owner, TX#tx.signature_type)),
				ar_util:encode(TX#tx.target),
				integer_to_binary(TX#tx.quantity div 100000000),
				ar_util:encode(TX#tx.signature),
				TX#tx.reward,
				integer_to_binary(lists:nth(4, BlockFields)),
				integer_to_binary(lists:nth(3, BlockFields)),
				TX#tx.data_size,
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
			]
		end,
		FullBlock#block.txs
	),
	{BlockFields, TxFieldsList}.

block_to_tag_fields_list(B) ->
	lists:flatmap(fun(TX) ->
		EncodedTXID = ar_util:encode(TX#tx.id),
		lists:map(fun({Name, Value}) -> [
			EncodedTXID,
			Name,
			Value
		] end, TX#tx.tags)
	end, B#block.txs).

block_to_fields(B) ->
	[
		ar_util:encode(B#block.indep_hash),
		ar_util:encode(B#block.previous_block),
		B#block.height,
		B#block.timestamp
	].

tx_to_fields(BH, TX, Timestamp, Height) ->
	TagsMap = lists:map(
									fun({Name, Value}) ->
										{Name, Value}
									end,
									TX#tx.tags),
	FileName = ar_storage:find_value_in_tags(<<"File-Name">>, TagsMap),
	ContentType = ar_storage:find_value_in_tags(<<"Content-Type">>, TagsMap),
	FileType = ar_storage:contentTypeToFileType(ContentType),
	FileParent = ar_storage:find_value_in_tags(<<"File-Parent">>, TagsMap),
	FileHash = ar_storage:find_value_in_tags(<<"File-Hash">>, TagsMap),
	FileSummary = ar_storage:find_value_in_tags(<<"File-Summary">>, TagsMap),
	CipherALG = ar_storage:find_value_in_tags(<<"Cipher-ALG">>, TagsMap),
	IsPublic = ar_storage:find_value_in_tags(<<"File-Public">>, TagsMap),
	% EntityType = ar_storage:find_value_in_tags(<<"Entity-Type">>, TagsMap),
	AppName = ar_storage:find_value_in_tags(<<"App-Name">>, TagsMap),
	AppVersion = ar_storage:find_value_in_tags(<<"App-Version">>, TagsMap),
	AppInstance = ar_storage:find_value_in_tags(<<"App-Instance">>, TagsMap),
	BundleFormat = ar_storage:find_value_in_tags(<<"Bundle-Format">>, TagsMap),
	EntityType = case BundleFormat of 
					<<"binary">> -> 
						<<"Bundle">>;
					_ -> 
						EntityTypeItem = ar_storage:find_value_in_tags(<<"Entity-Type">>, TagsMap),
						case byte_size(EntityTypeItem) > 0 of
							true ->
								EntityTypeItem;
							false ->
								case byte_size(FileName) > 0 of
									true ->
										"File";
									false ->
										"Tx"
								end
						end
				end,
	?LOG_INFO([{contentType__________________________________, ContentType}]),
	?LOG_INFO([{bundleFormat__________________________________, EntityType}]),
	Bundleid = <<"">>,
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
	Last_tx_action = Height,
	BundleTxParse = <<"">>,
	[
		ar_util:encode(TX#tx.id),
		ar_util:encode(BH),
		ar_util:encode(TX#tx.last_tx),
		ar_util:encode(TX#tx.owner),
		ar_util:encode(ar_wallet:to_address(TX#tx.owner, TX#tx.signature_type)),
		ar_util:encode(TX#tx.target),
		TX#tx.quantity,
		ar_util:encode(TX#tx.signature),
		TX#tx.reward,
		Timestamp,
		Height,
		TX#tx.data_size,		
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
	].

tx_to_tag_fields_list(TX) ->
	EncodedTXID = ar_util:encode(TX#tx.id),
	lists:map(
		fun({Name, Value}) ->
			[EncodedTXID, Name, Value]
		end,
		TX#tx.tags
	).

record_query_time(Label, TimeUs) ->
	prometheus_histogram:observe(sqlite_query_time, [Label], TimeUs div 1000).
