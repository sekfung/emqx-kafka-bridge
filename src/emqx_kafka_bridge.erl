%%--------------------------------------------------------------------
%% Copyright (c) 2018 Wen Jing<wenjing2016@gmail.com>, All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_kafka_bridge).

-include("emqx_kafka_bridge.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("brod/include/brod_int.hrl").


-import(string, [concat/2]).
-import(lists, [nth/2]).
-export([load/1, unload/0]).

%% Hooks functions

-export([on_client_connected/3, on_client_disconnected/4]).

-export([on_client_subscribe/4, on_client_unsubscribe/4]).

-export([on_session_created/3, on_session_subscribed/4, on_session_unsubscribed/4, on_session_terminated/4]).

-export([on_message_publish/2, on_message_delivered/3, on_message_acked/3]).

-export([description/0, register_metrics/0]).

-spec(register_metrics() -> ok).
register_metrics() ->
  lists:foreach(fun emqx_metrics:ensure/1, ?BRIDGE_METRICS).

%% Called when the plugin application start
load(Env) ->
  brod_init([Env]),
  emqx:hook('client.connected', fun ?MODULE:on_client_connected/3, [Env]),
  emqx:hook('client.disconnected', fun ?MODULE:on_client_disconnected/4, [Env]),
  emqx:hook('client.subscribe', fun ?MODULE:on_client_subscribe/4, [Env]),
  emqx:hook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/4, [Env]),
  emqx:hook('session.created', fun ?MODULE:on_session_created/3, [Env]),
  emqx:hook('session.subscribed', fun ?MODULE:on_session_subscribed/4, [Env]),
  emqx:hook('session.unsubscribed', fun ?MODULE:on_session_unsubscribed/4, [Env]),
  emqx:hook('session.terminated', fun ?MODULE:on_session_terminated/4, [Env]),
  emqx:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]),
  emqx:hook('message.delivered', fun ?MODULE:on_message_delivered/3, [Env]),
  emqx:hook('message.acked', fun ?MODULE:on_message_acked/3, [Env]).

brod_init(_Env) ->
  % broker 代理服务器的地址
  {ok, BootstrapBrokers} = get_bootstrap_brokers(),
%%  % data points 数据流主题及策略
%%  {ok, DpTopic, _, _} = get_points_topic(),
%%  % device status 设备状态流主题及策略
%%  {ok, DsTopic, _, _} = get_status_topic(),

  ok = brod:start(),

  % socket error recovery
  ClientConfig =
    [
      {reconnect_cool_down_seconds, 10},
      %% avoid api version query in older version brokers. needed with kafka 0.9.x or earlier.
      % {query_api_version, false},

      %% Auto start producer with default producer config
      {auto_start_producers, true},
      %%
      {default_producer_config, []},

      %% disallow
      {allow_topic_auto_creation, false}
    ],

  ok = brod:start_client(BootstrapBrokers, brod_client_1, ClientConfig),
  % Start a Producer on Demand
  %ok = brod:start_producer(brod_client_1, DpTopic, _ProducerConfig = []),
  %ok = brod:start_producer(brod_client_1, DsTopic, _ProducerConfig = []),
  ?LOG(info, "Init brod kafka client with ~p", [BootstrapBrokers]).

on_client_connected(Client = #{
  clientid := ClientId,
  peerhost := {IPA, IPB, IPC, IPD},
  username := Username}, _Conn = #{
  connected_at := ConnectedAt
}, _Env) ->
  ?LOG(info, "Client ~s connected.", [ClientId]),
  IPAddress = io_lib:format("~p\.~p\.~p\.~p", [IPA, IPB, IPC, IPD]),
  Json = mochijson2:encode([
    {type, <<"connected">>},
    {clientid, ClientId},
    {username, Username},
    {client_ip_address, unicode:characters_to_binary(IPAddress)},
    {cluster_node, node()},
    {ts, ConnectedAt}
  ]),
  ok = produce_device_status(ClientId, Username, Json),
  {ok, Client}.

on_client_disconnected(_Client = #{
  clientid := ClientId,
  peerhost := {IPA, IPB, IPC, IPD},
  username := Username
}, Reason,
    _Conn = #{
      connected_at := ConnectedAt,
      disconnected_at := DisconnectedAt
    }, _Env) ->
  ?LOG(info, "Client ~s disconnected, reason: ~w", [ClientId, Reason]),
  IPAddress = io_lib:format("~p\.~p\.~p\.~p", [IPA, IPB, IPC, IPD]),
  Json = mochijson2:encode([
    {type, <<"disconnected">>},
    {clientid, ClientId},
    {username, Username},
    {cluster_node, node()},
    {client_ip_address, unicode:characters_to_binary(IPAddress)},
    {reason, Reason},
    {ts, ConnectedAt},
    {disconnected_at, DisconnectedAt}
  ]),
  ok = produce_device_status(ClientId, Username, Json),
  ok.

on_client_subscribe(#{clientid := ClientId}, _Properties, TopicFilters, _Env) ->
  ?LOG(info, "Client(~s) will subscribe: ~p~n", [ClientId, TopicFilters]),
  {ok, TopicFilters}.

on_client_unsubscribe(#{clientid := ClientId}, _Properties, TopicFilters, _Env) ->
  ?LOG(info, "Client(~s) will unsubscribe ~p~n", [ClientId, TopicFilters]),
  {ok, TopicFilters}.

%%--------------------------------------------------------------------
%% Session Lifecircle Hooks
%%--------------------------------------------------------------------

on_session_created(#{clientid := ClientId}, SessInfo, _Env) ->
  ?LOG(info, "Session(~s) created, Session Info:~n~p~n", [ClientId, SessInfo]).

on_session_subscribed(#{clientid := ClientId}, Topic, SubOpts, _Env) ->
  ?LOG(info, "Session(~s) subscribed ~s with subopts: ~p~n", [ClientId, Topic, SubOpts]).

on_session_unsubscribed(#{clientid := ClientId}, Topic, Opts, _Env) ->
  ?LOG(info, "Session(~s) unsubscribed ~s with opts: ~p~n", [ClientId, Topic, Opts]).


on_session_terminated(_ClientInfo = #{clientid := ClientId}, Reason, SessInfo, _Env) ->
  ?LOG(info, "Session(~s) is terminated due to ~p~nSession Info: ~p~n",
    [ClientId, Reason, SessInfo]).

%%--------------------------------------------------------------------
%% Message PubSub Hooks
%%--------------------------------------------------------------------

%% transform message and return
on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
  ?LOG(info, "Message Publish (~s) : ~p", [Message]),
  {ok, Message};

on_message_publish(Message = #message{
  id = MessageId,
  from = ClientId,
  qos = QoS,
  topic = Topic,
  payload = Payload,
  flags = #{retain := Retain},
  headers = #{},
  timestamp = Timestamp
}, _Env) ->
  ?LOG(info, "Publish ~s~n", [emqx_message:format(Message)]),
  Json = mochijson2:encode([
    {type, <<"published">>},
    {message_id, MessageId},
    {clientid, ClientId},
    {topic, Topic},
    {payload, Payload},
    {qos, QoS},
    {retain, Retain},
    {cluster_node, node()},
    {ts, Timestamp}
  ]),
  ok = produce_device_points(Topic, ClientId, Json),
  {ok, Message}.

on_message_delivered(_ClientInfo = #{clientid := ClientId}, Message, _Env) ->
  ?LOG(info, "Message delivered to client(~s): ~s~n",
    [ClientId, emqx_message:format(Message)]),
  {ok, Message}.

on_message_acked(_ClientInfo = #{clientid := ClientId}, Message, _Env) ->
  ?LOG(info, "Message acked by client(~s): ~s~n",
    [ClientId, emqx_message:format(Message)]).

produce_device_points(Topic, ClientId, Json) ->
  %% kafka topic legal char is [a-zA-Z0-9\\._\\-], CAN NOT contain '/'
  TopicResult = lists:flatten(string:replace(lists:flatten(binary_to_list(Topic)), "/", "_", all)),
  {ok, PartitionStrategy, PartitionWorkers} = get_points_topic(),
  TopicInfo = {ok, unicode:characters_to_binary(TopicResult), PartitionStrategy, PartitionWorkers},
  produce(TopicInfo, ClientId, Json),
  ok.

produce_device_status(ClientId, UserName, Json) ->
  {ProductKey, DeviceName} = getProductKeyAndDeviceName(UserName),
  %% custom topic for post device status
  Topic = io_lib:format("_sys_~s_~s_thing_event_status_post", [ProductKey, DeviceName]),
  {ok, PartitionStrategy, PartitionWorkers} = get_status_topic(),
  TopicInfo = {ok, unicode:characters_to_binary(Topic), PartitionStrategy, PartitionWorkers},
  produce(TopicInfo, ClientId, Json),
  ok.

getProductKeyAndDeviceName(UserName) ->
  UserNameResult = string:tokens(lists:flatten(io_lib:format("~s", [UserName])), "&"),
  ProductKey = lists:nth(2, UserNameResult),
  DeviceName = lists:nth(1, UserNameResult),
  {ProductKey, DeviceName}.

produce(TopicInfo, ClientId, Json) ->
  case TopicInfo of
    {ok, Topic, custom, _} ->
      brod_produce(Topic, hash, ClientId, Json);
    {ok, Topic, _, _} ->
      brod_produce(Topic, random, ClientId, Json)
  end.

brod_produce(Topic, Partitioner, ClientId, Json) ->
  {ok, CallRef} = brod:produce(brod_client_1, Topic, Partitioner, ClientId, list_to_binary(Json)),
  receive
    #brod_produce_reply{call_ref = CallRef, result = brod_produce_req_acked} -> ok
  after 5000 ->
    ?LOG(error, "Produce message to ~p for ~p timeout.", [Topic, ClientId])
  end,
  ok.

%% 从配置中获取当前Kafka的初始broker配置
get_bootstrap_brokers() ->
  application:get_env(?APP, bootstrap_brokers).

get_config_prop_list() ->
  application:get_env(?APP, config).

get_instrument_config() ->
  {ok, Values} = get_config_prop_list(),
  Instrument = proplists:get_value(instrument, Values),
  {ok, Instrument}.

%% 从配置中获取设备数据流主题Points的配置
get_points_topic() ->
  {ok, Values} = application:get_env(?APP, points),
  get_topic(Values).

%% 从配置中获取设备状态流主题Status的配置
get_status_topic() ->
  {ok, Values} = application:get_env(?APP, status),
  get_topic(Values).

get_topic(Values) ->
%%  Topic = proplists:get_value(topic, Values),
  PartitionStrategy = proplists:get_value(partition_strategy, Values),
  PartitionWorkers = proplists:get_value(partition_workers, Values),
  {ok, PartitionStrategy, PartitionWorkers}.

%% Called when the plugin application stop
unload() ->
  ?LOG(info, "Unhooking the emq callbacks."),
  emqx:unhook('client.connected', fun ?MODULE:on_client_connected/3),
  emqx:unhook('client.disconnected', fun ?MODULE:on_client_disconnected/4),
  emqx:unhook('client.subscribe', fun ?MODULE:on_client_subscribe/4),
  emqx:unhook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/4),
  emqx:unhook('session.created', fun ?MODULE:on_session_created/3),
  emqx:unhook('session.subscribed', fun ?MODULE:on_session_subscribed/4),
  emqx:unhook('session.unsubscribed', fun ?MODULE:on_session_unsubscribed/4),
  emqx:unhook('session.terminated', fun ?MODULE:on_session_terminated/4),
  emqx:unhook('message.publish', fun ?MODULE:on_message_publish/2),
  emqx:unhook('message.delivered', fun ?MODULE:on_message_delivered/3),
  emqx:unhook('message.acked', fun ?MODULE:on_message_acked/3),
  ?LOG(info, "Stopping brod kafka client."),
  % It is ok to leave brod application there.
  brod:stop_client(brod_client_1),
  ?LOG(info, "Finished all unload works.").



description() -> "EMQ X Kafka Bridge Plugin".