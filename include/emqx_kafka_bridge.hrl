-define(APP, emqx_kafka_bridge).
-record(bridge_metrics, {
  message_send = 'bridge.mqtt.message_sent',
  message_received = 'bridge.mqtt.message_received'
}).

-define(METRICS(Type), tl(tuple_to_list(#Type{}))).
-define(METRICS(Type, K), #Type{}#Type.K).

-define(BRIDGE_METRICS, ?METRICS(bridge_metrics)).

