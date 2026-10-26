# go-rabbitmq: connection never reconnects after a reconnect race

A reproduction for a bug in [wagslane/go-rabbitmq](https://github.com/wagslane/go-rabbitmq) v0.16.1 (amqp091-go v1.12.0).

TL;DR: if a new connection dies right after a reconnect,
go-rabbitmq [starts watching it too late](https://github.com/wagslane/go-rabbitmq/blob/3ba1c62ee9431192dee896b96ba5e6f4ebb169e9/internal/connectionmanager/connection_manager.go#L177)
and takes the already closed connection for a [graceful close](https://github.com/wagslane/go-rabbitmq/blob/3ba1c62ee9431192dee896b96ba5e6f4ebb169e9/internal/connectionmanager/connection_manager.go#L141-L143).
It stops reconnecting and every publish fails until the process restarts.

## Run

Needs Go and Docker.
The program starts RabbitMQ 4.3.7 with testcontainers-go.

```sh
make run
```

## Repro output

```
2026/10/27 10:37:20.224195 1. connection #1 is up, publishing works
2026/10/27 10:37:20.224439 2. a publish gets stuck writing to connection #1
2026/10/27 10:37:20.224510 3. break connection #1, go-rabbitmq reconnects with connection #2
2026/10/27 10:37:20.224547 gorabbit ERROR: attempting to reconnect to amqp server after connection close with error: Exception (501) Reason: "read tcp [::1]:62269->[::1]:56365: use of closed network connection"
2026/10/27 10:37:20.224591 gorabbit INFO: waiting 157.097917ms to attempt to reconnect to amqp server
2026/10/27 10:37:20.386819 4. break connection #2
2026/10/27 10:37:20.386948 5. let the stuck publish return
2026/10/27 10:37:20.386977 6. publish every second
2026/10/27 10:37:20.386994 stuck publish returned: write tcp [::1]:62269->[::1]:56365: use of closed network connection
2026/10/27 10:37:20.387018 gorabbit WARN: error closing connection while reconnecting: Exception (504) Reason: "channel/connection is not open"
2026/10/27 10:37:20.387035 gorabbit ERROR: attempting to reconnect to amqp server after close with error: Exception (501) Reason: "read tcp [::1]:62269->[::1]:56365: use of closed network connection"
2026/10/27 10:37:20.387060 gorabbit INFO: amqp connection closed gracefully
2026/10/27 10:37:20.387066 gorabbit INFO: waiting 177.523764ms to attempt to reconnect to amqp server
2026/10/27 10:37:20.387057 gorabbit WARN: successfully reconnected to amqp server
2026/10/27 10:37:20.387110 gorabbit INFO: successful connection recovery from: Exception (501) Reason: "read tcp [::1]:62269->[::1]:56365: use of closed network connection"
2026/10/27 10:37:20.565658 gorabbit ERROR: error reconnecting to amqp server: Exception (504) Reason: "channel/connection is not open"
2026/10/27 10:37:20.565767 gorabbit INFO: waiting 306.856743ms to attempt to reconnect to amqp server
2026/10/27 10:37:20.873265 gorabbit ERROR: error reconnecting to amqp server: Exception (504) Reason: "channel/connection is not open"
2026/10/27 10:37:20.873355 gorabbit INFO: waiting 679.1533ms to attempt to reconnect to amqp server
2026/10/27 10:37:21.388083 publish: Exception (504) Reason: "channel/connection is not open" IsClosed: true dials: 2
2026/10/27 10:37:21.553598 gorabbit ERROR: error reconnecting to amqp server: Exception (504) Reason: "channel/connection is not open"
2026/10/27 10:37:21.553734 gorabbit INFO: waiting 1.22036669s to attempt to reconnect to amqp server
2026/10/27 10:37:22.388453 publish: Exception (504) Reason: "channel/connection is not open" IsClosed: true dials: 2
...
```

It logs like that forever.
The dial counter stays at 2.

## Probable cause

go-rabbitmq starts watching a new connection
only [after `reconnect()` returns](https://github.com/wagslane/go-rabbitmq/blob/3ba1c62ee9431192dee896b96ba5e6f4ebb169e9/internal/connectionmanager/connection_manager.go#L177).
If the connection dies before that, amqp091-go has already [set `noNotify`](https://github.com/rabbitmq/amqp091-go/blob/672e67156f27f7333ff848f3d8302bbab99f2936/connection.go#L816),
so `NotifyClose` [closes the receiver immediately](https://github.com/rabbitmq/amqp091-go/blob/672e67156f27f7333ff848f3d8302bbab99f2936/connection.go#L436-L447).
The watcher reads `nil`, takes it for a [graceful close](https://github.com/wagslane/go-rabbitmq/blob/3ba1c62ee9431192dee896b96ba5e6f4ebb169e9/internal/connectionmanager/connection_manager.go#L141-L143)
and returns. Nobody reconnects.

Normally the window is microseconds. The repro widens it:

1. A publish blocks writing to the socket while holding the channel lock
   ([`PublishWithDeferredConfirm`](https://github.com/rabbitmq/amqp091-go/blob/672e67156f27f7333ff848f3d8302bbab99f2936/channel.go#L1654-L1655)).
2. Connection #1 fails. Its [`shutdown`](https://github.com/rabbitmq/amqp091-go/blob/672e67156f27f7333ff848f3d8302bbab99f2936/connection.go#L748-L824)
   [holds `c.m`](https://github.com/rabbitmq/amqp091-go/blob/672e67156f27f7333ff848f3d8302bbab99f2936/connection.go#L759-L760)
   while it [shuts down each channel](https://github.com/rabbitmq/amqp091-go/blob/672e67156f27f7333ff848f3d8302bbab99f2936/connection.go#L805-L807),
   which [needs the channel lock](https://github.com/rabbitmq/amqp091-go/blob/672e67156f27f7333ff848f3d8302bbab99f2936/channel.go#L121-L122).
   The shutdown stalls with `c.m` held.
3. `reconnect()` dials connection #2, then [closes #1](https://github.com/wagslane/go-rabbitmq/blob/3ba1c62ee9431192dee896b96ba5e6f4ebb169e9/internal/connectionmanager/connection_manager.go#L199-L201).
   That [needs `c.m`](https://github.com/rabbitmq/amqp091-go/blob/672e67156f27f7333ff848f3d8302bbab99f2936/connection.go#L473-L474) and blocks.
4. Connection #2 dies during the wait.
5. The stuck publish returns, everything unwinds, and the watcher subscribes to the dead #2.
