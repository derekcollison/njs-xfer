# njs-xfer

njs-xfer is a sample application that demonstrates the ability to use NATS and JetStream to store and retrieve large file assets.

This sample shows how we can break a large file asset into smaller messages and place them in a NATS JetStream stream. We show async publishing with a sliding window to maximize upload speeds. For downloading we show flow control from a JetStream consumer and failure recovery.

## Prerequisites

You will need a nats-server capable of running JetStream. We recommend at least version 2.2.1, or the latest from master.

See https://docs.nats.io/jetstream/jetstream for more information on JetStream.

See https://docs.nats.io/nats-server/running for running a nats-server.

## Usage

```
njs-xfer put <large-file>
njs-xfer get <large-file>
````
