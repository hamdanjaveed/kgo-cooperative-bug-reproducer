# kgo-cooperative-sticky-bug-reproducer

This repository is a minimal reproduction of a consumer group rebalancing bug present in franz-go between at least
`v1.14.3 - v1.16.1` (possibly outside these versions as well but that hasn't been tested). We've observed this behaviour
on Kafka v2.8.1 as well as the `confluent-local` and RedPanda docker containers spun up for this reproduction.

## What
The bug causes partitions to either be double consumed or never consumed after a consumer group rebalance, leading to
stuck partitions.

## How
Say we have the following consumers with the corresponding list of partition assignors:

* Consumer `both`: `[cooperative-sticky, range]`
* Consumer `range`: `[range]`
* Consumer `cooperative-sticky`: `[cooperative-sticky]`

### Pre-requisites
This repository requires `go v1.22.0+`. 

Start the Kafka cluster by running:

```shell
go run . kafka
```

This will print a line like `Running Kafka on localhost:12345`. Use the port number printed as the `<kafka-port>` down
below.

Then start a producer by running:

```shell
go run . producer -p <kafka-port> [-d <optional msg production delay>]
```

This producer will publish events evenly to all 16 topics of a `test-topic` in the Kafka cluster.

### Bug
Start the `range` consumer by running:

```shell
go run . range -p <kafka-port>
```

Then start the `both` consumer by running:

```shell
go run . both -p <kafka-port>
```

This should trigger a rebalance and everything should be working as expected: both consumers are consuming from
exactly half of the partitions.

To trigger the bug, stop the `range` consumer. The `both` consumer will stop consuming from half of its partitions.
