# Kafka Lab

A local playground for learning Kafka by doing. It pairs a small hands-on lab with Kafka UI, so you can move from a concept to an observable result without writing setup code first.

## Start

```bash
docker compose up --build
```

Open [Kafka Lab](http://localhost:8000) to run exercises and [Kafka UI](http://localhost:8080) to inspect the cluster.

## What you can practice

- Create topics with a chosen partition count and replication factor.
- Produce keyed or unkeyed records and immediately see the selected partition and offset.
- Read a non-destructive snapshot from the earliest retained event; it uses a fresh consumer group and never commits offsets.
- Inspect every partition's start offset, end offset, and retained-record count.
- Follow the built-in lab notebook for keys and ordering, offsets, and producer routing. Completion markers stay in your browser only.
- Use Kafka UI for consumer groups, offset resets, topic configuration, and broker-level exploration.

## First experiment

1. Create `orders.created` with 3 partitions and replication factor 2.
2. Send several events with `customer-42` as the key and observe its partition.
3. Change the key, compare partitions, then read the topic. The preview does not commit an offset.

Stop while retaining data with `docker compose down`. Reset container data deliberately with `docker compose down -v`.

## A useful learning loop

1. Pick an experiment in **Lab Notebook** and choose **Load setup**.
2. Create its topic, then send a few records while changing just one variable (key, partition count, or consumer group).
3. Use **Inspect topic** to compare partition end offsets before and after your change.
4. Open Kafka UI to validate the same behavior at the broker or consumer-group level.

## Learn this guide with Kafka Lab

This README is the course material used by the lab. From the running application, choose **Read course notes** to open this exact document. Work in short loops: read one concept, perform its experiment, inspect the result, then explain what changed in your own words.

| Module | Read in these notes | Perform in Kafka Lab | What to observe |
|---|---|---|---|
| 1. Event streaming | 1–10 | Create `events.demo`, send one record, read it back | A record is retained after it is read. |
| 2. Topics and partitions | 12–19 | Create a topic with 3 partitions; use **Inspect topic** | A topic is split into independent ordered logs. |
| 3. Keys and ordering | 24–25 | Send three records with the same key, then change the key | The same key stays on one partition; ordering is per partition. |
| 4. Offsets and replay | 26–28, 43–45 | Inspect before/after producing; use **Read snapshot** twice | Offsets are positions, and a fresh reader can replay. |
| 5. Consumer groups | 29–35 | Open Kafka UI → Consumers; create/read with one group, then another | A group shares partitions; different groups each get the stream. |
| 6. Reliability | 20–23, 37–42 | Create a topic with replication factor 2; inspect it in Kafka UI | Leaders accept writes; replicas protect against broker loss. |
| 7. Data quality | 46–49 | Send valid and deliberately malformed JSON to a `*.dlq` topic | Validation belongs at a boundary; failures need a deliberate path. |
| 8. Production features | Advanced notes below | Use the checklist and Kafka UI topic configuration pages | Retention, compaction, security, and observability are design choices. |

### Lab rules

1. Use throwaway topic names such as `lab.keys.v1`; this keeps experiments isolated.
2. Change one variable at a time—key, partition count, consumer group, or replication factor.
3. A topic name cannot be reused with different settings. If it already exists, create a new `.v2` topic instead.
4. The lab’s **Read snapshot** deliberately uses a new group and does not commit offsets. It is a safe viewer, not a production consumer.
5. Kafka UI is the companion tool for broker state, consumer groups, topic configuration, and administrative actions.

# Apache Kafka — Complete Practical Notes

> A practical, beginner-friendly guide to Kafka covering Event-Driven Architecture, topics, brokers, partitions, producers, consumers, consumer groups, offsets, replication, acknowledgements, error handling, KRaft, and an end-to-end flow.

## 📚 Table of Contents

- [1. Event-Driven Architecture](#1-event-driven-architecture)
- [2. Why Async Processing?](#2-why-async-processing)
- [3. Message Queue vs Event Streaming](#3-message-queue-vs-event-streaming)
- [4. What Is Kafka?](#4-what-is-kafka)
- [5. Kafka Cluster and Brokers](#5-kafka-cluster-and-brokers)
- [6. Topics](#6-topics)
- [7. Partitions](#7-partitions)
- [8. Producers](#8-producers)
- [9. How a Producer Chooses a Partition](#9-how-a-producer-chooses-a-partition)
- [10. Leader and Follower Replicas](#10-leader-and-follower-replicas)
- [11. Replication and ISR](#11-replication-and-isr)
- [12. Offsets](#12-offsets)
- [13. Consumer Groups](#13-consumer-groups)
- [14. Multiple Consumers and One Partition](#14-multiple-consumers-and-one-partition)
- [15. Acknowledgements (`acks`)](#15-acknowledgements-acks)
- [16. What Happens When Bad Data Enters Kafka?](#16-what-happens-when-bad-data-enters-kafka)
- [17. Dead Letter Queue](#17-dead-letter-queue)
- [18. Schema Validation](#18-schema-validation)
- [19. How Kafka Knows the Leader](#19-how-kafka-knows-the-leader)
- [20. KRaft](#20-kraft)
- [21. Complete End-to-End Flow](#21-complete-end-to-end-flow)
- [22. Practical Example](#22-practical-example)
- [23. Useful Kafka CLI Commands](#23-useful-kafka-cli-commands)
- [24. Quick Revision](#24-quick-revision)

---

## 1. Event-Driven Architecture

In an **Event-Driven Architecture (EDA)**, services communicate by producing and consuming events instead of every service directly calling every other service.

Example:

```text
Order Service
     |
     | ORDER_PLACED
     v
    Kafka
   /  |   \
  v   v    v
Payment Inventory Notification
Service  Service   Service
```

This makes systems more loosely coupled and allows consumers to process work asynchronously.

### Common asynchronous approaches

| Approach | Typical use |
|---|---|
| Application threads | Small background work inside one application |
| Database polling | Periodically check DB for changes |
| Cron jobs | Scheduled batch processing |
| Webhooks | Notify another service through HTTP |
| Serverless / task services | Managed asynchronous execution |
| Message queues | Distribute tasks/jobs |
| Event streaming | Durable event history and multiple consumers |

---

## 2. Why Async Processing?

Suppose an order API needs to:

1. Save an order
2. Charge payment
3. Update inventory
4. Send email
5. Generate analytics

Doing everything synchronously makes the API slow and tightly coupled.

Instead:

```text
POST /orders
     |
     v
Order Service
     |
     | ORDER_PLACED
     v
   Kafka
   / |  \
  v  v   v
Payment Inventory Email
```

The API can finish quickly while downstream services process the event independently.

---

## 3. Message Queue vs Event Streaming

### Message Queue

Think of a message queue as a **to-do list**.

```text
Producer
   |
   v
Queue
   |
   v
Consumer
   |
   X
message removed/acknowledged
```

A message generally represents a job:

```json
{
  "type": "SEND_EMAIL",
  "orderId": "123"
}
```

Once processed, the application usually does not need the message anymore.

### Event Streaming

Think of Kafka as an **immutable log/history**.

```text
Producer
   |
   v
Kafka
   |
   +--> Payment Service
   +--> Analytics Service
   +--> Fraud Service
```

Example:

```json
{
  "event": "ORDER_PLACED",
  "orderId": "123"
}
```

The event remains available according to the topic's retention policy.

Consumers can read at their own pace and can replay old events.

### Key difference

| Message Queue | Event Streaming |
|---|---|
| Task-oriented | Event/history-oriented |
| Usually processed once by a worker | Multiple consumer groups can read independently |
| Message disappears after processing in many queue systems | Event remains for retention period |
| Replay is usually not the primary model | Replay is a core capability |
| Example: send-email job | Example: `ORDER_PLACED` event |

---

## 4. What Is Kafka?

**Apache Kafka is a distributed event streaming platform.**

A simple mental model:

> **Kafka = distributed, durable, append-only log + consumers that track where they are in that log.**

Kafka is commonly used for:

- Event-driven microservices
- Log/event pipelines
- Real-time analytics
- Data integration
- CDC pipelines
- Monitoring
- Stream processing

Kafka is **not primarily a traditional database**. It stores event records durably, but applications normally use databases for current-state queries.

---

## 5. Kafka Cluster and Brokers

A **Kafka cluster** is made up of multiple Kafka brokers.

A broker is a Kafka server/process that stores and serves partition data.

```text
Kafka Cluster
│
├── Broker 1
├── Broker 2
└── Broker 3
```

Why multiple brokers?

- High availability
- Horizontal scaling
- Data replication
- Fault tolerance

### Important terminology

```text
Cluster
  └── Brokers
       └── Partitions
            └── Records
```

---

## 6. Topics

A **topic** is a logical name/category for a stream of related events.

Examples:

```text
orders
payments
shipments
user-events
notifications
```

Suppose we create:

```text
Topic: orders
```

Events could be:

```json
{"event":"ORDER_PLACED","orderId":101}
{"event":"ORDER_PAID","orderId":101}
{"event":"ORDER_SHIPPED","orderId":101}
```

### Important

A topic is a **logical concept**.

The actual records are stored inside the topic's **partitions**.

```text
Topic: orders
   |
   +-- Partition 0
   +-- Partition 1
   +-- Partition 2
```

---

## 7. Partitions

A partition is an **ordered, append-only log**.

Example:

```text
Partition 0

offset 0 → ORDER_PLACED
offset 1 → PAYMENT_RECEIVED
offset 2 → ORDER_PACKED
offset 3 → ORDER_SHIPPED
```

Each record gets a monotonically increasing offset within its partition.

### Ordering

Kafka guarantees ordering **within a partition**, not across the entire topic.

If:

```text
P0 → A B C
P1 → D E F
P2 → G H I
```

Kafka guarantees:

```text
A before B before C
```

but does not provide a global ordering such as:

```text
A B C D E F G H I
```

across all partitions.

---

## 8. Producers

A **producer** publishes records to Kafka.

Example:

```text
Order Service
     |
     | produce ORDER_PLACED
     v
Kafka topic: orders
```

Example record:

```json
{
  "key": "123",
  "event": "ORDER_PLACED",
  "customerId": "C42",
  "amount": 1499
}
```

The producer is responsible for deciding which partition the record should go to.

---

## 9. How a Producer Chooses a Partition

There are several possibilities.

### Case 1: Key is present

A common strategy is:

```text
partition = hash(key) % number_of_partitions
```

Example:

```text
key = orderId = 123

hash(123) % 3 = 1

→ Partition 1
```

This gives an important property:

> The same key is consistently mapped to the same partition, assuming the partitioning setup remains compatible.

Therefore events for the same order can remain ordered relative to one another.

```text
orderId=123
     |
     v
Partition 1

ORDER_PLACED
PAYMENT_RECEIVED
ORDER_SHIPPED
```

### Case 2: No key

Kafka's producer partitioner can distribute records among available partitions according to its configured behavior.

For workloads where ordering by an entity matters, using a meaningful key such as `orderId` is common.

---

## 10. Leader and Follower Replicas

Partitions can be replicated across brokers.

Example:

```text
Topic: orders

Partition 0
   |
   +--> Broker 1  ← Leader
   +--> Broker 2  ← Follower
   +--> Broker 3  ← Follower
```

For a partition:

- **Leader** handles writes and serves reads according to Kafka's protocol/configuration.
- **Followers** replicate the leader's log.

If the leader fails, Kafka can elect another suitable replica as leader.

### Why replicate?

Without replication:

```text
Broker 1
   |
Partition 0
   |
Broker dies
   |
DATA UNAVAILABLE
```

With replication:

```text
Broker 1 ← Leader
Broker 2 ← Replica
Broker 3 ← Replica

Broker 1 dies
     |
     v
Another eligible replica can become leader
```

---

## 11. Replication and ISR

**ISR = In-Sync Replicas**

These are replicas considered sufficiently caught up with the leader according to Kafka's replication rules.

Example:

```text
Partition 0

Broker 1 → Leader
Broker 2 → ISR
Broker 3 → ISR
```

If Broker 2 falls too far behind, it can leave the ISR.

```text
Broker 1 → Leader
Broker 2 → ISR
Broker 3 → Out of Sync
```

ISR matters when using stronger producer acknowledgement settings such as:

```text
acks=all
```

because Kafka can require acknowledgement from the relevant in-sync replicas according to the topic/broker configuration.

---

## 12. Offsets

An **offset** is a monotonically increasing number identifying a record's position within a partition.

Example:

```text
Partition 0

offset 0 → A
offset 1 → B
offset 2 → C
offset 3 → D
```

Offsets are **partition-specific**.

So this is valid:

```text
P0 → offset 0, 1, 2
P1 → offset 0, 1, 2
P2 → offset 0, 1, 2
```

Offset `2` in P0 is a different record from offset `2` in P1.

### Important mental model

> Offset = position in a partition log.

---

## 13. Consumer Groups

A **consumer group** is a set of consumers cooperating to consume partitions of a topic.

Example:

```text
Topic: orders
Partitions: P0 P1 P2

Consumer Group: payment-service

C1 → P0
C2 → P1
C3 → P2
```

Each partition is assigned to at most one consumer within the same consumer group at a time.

### Multiple consumer groups

Different groups can independently consume the same topic.

```text
                    orders
                      |
          +-----------+-----------+
          |                       |
    Payment Group            Analytics Group
       / | \                     / |       C1 C2 C3                  A1 A2 A3
```

Both applications can process the same events independently.

---

## 14. Multiple Consumers and One Partition

### Same consumer group

Suppose:

```text
Partitions = 3
Consumers  = 5
```

Only up to 3 consumers can actively own partitions at once.

```text
C1 → P0
C2 → P1
C3 → P2
C4 → idle
C5 → idle
```

Why?

Because a partition is assigned to only one consumer within a consumer group at a time.

### Different consumer groups

The same partition can absolutely be read by consumers in different groups.

```text
              P0
            /    \
           v      v
       Group A  Group B
          C1       C7
```

This is one of Kafka's most important scaling and fan-out concepts.

---

## 15. Acknowledgements (`acks`)

Producer acknowledgement controls how much confirmation the producer requires from Kafka.

### `acks=0`

Producer does not wait for a broker acknowledgement.

```text
Producer → Kafka
     |
     └── no acknowledgement wait
```

Fast, but weakest delivery guarantee.

### `acks=1`

Leader acknowledges after the record is written to the leader's log according to Kafka's acknowledgement semantics.

```text
Producer → Leader
             |
             └── ACK
```

If the leader fails before replication, durability can be weaker than `acks=all`.

### `acks=all`

The leader waits for the required in-sync replica acknowledgements based on Kafka configuration.

```text
Producer
   |
   v
Leader
   |
   +--> Follower
   +--> Follower
   |
   v
ACK
```

This provides stronger durability, at the cost of additional coordination/latency.

---

## 16. What Happens When Bad Data Enters Kafka?

Kafka is fundamentally a log. Once an event is written, applications generally should not think of Kafka like a database row that can simply be edited in place.

### Strategy 1: Treat events as immutable facts

Suppose:

```json
{
  "orderId": 123,
  "status": "DELIVERED"
}
```

was wrong.

Instead of changing history, publish a correcting event:

```json
{
  "orderId": 123,
  "status": "CANCELLED"
}
```

Consumers build the appropriate state from the event history.

This is related to **event-sourcing thinking**.

### Strategy 2: Validate before producing

Prevent invalid events from entering Kafka.

Common validation:

- Schema validation
- Required fields
- Type checks
- Business rules
- Enum/value validation

Example:

```text
Application
    |
    v
Validate event
    |
    +---- invalid → reject
    |
    +---- valid → Kafka
```

### Strategy 3: Dead Letter Queue

If a consumer cannot process a record:

```text
Main Topic
    |
    v
Consumer
    |
    X processing fails
    |
    v
DLQ Topic
```

The DLQ can retain:

- malformed records
- unexpected values
- processing failures
- poison messages

Later:

```text
DLQ
 |
 +--> inspect
 +--> fix consumer/data
 +--> replay if appropriate
```

---

## 17. Dead Letter Queue

A Kafka DLQ is commonly implemented as another Kafka topic.

Example:

```text
orders
   |
   v
payment-service
   |
   X invalid payment event
   |
   v
orders.dlq
```

A useful DLQ record often contains the original payload plus metadata:

```json
{
  "originalTopic": "orders",
  "originalPartition": 2,
  "originalOffset": 9182,
  "error": "INVALID_PAYMENT_STATE",
  "payload": {
    "orderId": 123
  }
}
```

This makes debugging and controlled replay easier.

---

## 18. Schema Validation

In real systems, producers and consumers need an agreed data contract.

Without a schema:

```text
Producer → { "amount": 100 }
Consumer expects → { "amount": "100" }
```

Problems can appear at runtime.

A schema system can define:

- Field names
- Data types
- Required/optional fields
- Compatibility rules
- Versioning

A common Kafka ecosystem component is **Schema Registry**.

Mental model:

```text
Producer
   |
   v
Schema validation
   |
   v
Kafka
   |
   v
Consumer
```

---

## 19. How Kafka Knows the Leader

Kafka maintains cluster metadata describing things such as:

- Topics
- Partitions
- Leader broker for each partition
- Replicas
- ISR

A producer first obtains metadata from Kafka.

Example:

```text
Producer
   |
   | metadata request
   v
Kafka
   |
   | P1 → Broker 2 is leader
   v
Producer
   |
   | send directly
   v
Broker 2
```

The producer caches metadata and refreshes it when it becomes stale or Kafka reports a leadership change.

There is **no need for a separate load balancer in front of every partition write**.

---

## 20. KRaft

Modern Kafka uses **KRaft** for its metadata management and consensus architecture.

Historically:

```text
Kafka
  |
ZooKeeper
```

Modern Kafka:

```text
Kafka
  |
KRaft controllers
```

KRaft removes Kafka's dependency on ZooKeeper for Kafka's own metadata/cluster management.

A simplified mental model:

```text
Kafka Cluster
│
├── Broker 1
├── Broker 2
├── Broker 3
│
└── Controller quorum
      ├── Controller 1
      ├── Controller 2
      └── Controller 3
```

The controllers manage cluster metadata and participate in the controller quorum.

---

## 21. Complete End-to-End Flow

### Step 1 — Producer starts

Producer connects to a Kafka broker.

```text
Producer
   |
   v
Any reachable Kafka broker
```

It asks for cluster metadata.

### Step 2 — Producer receives metadata

Producer learns information such as:

```text
Topics
Partitions
Leader brokers
Replica information
```

It caches the metadata.

### Step 3 — Producer wants to send an event

Example:

```json
{
  "key": "123",
  "event": "ORDER_PLACED"
}
```

Producer must determine the target partition.

### Step 4 — Producer chooses partition

With a key:

```text
hash("123") % 3 = 1

→ Partition 1
```

### Step 5 — Producer finds partition leader

Metadata says:

```text
Partition 1 → Broker 2
```

So the producer sends the record directly to Broker 2.

### Step 6 — Leader writes the record

Broker 2:

1. Appends record to partition log
2. Assigns an offset
3. Replicates to followers

Example:

```text
P1

offset 0 → ORDER_PLACED
offset 1 → PAYMENT_RECEIVED
offset 2 → ORDER_SHIPPED
```

### Step 7 — Followers replicate

Followers copy the leader's records and maintain their replica logs.

With:

```text
acks=all
```

the producer gets acknowledgement after the required ISR acknowledgement conditions are satisfied.

### Step 8 — Consumer reads

Consumer obtains metadata and knows:

```text
Partition 1 → Broker 2
```

It fetches records and tracks its progress using offsets.

```text
Consumer
   |
   | fetch from P1
   v
offset 0 → processed
offset 1 → processed
offset 2 → next
```

---

## 22. Practical Example

Imagine an e-commerce application.

### Services

```text
order-service
payment-service
inventory-service
notification-service
analytics-service
```

### Topic

```text
orders
```

### Event

```json
{
  "eventId": "evt-1001",
  "eventType": "ORDER_PLACED",
  "orderId": "ORD-123",
  "customerId": "C-42",
  "amount": 2499
}
```

### Producer

`order-service` produces:

```text
key = ORD-123
topic = orders
```

Kafka maps the key to a partition:

```text
hash(ORD-123) % 3 = 1

→ orders-1
```

### Consumers

```text
orders
  |
  +--> payment-service
  |
  +--> inventory-service
  |
  +--> notification-service
  |
  +--> analytics-service
```

Each service can use a separate consumer group.

For example:

```text
payment-group
inventory-group
notification-group
analytics-group
```

Therefore each service can independently receive the same order event.

### Event sequence

```text
ORDER_PLACED
      |
      v
PAYMENT_COMPLETED
      |
      v
ORDER_PACKED
      |
      v
ORDER_SHIPPED
      |
      v
ORDER_DELIVERED
```

The exact architecture depends on the application's business rules; Kafka does not automatically enforce this business state machine.

---

## 23. Useful Kafka CLI Commands

### Create a topic

```bash
kafka-topics.sh   --create   --topic orders   --partitions 3   --replication-factor 3   --bootstrap-server localhost:9092
```

### List topics

```bash
kafka-topics.sh   --list   --bootstrap-server localhost:9092
```

### Describe a topic

```bash
kafka-topics.sh   --describe   --topic orders   --bootstrap-server localhost:9092
```

This helps inspect partition leaders, replicas and ISR.

### Produce messages

```bash
kafka-console-producer.sh   --topic orders   --bootstrap-server localhost:9092
```

### Consume messages

```bash
kafka-console-consumer.sh   --topic orders   --bootstrap-server localhost:9092
```

### Consume from the beginning

```bash
kafka-console-consumer.sh   --topic orders   --from-beginning   --bootstrap-server localhost:9092
```

---

## 24. Quick Revision

### Kafka in one diagram

![Kafka Architecture](docs/images/kafka-architecture.svg)

```text
                    Kafka Cluster
                         |
        +----------------+----------------+
        |                |                |
     Broker 1         Broker 2         Broker 3
        |                |                |
       P0               P1               P2
        |
   ordered log
        |
  offset 0,1,2,3...
```

### Remember these relationships

```text
Kafka Cluster
     |
     +-- Broker
          |
          +-- Topic
               |
               +-- Partition
                    |
                    +-- Record
                         |
                         +-- Offset
```

### Most important rules

1. **Topic = logical stream/category**
2. **Partition = ordered append-only log**
3. **Broker = Kafka server**
4. **Cluster = multiple brokers**
5. **Offset = record position inside a partition**
6. **Ordering is guaranteed within a partition**
7. **Same key commonly maps records to the same partition**
8. **Partition leaders handle writes**
9. **Followers replicate leaders**
10. **ISR = in-sync replicas**
11. **Consumer groups provide parallel processing**
12. **One partition is assigned to at most one consumer in a consumer group at a time**
13. **Different consumer groups can independently read the same topic**
14. **Kafka retains records according to retention configuration**
15. **Kafka is a log/event-streaming platform, not simply a task queue**
16. **Validate data before producing whenever possible**
17. **DLQ topics can isolate records that fail processing**
18. **KRaft provides Kafka's modern metadata/consensus architecture**

---

## 📁 Suggested GitHub Repository Structure

```text
kafka-learning/
│
├── README.md
│
├── docs/
│   └── images/
│       ├── kafka-architecture.svg
│       └── kafka-partition-offset.svg
│
├── examples/
│   ├── producer/
│   └── consumer/
│
├── docker/
│   └── docker-compose.yml
│
└── commands/
    └── kafka-cli.md
```

The SVG diagrams in this repository are intentionally kept as text-based SVG files, so GitHub can render them directly and they remain easy to version-control.

## Advanced notes: the concepts needed for real systems

### 59. Delivery semantics: at-most-once, at-least-once, exactly-once

Delivery semantics describe what can happen when a network, process, or broker fails during processing.

| Mode | Meaning | Typical trade-off |
|---|---|---|
| At-most-once | A record may be lost but is never intentionally retried. | Low latency; loss is possible. |
| At-least-once | A record is retried until processing succeeds. | Durable; duplicates are possible. |
| Exactly-once processing | Kafka coordinates read, processing, and write so output is committed once. | More constraints and configuration; not magic across arbitrary external systems. |

The common starting point is **at-least-once plus idempotent consumers**. An idempotent consumer can safely process the same business event twice—for example, store a processed event ID with a unique constraint before charging a card or sending a state change.

**Lab exercise:** Produce the same JSON event twice to `lab.delivery.v1`. The lab correctly shows two Kafka records: Kafka does not infer that they are business duplicates. Explain where your application would de-duplicate them.

### 60. Producer reliability and idempotence

`acks=0` means the producer does not wait for a broker acknowledgement. `acks=1` waits for the leader. `acks=all` waits for all in-sync replicas and is the normal durability-oriented choice. Strong durability also needs an appropriate `min.insync.replicas`; otherwise `acks=all` alone cannot enforce the availability requirement you expect.

Enable producer idempotence for retry-safe writes from a producer session. It prevents duplicate records caused by producer retries, while preserving ordering per partition. It does not make a consumer's database update idempotent and does not remove the need for an event ID in business data.

**Lab exercise:** Create `lab.reliable.v1` with replication factor 2. In Kafka UI, inspect its replica assignment and topic configuration. Compare what you would accept for telemetry (`acks=1` may be acceptable) versus payments (`acks=all`, idempotence, and careful consumer design).

### 61. Transactions and exactly-once processing (EOS)

A Kafka transaction atomically writes records to one or more partitions and commits the consumed offsets as part of the same transaction. Consumers configured with `isolation.level=read_committed` do not see aborted output. This is especially useful for Kafka Streams read-process-write pipelines.

Transactions do **not** atomically include an arbitrary HTTP call, email, or database write. For those, use patterns such as an outbox table, idempotency keys, a transactional inbox, or compensating actions.

### 62. Retention versus log compaction

Retention answers: “When may old log segments be deleted?” Time- and size-based retention keep an event history for a bounded period.

Compaction answers: “For each key, may Kafka retain only the latest value?” A compacted topic is useful for current state, such as `customer-profile-by-id`. A record with a key and a null value is a tombstone; after its delete retention period, it allows the key to disappear from the compacted log.

Compaction is asynchronous and does not mean old values vanish immediately. A compacted topic can also retain history when it has a retention policy. Never use compaction as a substitute for a database transaction.

**Lab exercise:** Create `lab.profile.v1`. Send multiple records with key `user-42` and different values. The snapshot shows the full append history; then use Kafka UI topic configuration to discuss how compaction would eventually reduce this to the latest keyed state.

### 63. Consumer offsets, commits, and rebalances

A consumer group's committed offset is its durable progress marker for each assigned partition. A consumer normally processes a record and commits progress only after its side effect succeeds. Commit before processing risks loss; commit after processing permits duplicates during a failure—hence idempotency.

A rebalance happens when members join/leave, subscriptions change, or partition counts change. Partitions are revoked and reassigned. Keep poll loops responsive, shut down gracefully, and avoid processing that exceeds `max.poll.interval.ms` without designing for it.

**Lab exercise:** In Kafka UI, create one consumer group for a topic and inspect its offsets. Create another group: it starts with independent progress. Add consumers to the first group and confirm that no partition is assigned to two members of that same group.

### 64. Data contracts, serialization, and Schema Registry

An event is an API contract. JSON is easy to inspect but has weak evolution rules by itself. Avro, Protobuf, and JSON Schema provide explicit fields, types, defaults, and compatibility checks. A Schema Registry stores versions and validates that a new schema is backward, forward, or fully compatible according to the selected policy.

Safe evolution examples: add an optional field with a default; avoid changing a field's meaning; do not rename/remove a field until every relevant reader has migrated. Include stable identifiers, event type, schema version, producer timestamp, and a correlation/trace ID where useful.

**Lab exercise:** Send `{"orderId":"o-1","amount":25}` and then `{"orderId":"o-2","amount":25,"currency":"INR"}`. Decide whether an old consumer can still read the new event. Record the answer in your schema contract before changing code.

### 65. Kafka Connect and the outbox pattern

Kafka Connect runs reusable source and sink connectors: databases, object storage, search indexes, and SaaS systems. A connector is configuration plus a runtime, not application business logic.

The dual-write problem occurs when an application saves to its database and publishes to Kafka separately: one action can succeed while the other fails. The transactional outbox pattern writes both the business change and an outbox row in one database transaction; change-data-capture (often via a Connect connector) publishes the outbox rows reliably.

### 66. Kafka Streams and stream processing

Kafka Streams is a Java library for transformations, filters, joins, aggregations, windows, and stateful processing. It uses Kafka topics for input, output, and fault-tolerant state recovery. A stream is an unbounded sequence of facts; a table is the latest state by key derived from a stream.

Important time concepts: event time (when the event happened), ingestion time (when Kafka received it), and processing time (when your application handled it). Windowed aggregations need a late-event policy and a grace period.

### 67. Security

Use TLS to encrypt traffic, SASL or mTLS to authenticate clients, and ACLs/RBAC to authorize operations. Give each application a separate identity and only the topic/group permissions it needs. Do not put secrets, card numbers, access tokens, or personal data into events without an explicit data-governance policy.

Local Kafka Lab uses plaintext only for learning. Treat that as intentionally unsafe for production.

### 68. Observability and operations

Monitor broker availability, under-replicated partitions, offline partitions, disk usage, controller health, request latency, producer errors/retries, consumer lag, rebalance rate, and throughput. Consumer lag is a symptom, not always an incident: compare it with the business SLA and its rate of change.

Plan partitions before high traffic: increasing partitions later can change key-to-partition mapping and does not create a global order. Size retention for disk capacity and recovery time. Test broker loss, slow consumers, bad messages, and restore/replay procedures before production.

### 69. Production design checklist

Before creating a production topic, answer these questions:

1. What business fact does the event represent, and who owns its contract?
2. What is the key, and what ordering boundary does it protect?
3. How many partitions are required for throughput and consumer parallelism?
4. What retention or compaction behavior is required?
5. What delivery semantics and de-duplication strategy are required?
6. What happens to invalid or permanently failing records?
7. Which schema compatibility rule will be enforced?
8. Which producer/consumer settings provide the intended durability and latency?
9. Which identities and ACLs may read, write, or administer the topic?
10. Which dashboards and alerts prove the system is healthy?

## Final practical sequence

Start with topics, keys, partitions, offsets, and consumer groups in Kafka Lab. Then use Kafka UI to inspect replicas and consumer offsets. Only after those are comfortable, add schemas, idempotency, retries/DLQs, compaction, and stream processing. Kafka becomes manageable when every configuration is tied to a business requirement and an observable experiment.
