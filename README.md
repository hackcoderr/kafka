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

## Apache Kafka — Complete Notes
1. First understand the big picture: Event-Driven Architecture

In a traditional application, one service often directly calls another:

Order Service
     |
     | HTTP request
     ↓
Payment Service
     |
     | HTTP request
     ↓
Notification Service

This creates tight coupling.

In an Event-Driven Architecture (EDA), a service produces an event and other services react to it.

             ┌───────────────┐
             │  Order Service│
             └───────┬───────┘
                     │
               ORDER_PLACED
                     │
                     ↓
              ┌─────────────┐
              │    Kafka    │
              └──────┬──────┘
                     │
        ┌────────────┼─────────────┐
        ↓            ↓             ↓
   Payment       Inventory     Notification
   Service        Service        Service

The important idea is:

Producer doesn't need to know who will consume the event.

That's the main benefit of event-driven architecture.

2. Different ways to perform asynchronous work

Your screenshot lists several approaches.

2.1 Application Threads

Example:

new Thread(() -> {
    sendEmail();
}).start();

Suppose:

User places order
       ↓
Application creates thread
       ↓
Send email
Problem

The work lives inside the application process.

If your application crashes:

Application
    ↓
Thread running
    ↓
💥 Application crashes

Email job → LOST

So application threads are:

Simple
Fast
Good for small background work
Not reliable for important distributed jobs
3. Database as a background-job mechanism

You can store jobs in a database.

Example:

jobs table

id | type       | status
---|------------|--------
1  | SEND_EMAIL | PENDING
2  | SEND_SMS   | PENDING
3  | SEND_EMAIL | PENDING

Worker periodically checks:

SELECT * FROM jobs
WHERE status = 'PENDING';

Then:

Database
    ↓
Worker
    ↓
Process job
    ↓
UPDATE status = DONE
Advantages
Reliable
Data survives application restart
Easy to understand
Problems

If you have 100 workers continuously asking:

"Any new jobs?"
"Any new jobs?"
"Any new jobs?"

you get polling overhead.

And at very large scale, the database can become the bottleneck.

4. Cron Jobs

Cron is basically:

"Run this task at a scheduled time."

Example:

Every day at 2 AM
        ↓
Generate daily report

Linux example:

0 2 * * * generate-report.sh

Good for:

Daily reports
Cleanup
Backups
Scheduled jobs

Not ideal for:

"Something happened. React immediately."

For example:

Payment successful
       ↓
Send notification immediately

Cron is the wrong abstraction.

5. Webhooks

Webhook = HTTP callback.

Imagine:

Stripe
   |
   | HTTP POST
   ↓
Your application

For example:

{
  "event": "payment.success",
  "paymentId": "123"
}

Very useful when another system needs to notify you.

Problem

The receiver must be available.

Stripe
   ↓
HTTP request
   ↓
Your server ❌ DOWN

Now you need retry mechanisms, idempotency, signatures, etc.

6. Serverless / Task Services

Example:

API
 ↓
Task Queue
 ↓
Worker / Lambda

Cloud providers can manage much of the infrastructure.

Useful when you don't want to operate your own worker infrastructure.

7. Message Queue

Think:

"I have work that needs to be done."

Example:

Order Service
     |
     | SEND_EMAIL
     ↓
 Message Queue
     |
     ↓
Email Worker

Message:

{
  "type": "SEND_EMAIL",
  "orderId": "123"
}

Worker processes it:

Message
   ↓
Send email
   ↓
Message removed/acknowledged

The mental model is:

To-do list

8. Event Streaming

Kafka belongs here.

Think:

"Something happened."

Example:

{
  "event": "ORDER_PLACED",
  "orderId": "123"
}

Kafka stores the event for a configured retention period.

Multiple applications can consume it.

                    ┌── Payment Service
                    │
ORDER_PLACED → Kafka├── Inventory Service
                    │
                    └── Analytics Service

And importantly:

Payment Service
      ↓
reads event

Inventory Service
      ↓
reads same event

Analytics Service
      ↓
reads same event

They don't have to consume it at exactly the same time.

9. Message Queue vs Event Streaming

This distinction is very important.

Message Queue

Mental model:

To-do list

Queue

[M1] [M2] [M3] [M4]

Worker A → M1
Worker B → M2
Worker C → M3

Generally, a message is processed by one worker in a competing-consumer setup.

After successful processing, the message is acknowledged and eventually removed according to the queue semantics.

Kafka / Event Stream

Mental model:

Immutable history / log

Partition

Offset
  0       1       2       3       4
  ↓       ↓       ↓       ↓       ↓
[M1]    [M2]    [M3]    [M4]    [M5]

The events remain available according to the topic's retention policy.

Consumer A:

M1 → M2 → M3

Consumer B:

M1 → M2

Consumer C could start later:

M1 → M2 → M3 → M4

That's why Kafka is much more than simply "a queue."

10. What exactly is Kafka?

A useful definition:

Apache Kafka is a distributed event-streaming platform used to publish, store, process, and consume streams of records at scale.

Think of Kafka as a distributed, durable, append-only log.

11. Kafka's main components

You need to understand these words extremely well:

Producer
Topic
Partition
Broker
Cluster
Consumer
Consumer Group
Offset
Leader
Follower
Replication
ISR
Controller / KRaft

Let's go one by one.

12. Producer

Producer = application that sends records to Kafka.

Example:

Order Service
     |
     | ORDER_PLACED
     ↓
   Kafka

Code conceptually:

producer.send(
    new ProducerRecord<>("orders", "123", orderData)
);

Here:

orders = topic
123    = key
orderData = value
13. Topic

A topic is a logical name/category for a stream of records.

Example:

orders
payments
shipments
user-events
notifications

You can think:

Topic: orders

ORDER_PLACED
ORDER_CANCELLED
ORDER_SHIPPED
ORDER_DELIVERED
Important correction

Saying:

"Topic is a set of similar events"

is a good beginner mental model.

But technically:

A topic is a logical category/name for a stream of records and is divided into one or more partitions.

Partitions actually contain the records.

The topic itself isn't a physical storage location.

14. Partition

This is one of the most important Kafka concepts.

Suppose:

Topic = orders

has 3 partitions:

orders
│
├── Partition 0
├── Partition 1
└── Partition 2

Each partition is an ordered append-only log.

Example:

Partition 0

Offset
  0       1       2       3
  ↓       ↓       ↓       ↓
 O1      O4      O7      O9

Another partition:

Partition 1

  0       1       2
  ↓       ↓       ↓
 O2      O5      O8

Another:

Partition 2

  0       1       2
  ↓       ↓       ↓
 O3      O6      O10
15. Ordering in Kafka

Kafka guarantees ordering within a partition.

Suppose:

Partition 0

M1 → M2 → M3 → M4

A consumer reading this partition will see:

M1
M2
M3
M4

in that order.

But if:

M1 → Partition 0
M2 → Partition 1

Kafka does not provide a global ordering guarantee across the two partitions.

Remember:

Ordering = partition-level, not topic-level.

16. Why do we need partitions?

Because one machine cannot necessarily handle all the traffic.

Suppose:

1 partition
1 consumer

and you have:

10 million events/sec

You need parallelism.

So:

orders topic

P0
P1
P2
P3
P4
P5

Different partitions can be processed in parallel.

That's where Kafka gets its scalability.

17. Broker

A Kafka broker is a Kafka server.

Suppose you have:

Kafka Cluster

Broker 1
Broker 2
Broker 3

Each broker is a machine/process running Kafka.

A broker can store partitions.

For example:

Broker 1
 ├── orders-P0
 └── payments-P1

Broker 2
 ├── orders-P1
 └── payments-P0

Broker 3
 ├── orders-P2
 └── payments-P2
18. Kafka Cluster

A Kafka cluster is a group of Kafka brokers working together.

              Kafka Cluster
        ┌────────┼────────┐
        ↓        ↓        ↓
     Broker 1 Broker 2 Broker 3

Why multiple brokers?

Scalability
Fault tolerance
Replication
Parallel processing
High availability
19. Topic → Partition → Broker

This hierarchy is crucial.

Kafka Cluster
     │
     ├── Broker 1
     │
     ├── Broker 2
     │
     └── Broker 3

Topic: orders
     │
     ├── Partition 0
     ├── Partition 1
     └── Partition 2

But partitions are hosted on brokers.

For example:

orders

P0 → Broker 1
P1 → Broker 2
P2 → Broker 3

And with replication:

P0:
Leader   → Broker 1
Follower → Broker 2
Follower → Broker 3
20. Replication

Suppose:

Topic: orders
Partition 0

Replication factor = 3.

Kafka may store:

Broker 1 → P0 Leader
Broker 2 → P0 Follower
Broker 3 → P0 Follower

So there are 3 copies of the partition's data.

If Broker 1 dies:

Broker 1 ❌

Broker 2 → becomes leader
Broker 3 → follower

This provides fault tolerance.

21. Leader and Followers

For every partition, one replica is the leader.

The others are followers.

Example:

Partition 0

Broker 1
   ↓
 LEADER

Broker 2
   ↓
 FOLLOWER

Broker 3
   ↓
 FOLLOWER

Producer normally sends records to the leader of the partition.

The leader coordinates writes and replication to followers.

22. What happens when Producer sends a message?

Let's use a real example.

Suppose:

{
  "orderId": 123,
  "customer": "Sachin",
  "amount": 999
}

Producer wants to publish:

Topic = orders
Key = 123

The flow is roughly:

Producer
   |
   | 1. Get metadata
   ↓
Kafka Broker
   |
   | "Partition 1 leader is Broker 2"
   ↓
Producer
   |
   | 2. Send directly
   ↓
Broker 2
   |
   | 3. Write to Partition 1
   ↓
Followers replicate
23. Does Producer send to any broker?

At startup, the producer can connect to one or more configured bootstrap servers.

It asks Kafka for metadata.

Producer
   |
   | "Tell me about cluster metadata"
   ↓
Broker
   |
   ↓
Metadata

Topics
Partitions
Leaders
Replicas

Then the producer knows:

orders-P0 → Broker 1
orders-P1 → Broker 2
orders-P2 → Broker 3

So the producer can send the record to the appropriate broker.

Important

There isn't normally some central load balancer sitting in front of every Kafka write.

The Kafka client uses metadata to route requests to the appropriate broker.

24. How does Producer choose the partition?

This depends on the record.

Case 1 — Key is present

Suppose:

key = orderId

Conceptually:

partition = hash(key) % number_of_partitions

For example:

key = 123
partitions = 3

hash(123) % 3 = 1

Therefore:

order 123 → Partition 1

This is useful because the same key is generally mapped to the same partition while the partitioning setup remains appropriate.

Why is this useful?

Suppose order 123 produces:

ORDER_CREATED
ORDER_PAID
ORDER_SHIPPED
ORDER_DELIVERED

If all use the same key:

orderId = 123

they can land in the same partition.

Therefore:

P1

ORDER_CREATED
      ↓
ORDER_PAID
      ↓
ORDER_SHIPPED
      ↓
ORDER_DELIVERED

Their order can be preserved within that partition.

25. What if there is no key?

Kafka's producer partitioner can distribute records among partitions according to the producer's partitioning behavior/configuration.

The important interview-level point is:

Key present
    ↓
partition chosen based on key

No key
    ↓
producer distributes records across available partitions

Don't memorize "no key always means random" as a universal rule—the exact behavior depends on Kafka client/version/partitioner.

26. Offset

An offset is:

A monotonically increasing number identifying a record's position within a partition.

Example:

Partition 0

Offset
  0       1       2       3       4
  ↓       ↓       ↓       ↓       ↓
  M1      M2      M3      M4      M5

Important:

Offset is unique only within a partition.

So:

Partition 0 → offset 10
Partition 1 → offset 10

can both exist.

They are different records because their partitions differ.

A record is effectively identified by:

(topic, partition, offset)
27. Offset is not "message ID"

This is a common beginner mistake.

Suppose:

Partition 0
Offset 0 → order A
Offset 1 → order B
Offset 2 → order C

Offset tells Kafka/consumer:

"Where am I in this partition?"

It isn't necessarily a business identifier.

Your business identifier could be:

orderId = 123

while Kafka offset could be:

offset = 84921
28. Where are consumer offsets stored?

Kafka can store committed consumer offsets in an internal Kafka topic:

__consumer_offsets

Example conceptually:

Consumer Group: payment-service

orders-P0 → offset 57
orders-P1 → offset 82

This allows the consumer to restart and continue from its committed position.

29. Consumer

Consumer = application that reads records from Kafka.

Example:

Kafka
  ↓
Payment Service

Payment service subscribes to:

orders

and consumes:

ORDER_PLACED
30. Consumer Group

This is another very important Kafka concept.

Suppose:

Topic: orders

P0
P1
P2

Consumer group:

payment-service

has:

Consumer A
Consumer B
Consumer C

Kafka can assign:

P0 → Consumer A
P1 → Consumer B
P2 → Consumer C

So the work is parallelized.

31. The golden rule of Consumer Groups

Within a consumer group, a partition is assigned to at most one consumer at a time.

Example:

Topic
 ├── P0
 ├── P1
 └── P2

Consumer Group
 ├── C1
 ├── C2
 └── C3

P0 → C1
P1 → C2
P2 → C3
32. Can multiple consumers read the same partition at the same time?

This is the question shown at the bottom of your screenshot.

Same consumer group?

Generally NO.

P0
 |
 +---- C1

C2 cannot simultaneously own P0
within the same group.
Different consumer groups?

YES.

This is one of Kafka's superpowers.

                 P0
                 |
        ┌────────┼─────────┐
        ↓        ↓         ↓
   Payment     Analytics   Audit
   Group       Group       Group

All three groups can independently read the same partition.

Example:

orders topic
     ↓
 ┌───┴───────────────┐
 ↓                   ↓
Payment Group     Analytics Group
 ↓                   ↓
process payment   calculate metrics

Each group maintains its own offsets.

33. Consumer Group vs Multiple Consumers

Suppose:

3 partitions
One consumer
C1 → P0
C1 → P1
C1 → P2
Three consumers
C1 → P0
C2 → P1
C3 → P2

More parallelism.

Six consumers
C1 → P0
C2 → P1
C3 → P2

C4 → idle
C5 → idle
C6 → idle

Because there are only 3 partitions.

Therefore:

Maximum active consumer parallelism within one consumer group is bounded by the number of partitions.

This is extremely important.

34. Consumer Group Example

Imagine an e-commerce system.

Topic:

orders

Consumer groups:

payment-service
inventory-service
notification-service
analytics-service

Each group independently consumes the same events.

                  orders
                    │
          ┌─────────┼─────────┐
          ↓         ↓         ↓
      Payment   Inventory  Notification
       Group      Group       Group
          │         │           │
          ↓         ↓           ↓
      Payments   Stock       Email/SMS

This is where Kafka differs strongly from a traditional task queue.

35. Kafka Consumer Pull Model

Kafka consumers generally pull records from Kafka.

Conceptually:

Consumer
   |
   | "Give me records"
   ↓
Kafka
   |
   ↓
Records

The consumer controls how quickly it processes data.

This is useful for backpressure.

36. Producer → Kafka → Consumer complete flow

Let's put everything together.

Imagine an e-commerce application.

User places an order:

User
 ↓
Order Service

Order Service creates:

{
  "event": "ORDER_PLACED",
  "orderId": 123,
  "amount": 999
}

Producer sends it to:

Topic: orders

Kafka:

orders
│
├── P0
├── P1
└── P2

Suppose:

hash(123) % 3 = 1

Therefore:

ORDER_PLACED
      ↓
Partition 1

Partition 1 leader:

Broker 2

So:

Producer
   ↓
Broker 2
   ↓
orders-P1

Followers replicate:

Broker 2 → Leader
Broker 1 → Follower
Broker 3 → Follower

Then consumers read it.

orders-P1
    │
    ├── Payment Group
    │       ↓
    │   Payment Service
    │
    ├── Inventory Group
    │       ↓
    │   Inventory Service
    │
    └── Analytics Group
            ↓
        Analytics Service

That's Kafka end-to-end.

37. acks — Producer Acknowledgement

One of the important producer configurations.

acks=0

Producer doesn't wait for acknowledgement.

Producer
   ↓
Kafka

"I sent it!"

Fast, but less reliable.

acks=1

Leader acknowledges after the leader has accepted/written the record.

Producer
   ↓
Leader
   ↓
ACK

Follower replication may still be ongoing.

acks=all

Producer waits for the leader to acknowledge after the record is replicated to the required ISR according to Kafka's replication/acknowledgement semantics.

Conceptually:

Producer
   ↓
Leader
   ↓
Followers
   ↓
ACK

This provides stronger durability, assuming appropriate replication configuration.

38. ISR — In-Sync Replicas

Suppose:

P0

Broker 1 → Leader
Broker 2 → Follower
Broker 3 → Follower

If all are caught up sufficiently:

ISR = [Broker 1, Broker 2, Broker 3]

ISR means:

Replicas that are considered sufficiently in sync with the leader according to Kafka's replication rules.

If Broker 3 falls behind badly:

Broker 1 → Leader
Broker 2 → In Sync
Broker 3 → Behind

ISR might become:

[Broker 1, Broker 2]
39. Why ISR matters

Suppose:

replication factor = 3

and:

ISR = 3 brokers

Then you have good redundancy.

If:

Broker 1 💥

Kafka can elect another eligible replica as leader.

Broker 2
   ↓
New Leader

This is how Kafka survives broker failures.

40. What happens when a broker dies?

Before:

P0

Broker 1 → Leader
Broker 2 → Follower
Broker 3 → Follower

Broker 1 crashes:

Broker 1 ❌

Kafka's controller manages partition leadership changes.

One eligible replica becomes leader:

Broker 2 → New Leader
Broker 3 → Follower

Producer gets updated metadata and starts sending to Broker 2.

41. How does Kafka know who is leader?

Kafka maintains cluster metadata.

It contains information such as:

Topics
Partitions
Partition leaders
Replica assignments
ISR

Modern Kafka uses KRaft for metadata management and consensus.

Older Kafka deployments used ZooKeeper.

Modern architecture
Kafka Cluster
      |
      ↓
KRaft controllers
      |
      ↓
Cluster metadata

So if you're learning modern Kafka:

Think KRaft, not ZooKeeper.

ZooKeeper is important historically, but new Kafka deployments generally use KRaft.

42. Kafka Controller

The controller has responsibilities around cluster management, including things such as:

Partition leadership
Replica state
Broker membership
Metadata management
Failover coordination

Think:

Controller = cluster management brain

Not every normal record passes through a controller.

That's an important distinction.

43. Event Retention

One of Kafka's biggest advantages is that records aren't necessarily deleted immediately after a consumer reads them.

Suppose:

ORDER_PLACED
ORDER_PAID
ORDER_SHIPPED

Kafka can retain them based on topic retention configuration.

For example:

Retention = 7 days

Then records can remain available for that period, subject to retention configuration and storage constraints.

44. Replay

Suppose Analytics Service had a bug.

It processed:

10 million events

incorrectly.

With a durable Kafka topic, you may be able to reset/reposition the consumer's offsets and process historical records again.

Kafka

M1
M2
M3
M4
M5
...

Consumer:

Read M1
Read M2
Read M3

Later:

Reset offset
      ↓
Read M1 again
Read M2 again
Read M3 again

This is called replay.

That's extremely useful in event-driven systems.

45. "Wrong data went into Kafka"

Your screenshot makes an important point:

Kafka is a log, not a traditional mutable database.

Suppose you accidentally publish:

{
  "orderId": 123,
  "status": "DELIVERED"
}

but the correct status was:

CANCELLED

You generally don't think:

UPDATE Kafka
SET status = CANCELLED

Instead, event-driven systems often publish another event:

{
  "orderId": 123,
  "status": "CANCELLED"
}

Now consumers process the history.

Conceptually:

ORDER_DELIVERED
       ↓
ORDER_CANCELLED

The current state can be derived by applying events in order.

This is related to event sourcing, although Kafka itself does not automatically make your entire system an event-sourced system.

46. Best place to validate data

Ideally:

Application
     ↓
Validation
     ↓
Schema/business validation
     ↓
Kafka

Instead of:

Application
     ↓
Kafka
     ↓
Consumer discovers garbage

Validation can include:

Required fields
Data types
Schema compatibility
Business rules
Valid enum values

For example:

{
  "orderId": 123,
  "status": "BANANA"
}

If valid statuses are:

PLACED
PAID
SHIPPED
CANCELLED

you should reject the bad event before it enters the main stream.

47. Schema Registry

Your screenshot mentions Schema Registry.

Suppose producers send:

{
  "orderId": 123,
  "amount": 999
}

and consumer expects:

{
  "orderId": 123,
  "amount": 999,
  "currency": "INR"
}

Schema management becomes important.

Schema Registry helps manage schemas and compatibility rules.

Common formats include:

Avro
Protobuf
JSON Schema

The key idea:

Producer and consumer need an agreed contract for the event structure.

48. Dead Letter Queue / Topic

Suppose a consumer receives:

{
  "orderId": 123,
  "amount": "HELLO"
}

Consumer tries:

parse amount as number

and fails.

You don't want the consumer to crash forever on the same poisonous record.

A common pattern is:

Main Topic
    ↓
Consumer
    ↓
Processing fails
    ↓
DLQ / Dead Letter Topic

DLQ might contain:

bad record
error reason
timestamp
original topic
partition
offset

Then later:

Inspect
Fix
Replay if appropriate
49. Important Kafka correction about DLQ

Kafka itself doesn't automatically create a DLQ for every failed consumer record.

Usually your application/framework implements the dead-letter behavior.

For example:

Kafka Consumer
      ↓
Processing
      ↓
Exception
      ↓
Application publishes record to
orders.DLT

So:

DLQ/DLT is a pattern, not a magical Kafka feature that automatically catches every error.

50. Complete Kafka Architecture

Here's the picture I want you to keep in your head:

                  ┌─────────────────┐
                  │   Producer      │
                  │  Order Service  │
                  └────────┬────────┘
                           │
                           │ ORDER_PLACED
                           ↓
                 ┌────────────────────┐
                 │   Kafka Cluster    │
                 │                    │
                 │  Broker 1          │
                 │  Broker 2          │
                 │  Broker 3          │
                 └─────────┬──────────┘
                           │
                      Topic: orders
                           │
             ┌─────────────┼─────────────┐
             ↓             ↓             ↓
            P0            P1             P2
             │             │              │
             └─────────────┼──────────────┘
                           │
          ┌────────────────┼─────────────────┐
          ↓                ↓                 ↓
    Payment Group    Inventory Group   Analytics Group
          ↓                ↓                 ↓
      Payment          Inventory          Analytics
       Service          Service            Service
51. One complete real-world example

Let's imagine Amazon-like e-commerce.

User buys a phone.

Step 1 — User places order
User
 ↓
Order Service

Order Service creates:

{
  "event": "ORDER_PLACED",
  "orderId": "ORD123",
  "productId": "IPHONE15",
  "amount": 69999
}
Step 2 — Producer sends event
Order Service
      ↓
Kafka Producer
      ↓
orders topic

Key:

ORD123
Step 3 — Kafka chooses partition

Suppose:

hash("ORD123") % 3 = 1

Then:

orders-P1
Step 4 — Find leader

Metadata says:

orders-P1

Leader → Broker 2
Follower → Broker 1
Follower → Broker 3

Producer sends directly to Broker 2.

Producer
   ↓
Broker 2
   ↓
orders-P1
Step 5 — Leader writes

Kafka appends:

Offset 5821

ORDER_PLACED
ORD123
Step 6 — Followers replicate
Broker 2
   ↓
Broker 1
Broker 3
Step 7 — Producer gets acknowledgement

If using:

acks=all

the producer gets acknowledgement after the relevant replication condition is satisfied.

52. Consumers now react
Payment Service
orders
 ↓
Payment Consumer
 ↓
Charge ₹69,999
Inventory Service
orders
 ↓
Inventory Consumer
 ↓
Reserve iPhone
Notification Service
orders
 ↓
Notification Consumer
 ↓
Send order confirmation
Analytics
orders
 ↓
Analytics Consumer
 ↓
Update dashboard

And all of these can independently maintain their own consumer-group offsets.

53. Why Kafka is powerful

Without Kafka:

Order Service
   ├──→ Payment
   ├──→ Inventory
   ├──→ Notification
   ├──→ Analytics
   └──→ Fraud

Order Service becomes tightly coupled to everyone.

With Kafka:

Order Service
      ↓
    Kafka
      ↓
 ┌────┼─────┬──────┬──────┐
 ↓    ↓     ↓      ↓      ↓
Pay  Inv  Notify  Fraud Analytics

Order Service only needs to know:

"Publish ORDER_PLACED."

It doesn't need to know who consumes it.

54. Scaling Kafka

Suppose:

orders topic

P0
P1
P2

and:

Consumer Group

C1
C2
C3

Kafka can parallelize:

P0 → C1
P1 → C2
P2 → C3

Now traffic increases.

You can increase partitions:

P0
P1
P2
P3
P4
P5

and scale consumers accordingly:

C1 → P0
C2 → P1
C3 → P2
C4 → P3
C5 → P4
C6 → P5

That's horizontal scalability.

55. One subtle but important point: partition count

Suppose you have:

6 partitions

You cannot get useful parallel consumption from 20 consumers in one consumer group.

At most 6 consumers can actively own partitions at a time.

6 partitions

C1 → P0
C2 → P1
C3 → P2
C4 → P3
C5 → P4
C6 → P5

C7 → idle
C8 → idle
...
C20 → idle

Therefore:

Partition count is an important scalability decision.

56. Kafka's mental model

If you remember only one picture, remember this:

                 KAFKA CLUSTER
                       │
                       ↓
                    TOPIC
                       │
              ┌────────┼────────┐
              ↓        ↓        ↓
             P0       P1       P2
              │        │        │
          ordered    ordered   ordered
             log       log       log
              │        │        │
           offsets   offsets   offsets
              │
              ↓
         Consumer Group
              │
        ┌─────┼─────┐
        ↓     ↓     ↓
       C1    C2    C3

And physically:

Partition 0
     ↓
Broker 1 = Leader
Broker 2 = Follower
Broker 3 = Follower
57. The entire Kafka vocabulary in one table
Concept	Simple meaning
Producer	Application that writes records
Consumer	Application that reads records
Topic	Logical stream/category of records
Partition	Ordered append-only log inside a topic
Broker	Kafka server
Cluster	Group of Kafka brokers
Offset	Position of a record within a partition
Consumer Group	Consumers working together
Leader	Replica responsible for partition's normal write/read coordination
Follower	Replica that follows the leader
Replication	Keeping copies of partition data
ISR	Replicas considered sufficiently in sync
Controller	Manages cluster metadata/state
KRaft	Kafka's modern metadata/consensus architecture
Retention	How long Kafka keeps records
Replay	Reading old records again
acks	Producer acknowledgement level
Schema Registry	Manages event schemas/contracts
DLQ/DLT	Destination for records that couldn't be processed
Rebalance	Reassignment of partitions among consumers in a group
58. The 10 things you absolutely must remember

If you're preparing for backend/system-design interviews, nail these first:

1.

Kafka is an event-streaming platform, not just a queue.

2.

Topic is logical; partitions are where records are stored.

3.

A partition is an ordered append-only log.

4.

Ordering is guaranteed within a partition, not across the entire topic.

5.

Offset identifies a record's position within a partition.

6.

A broker is a Kafka server.

7.

A Kafka cluster contains multiple brokers.

8.

A partition can have multiple replicas, with one leader and followers.

9.

Within one consumer group, a partition is assigned to at most one consumer at a time.

10.

Different consumer groups can independently consume the same Kafka records.

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
