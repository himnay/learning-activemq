# learning-activemq

Spring Boot + ActiveMQ Classic learning project — event-driven style. A REST API publishes a typed `OrderCreatedEvent` as JSON (Jackson message converter with type-id mappings, shared records in a common module); a standalone consumer subscribes with `@JmsListener`s. Around that one event the project demonstrates the classic messaging patterns: plain pub/sub, virtual-topic round-robin, request-reply, durable subscriptions, and redelivery with per-queue DLQs.

## Architecture

```mermaid
flowchart LR
    client[REST Client / Insomnia] -->|"POST /v1/events/orders/bulk"| pub[activemq-publisher :8080]
    client -->|"POST /v1/orders/quote"| pub
    pub -->|"order-created (seq 1..N)"| vt[(VirtualTopic.orders)]
    pub <-->|request-reply| qq[[orders.quote.queue]]
    vt -->|"plain + durable topic subscribers"| con[activemq-consumer :8081]
    vt -->|"workerA / workerB queues (round-robin)"| con
    qq <--> con
```

## Types 

![messaging-broker-types.png](images/messaging-broker-types.png)

## Table of Contents

**Part 1 — The Project**

1. 📦 [Modules](#modules)
2. ✉️ [Events & serialization](#events--serialization)
3. 🚀 [Quick start](#quick-start)
4. 🌐 [API](#api)
5. 🧠 [Topics vs Queues](#topics-vs-queues-in-activemq)
6. 🔁 [Pattern: round-robin over a virtual topic](#round-robin-demo-virtual-topic)
7. 🤝 [Pattern: request-reply](#request-reply-jmsreplyto--jmscorrelationid)
8. 📌 [Pattern: durable topic subscription](#durable-topic-subscription)
9. ☠️ [Pattern: redelivery + per-queue DLQ](#redelivery--per-queue-dlq)
10. 🖥️ [The broker: console & config](#activemq-broker)
11. ⚙️ [Configuration](#configuration)
12. 🤝 [Insomnia](#insomnia) · 📊 [Observability](#observability)

**Part 2 — [ActiveMQ Deep Dive](#activemq-deep-dive)** — 20 illustrated reference sections: broker internals, queues vs topics, prefetch, acks, DLQ, KahaDB, virtual topics, HA, Spring wiring, ActiveMQ vs Kafka vs RabbitMQ.

---

## Modules

| Module                | Port | What it does                                                                                    |
|-----------------------|------|-------------------------------------------------------------------------------------------------|
| `activemq-common`     | —    | Shared library: the event records + `JmsEventConverterConfig` (JSON converter)                  |
| `activemq-publisher`  | 8080 | REST API (`POST /v1/events/*`) → builds an event, publishes it to its topic, stamps `messageId` |
| `activemq-consumer`   | 8081 | `@JmsListener`s — topic listeners, virtual-topic workers, quote responder                       |

All modules inherit from the root POM (shared: `spring-boot-starter-activemq`, actuator, Lombok, test) which inherits from `super-pom` (Spring Boot parent, Java toolchain, BOM).

## Events & serialization

| Event                     | Destination                            | Fields                                                                          |
|---------------------------|----------------------------------------|---------------------------------------------------------------------------------|
| `OrderCreatedEvent`       | `VirtualTopic.orders`                  | orderId, product, quantity, amount, createdAt                                   |
| `OrderQuoteRequest/Reply` | `orders.quote.queue`                   | request-reply pair (see [pattern](#request-reply-jmsreplyto--jmscorrelationid)) |

Serialization: `JacksonJsonMessageConverter` writes JSON text messages and sets an `_event` type-id header (`order-created`, `order-quote-request`, `order-quote-reply`). The consumer maps that header back to its event class — both sides share the records via `activemq-common`, and the type-id (not the class name) travels on the wire.

Topics are pub/sub (`spring.jms.pub-sub-domain: true` on both sides): every live subscriber gets a copy, and listener concurrency stays at 1 — extra topic consumers would each receive a duplicate.

Every listener also logs the JMS destination it consumed from (`from=topic://VirtualTopic.orders`, `from=queue://Consumer.workerA.VirtualTopic.orders`) via the `jms_destination` header.

### Message properties — the JMS version of Kafka producer headers

`EventHeaderPostProcessor` (a `MessagePostProcessor`) runs after the converter builds the message and stamps metadata as **JMS properties**, keeping it out of the JSON body:

```java
jmsTemplate.convertAndSend(topic, event, new EventHeaderPostProcessor("order-created", messageId));
```

| Property             | Example              | Kafka analogue                  |
|----------------------|----------------------|---------------------------------|
| `messageId`          | uuid                 | header                          |
| `eventType`          | `order-created`      | header                          |
| `source`             | `activemq-publisher` | header                          |
| `publishedAtEpochMs` | `1784352579427`      | record timestamp                |
| `seq` (bulk only)    | `42`                 | header                          |
| `_event`             | set by converter     | — (type id for deserialization) |

Unlike Kafka headers, JMS properties are broker-visible: usable in consumer selectors (`selector = "eventType = 'order-created'"`) and shown when browsing queues in the web console. Consumers read them with `@Header("seq")` etc.

## Quick start

Prerequisites: Java 25, Maven, Docker (for the ActiveMQ broker).

```bash
# 1. Start ActiveMQ
docker compose up -d

# 2. Build everything
mvn clean install

# 3. Start consumer (terminal 1)
mvn -pl activemq-consumer spring-boot:run

# 4. Start publisher (terminal 2)
mvn -pl activemq-publisher spring-boot:run

# 5. Publish events (count=1 for a single event)
curl -X POST 'http://localhost:8080/v1/events/orders/bulk?count=1' \
  -H "Content-Type: application/json" \
  -d '{"product": "Laptop", "quantity": 2, "amount": 2499.98}'
```

One published event fans out to every consumption pattern — consumer log:

```
Consumed order-created from=topic://VirtualTopic.orders id=<uuid> orderId=<uuid> product=Laptop qty=2 amount=2499.98
DURABLE consumed order-created from=topic://VirtualTopic.orders id=<uuid> orderId=<uuid> product=Laptop qty=2
workerA consumed from=queue://Consumer.workerA.VirtualTopic.orders seq=1 orderId=<uuid> thread=...
workerB consumed from=queue://Consumer.workerB.VirtualTopic.orders seq=1 orderId=<uuid> thread=...
```

## API

The publish endpoint returns `202 Accepted`:

```json
{
  "count": 100,
  "topic": "VirtualTopic.orders",
  "publishedAt": "2026-07-17T19:00:00Z"
}
```

| Endpoint                              | Request body                                | Notes                                                        |
|---------------------------------------|---------------------------------------------|--------------------------------------------------------------|
| `POST /v1/events/orders/bulk?count=N` | `{"product", "quantity", "amount"}`         | N seq-numbered events to the virtual topic (1–1000, default 100); product not blank, numbers positive |
| `POST /v1/orders/quote`               | same body                                   | request-reply — returns `200 {approved, totalPrice, note}`   |

Invalid payloads get `400`. The server generates `orderId` and the timestamp.

## Topics vs Queues in ActiveMQ

The mental model behind every pattern below:

| Aspect                 | Queue (point-to-point)                           | Topic (pub/sub)                                      |
|------------------------|--------------------------------------------------|------------------------------------------------------|
| Delivery               | Each message goes to exactly **one** consumer    | Each message goes to **every** active subscriber     |
| Multiple consumers     | Compete — broker round-robins between them       | Duplicate — each one gets its own copy               |
| Offline consumer       | Messages wait in the queue (retained)            | Message lost unless subscriber is durable            |
| Browsing (console/GUI) | Yes — pending messages + bodies visible          | No — only enqueue/dequeue counters                   |
| Typical use            | Work distribution, task processing               | Event broadcast, notifications                       |
| Spring switch          | `spring.jms.pub-sub-domain: false` (default)     | `spring.jms.pub-sub-domain: true`                    |

**Virtual topics** combine both: publish once to a topic, consume from queues. The broker watches the naming convention — any queue named `Consumer.<group>.VirtualTopic.<name>` automatically receives a **copy** of every message published to topic `VirtualTopic.<name>`. No broker config needed, just the names.

- Each *group queue* gets all messages (topic-style fan-out across groups).
- Within one group queue, competing consumers split them round-robin (queue-style work sharing).
- Same mental model as Kafka consumer groups: group = "gets all the data", consumers in the group = "share the work".

What this project runs:

| Kind           | Destination                                             | Consumers                                       |
|----------------|---------------------------------------------------------|-------------------------------------------------|
| Virtual topic  | `VirtualTopic.orders` (publish side only)               | — (broker copies into queues below)             |
| Queue          | `Consumer.workerA.VirtualTopic.orders`                  | 3 competing consumers (round-robin)             |
| Queue          | `Consumer.workerB.VirtualTopic.orders`                  | 3 competing consumers (round-robin)             |
| Queue          | `orders.quote.queue`                                    | 1 quote responder (request-reply)               |

So one bulk message is copied **twice** (once per worker queue), and inside each queue exactly one of the 3 consumers receives it. 100 published → 100 in workerA + 100 in workerB → ~33/33/34 per consumer thread.

## Round-robin demo (virtual topic)

`POST /v1/events/orders/bulk?count=100` publishes a numbered burst of `OrderCreatedEvent`s to the **virtual topic** `VirtualTopic.orders`. The broker mirrors every message into each `Consumer.*.VirtualTopic.orders` queue, and inside a queue the competing consumers split the work round-robin:

```
publisher ──▶ VirtualTopic.orders ──▶ Consumer.workerA.VirtualTopic.orders ──▶ 3 competing consumers (~33/33/34)
                                  └─▶ Consumer.workerB.VirtualTopic.orders ──▶ 3 competing consumers (~33/33/34)
```

- **Fan-out across queues**: workerA and workerB each receive all 100 (copies).
- **Round-robin within a queue**: the 3 consumers (`queueListenerFactory`; concurrency from `app.listener.worker-concurrency`) alternate — watch `seq=1/2/3/4…` land on threads `#-1/#-2/#-3/#-1…` in the consumer log.
- Virtual-topic queues are real queues — browsable in the web console under **Queues**, unlike plain topics.

```bash
curl -X POST 'http://localhost:8080/v1/events/orders/bulk?count=100' \
  -H "Content-Type: application/json" \
  -d '{"product": "Widget", "quantity": 1, "amount": 9.99}'
```

## Request-reply (JMSReplyTo + JMSCorrelationID)

Synchronous question over asynchronous messaging: `POST /v1/orders/quote` asks the consumer to price an order and waits for the answer.

```mermaid
sequenceDiagram
    participant C as REST client
    participant P as Publisher (QuoteService)
    participant Q as orders.quote.queue
    participant R as Consumer (QuoteRequestListener)
    participant T as temp reply queue
    C->>P: POST /v1/orders/quote
    P->>Q: OrderQuoteRequest (JMSReplyTo=temp, JMSCorrelationID=uuid)
    Q->>R: dispatch
    R->>T: OrderQuoteReply (JMSCorrelationID copied)
    T->>P: reply received
    P->>C: 200 {approved, totalPrice, note}
```

How the pieces work:

- **Publisher** — `queueJmsTemplate.sendAndReceive(...)` creates a **temporary reply queue**, stamps it as `JMSReplyTo` plus a `JMSCorrelationID`, sends, and blocks (5s timeout → `504`). Queue semantics required, so a second, non-primary `JmsTemplate` bean exists just for this (`QueueJmsConfig`).
- **Consumer** — `QuoteRequestListener` simply **returns** `OrderQuoteReply` from the `@JmsListener` method; Spring sends it to the request's `JMSReplyTo` and copies the correlation ID. No manual reply plumbing.
- Business rule for the demo: `total = unitPrice × quantity`, approved while ≤ 5000.

```bash
curl -X POST http://localhost:8080/v1/orders/quote \
  -H "Content-Type: application/json" \
  -d '{"product": "Laptop", "quantity": 2, "amount": 1200.00}'
# → {"approved":true,"totalPrice":2400.00,"note":"within approval limit"}
```

## Durable topic subscription

Plain topic subscribers miss whatever is published while they're offline. `DurableOrderListener` registers a **durable subscription** on the `VirtualTopic.orders` topic (identity = `clientId` `activemq-consumer` + subscription `orders-durable-sub`) — the broker stores missed events and replays them on reconnect.

```mermaid
sequenceDiagram
    participant P as Publisher
    participant B as Broker
    participant D as Durable subscriber
    D->>B: subscribe (clientId + subscription name)
    D--xB: consumer stopped
    P->>B: orders published while offline (x3)
    Note over B: stored for the durable subscription
    D->>B: consumer restarts
    B-->>D: replays all 3 (plain subscriber gets nothing)
```

Implementation note: a `clientId` must be set before the connection starts, which Boot's shared `CachingConnectionFactory` forbids — so `durableTopicListenerFactory` builds its own private connection factory (deliberately not a Spring bean; a second `ConnectionFactory` bean would switch off Boot's auto-configuration).

Try it: stop the consumer → `POST /v1/events/orders` a few times → start the consumer → watch `DURABLE consumed ...` catch-up lines; the plain listener stays silent.

## Redelivery + per-queue DLQ

What happens when a listener throws — demonstrated end-to-end:

```mermaid
flowchart LR
    q[["Consumer.workerA.<br/>VirtualTopic.orders"]] --> l[workerA listener]
    l -->|"seq % fail-seq-multiple == 0<br/>throws"| rb["rollback (transacted session)"]
    rb -->|"redeliver after 0.5s, 1s, 2s"| q
    rb -->|"4th failure"| dlq[["DLQ.Consumer.workerA.<br/>VirtualTopic.orders"]]
```

- **Client-side redelivery policy** (`RedeliveryConfig`): 3 redeliveries, initial delay 500ms, exponential backoff ×2 → attempts at ~0s, 0.5s, 1s, 2s. Boot's listener sessions are transacted, so an exception rolls the message back.
- **Broker-side per-queue DLQ** (`broker/activemq.xml`, mounted by docker-compose): `individualDeadLetterStrategy` with prefix `DLQ.` — poison messages land in `DLQ.<original-queue>` instead of the shared `ActiveMQ.DLQ`, so failures stay attributable. Browsable in the console under Queues.
- **Failing-listener demo**: set `app.listener.fail-seq-multiple` (default 0 = off) and workerA throws for every divisible `seq`:

```bash
# run consumer with the poison switch on
mvn -pl activemq-consumer spring-boot:run -Dspring-boot.run.arguments=--app.listener.fail-seq-multiple=10

# publish 10 — seq 10 fails 4 times (0.5s/1s/2s backoff), then dead-letters
curl -X POST 'http://localhost:8080/v1/events/orders/bulk?count=10' \
  -H "Content-Type: application/json" -d '{"product": "Widget", "quantity": 1, "amount": 9.99}'
```

Verified run: 4 `SIMULATED FAILURE seq=10` warnings with exact 0.5s/1s/2s gaps, then `DLQ.Consumer.workerA.VirtualTopic.orders` size 1; workerB's copy processed normally.

## ActiveMQ broker

| Thing                 | Value                                               |
|-----------------------|-----------------------------------------------------|
| Broker (OpenWire/JMS) | `tcp://localhost:61616`                             |
| Web console           | <http://localhost:8161> — `admin` / `admin`         |
| Topic                 | `VirtualTopic.orders`                               |
| Image                 | `apache/activemq-classic:6.1.7`                     |
| Broker config         | `broker/activemq.xml` (per-queue DLQ strategy)      |

![messaging-broker-gui.png](images/messaging-broker-gui.png)

Watch the topics in the console under **Topics** — enqueued/dequeued counters move as you publish. Start the consumer first: topic messages are not retained for subscribers that aren't connected. Queues (worker, quote, DLQ) are browsable under **Queues**, message bodies included.

## Configuration

Overridable via env vars (12-factor style):

| Env var                               | Default                 | Used by |
|---------------------------------------|-------------------------|---------|
| `ACTIVEMQ_BROKER_URL`                 | `tcp://localhost:61616` | both    |
| `ACTIVEMQ_USER` / `ACTIVEMQ_PASSWORD` | `admin` / `admin`       | both    |

Destination names live under `app.topics.*` / `app.queues.*`, worker concurrency under `app.listener.worker-concurrency`, and the DLQ demo switch under `app.listener.fail-seq-multiple` — all in each module's `application.yml`.

## Insomnia

Import `insomnia-collection.json` — one request per event type, the bulk burst, the quote request-reply, plus health checks for both modules.

## Observability

Actuator on both modules: `/actuator/health`, `/actuator/metrics`, `/actuator/prometheus`.

---

# ActiveMQ Deep Dive

Reference half of this README — everything you need to reason about ActiveMQ Classic, with diagrams. Each section stands alone; skim the diagrams first, read the prose when needed.

---

## Table of Contents

1. 💡 [What is ActiveMQ](#1-what-is-activemq)
2. 🏗️ [Broker architecture](#2-broker-architecture)
3. 📦 [Destinations: queues](#3-destinations-queues)
4. 📣 [Destinations: topics](#4-destinations-topics)
5. ✉️ [Anatomy of a message](#5-anatomy-of-a-message)
6. ⚡ [Message consumption: prefetch and dispatch](#6-message-consumption-prefetch-and-dispatch)
7. ✅ [Acknowledgement modes and transactions](#7-acknowledgement-modes-and-transactions)
8. 🔁 [Competing consumers and round-robin](#8-competing-consumers-and-round-robin)
9. 📌 [Durable subscriptions](#9-durable-subscriptions)
10. 🪄 [Virtual topics](#10-virtual-topics)
11. 💾 [Persistence: KahaDB](#11-persistence-kahadb)
12. ☠️ [Redelivery and dead-letter queues](#12-redelivery-and-dead-letter-queues)
13. 🎯 [Selectors, exclusive consumers, message groups](#13-selectors-exclusive-consumers-message-groups)
14. ⏰ [Scheduled and delayed delivery](#14-scheduled-and-delayed-delivery)
15. 🚦 [Memory limits and flow control](#15-memory-limits-and-flow-control)
16. 🛡️ [High availability and networks of brokers](#16-high-availability-and-networks-of-brokers)
17. 🌱 [Spring Boot integration model](#17-spring-boot-integration-model)
18. 📊 [Monitoring: console, JMX, advisory topics](#18-monitoring-console-jmx-advisory-topics)
19. ⚖️ [ActiveMQ vs Kafka vs RabbitMQ](#19-activemq-vs-kafka-vs-rabbitmq)
20. 🗺️ [How this project maps to all of the above](#20-how-this-project-maps-to-all-of-the-above)

---

## 1. What is ActiveMQ

ActiveMQ is a **message broker**: a server that sits between applications and moves messages from producers to consumers so the two sides never talk directly. Decoupling in three dimensions:

- **Space** — producer doesn't know who consumes, or how many.
- **Time** — consumer may be down; queue holds the message until it returns.
- **Load** — burst of 10k messages doesn't crash a consumer that processes 100/s; the queue absorbs the spike.

```mermaid
flowchart LR
    p1[Producer A] --> b((ActiveMQ<br/>Broker))
    p2[Producer B] --> b
    b --> c1[Consumer X]
    b --> c2[Consumer Y]
    b --> c3[Consumer Z]
```

Two products share the name:

|             | ActiveMQ **Classic** (this project)    | ActiveMQ **Artemis**       |
|-------------|----------------------------------------|----------------------------|
| Lineage     | Original codebase (2004+)              | HornetQ donation, next-gen |
| Threading   | Thread-per-connection oriented         | Reactor / async core       |
| Persistence | KahaDB journal                         | Own append-only journal    |
| Protocols   | OpenWire native; STOMP, AMQP, MQTT, WS | Same set, AMQP first-class |
| Future      | Maintenance + steady releases          | Where new features land    |

ActiveMQ implements **JMS** (Jakarta Messaging) — the Java standard API for messaging — so application code depends on `jakarta.jms.*` interfaces, not on ActiveMQ classes. Swap the broker, keep the code.

---
## 2. Broker architecture

```mermaid
flowchart TB
    subgraph clients["Clients"]
        prod[Producers]
        cons[Consumers]
    end
    subgraph broker["ActiveMQ Classic broker (JVM)"]
        subgraph transports["Transport connectors"]
            ow["OpenWire :61616"]
            amqp["AMQP :5672"]
            stomp["STOMP :61613"]
            mqtt["MQTT :1883"]
            ws["WebSocket :61614"]
        end
        core["Broker core<br/>(dispatch, security, policies)"]
        subgraph dests["Destinations"]
            q1[["queue: orders"]]
            t1(("topic: events"))
        end
        store[("KahaDB<br/>persistence store")]
        jmx["JMX MBeans"]
        web["Web console :8161"]
    end
    prod --> ow --> core
    cons --> ow
    core --> q1
    core --> t1
    q1 <--> store
    t1 <--> store
    core --- jmx
    jmx --- web
```

Key pieces:

- **Transport connectors** — one broker, many wire protocols. JVM clients use **OpenWire** (fastest, full feature set). Cross-language clients use AMQP/STOMP/MQTT. The URL `tcp://localhost:61616` in `application.yml` is the OpenWire connector.
- **Broker core** — routing, security (authentication/authorization plugins), per-destination policies (memory limits, DLQ strategy, prefetch defaults).
- **Destinations** — named queues and topics, created on demand by default (first producer/consumer creates them — that's why the project needs zero broker config).
- **Persistence store** — messages marked persistent survive broker restart (see §11).
- **JMX + web console** — every queue/topic/connection is an MBean; the console at :8161 is a UI over them.

---

## 3. Destinations: queues

**Point-to-point.** Each message is delivered to **exactly one** consumer, no matter how many are attached. Undelivered messages wait — minutes or days — until someone consumes or they expire.

```mermaid
flowchart LR
    p[Producer] -->|"m1, m2, m3, m4"| q[["queue: work"]]
    q -->|"m1, m4"| c1[Consumer 1]
    q -->|m2| c2[Consumer 2]
    q -->|m3| c3[Consumer 3]
```

Properties:

- **Retention** — messages stay until consumed, expired (TTL), or purged. A queue is a buffer.
- **Competing consumers** — multiple consumers on one queue split the messages (see §8). This is *the* horizontal-scaling primitive for work processing.
- **Browsable** — you can look at pending messages (web console → Queues → click message; or JMS `QueueBrowser`) without consuming them.
- **Ordering** — FIFO as enqueued, but competing consumers can complete out of order. Strict ordering needs a single consumer or message groups (§13).

Use for: task distribution, order processing, anything where each unit of work must be handled once.

---

## 4. Destinations: topics

**Publish/subscribe.** Each message is delivered to **every** subscriber that is connected at publish time. Subscribers do not compete — they each get a full copy.

```mermaid
flowchart LR
    p[Publisher] -->|m1| t(("topic: price.updates"))
    t -->|"copy of m1"| s1[Subscriber A]
    t -->|"copy of m1"| s2[Subscriber B]
    t -->|"copy of m1"| s3[Subscriber C]
    s4["Subscriber D<br/>offline"]
    t -.->|"nothing — was offline"| s4
```

Properties:

- **No retention** — a topic is a broadcast, not a buffer. Offline at publish time → message missed forever (unless the subscription is *durable*, §9).
- **No browsing** — nothing is stored, so the console shows only counters.
- **Adding consumers duplicates work** — two subscribers running the same code both process every message. That's why this project keeps topic listener concurrency at 1.

Use for: event notification, cache invalidation, live feeds — anything where "whoever cares right now should hear this".

**The one-slide summary:**

```mermaid
flowchart TB
    subgraph queue["QUEUE — divide the work"]
        qm["message"] --> qc1[consumer]
        qm -.->|"only ONE gets it"| qc2[consumer]
    end
    subgraph topic["TOPIC — broadcast the news"]
        tm["message"] --> tc1[subscriber]
        tm -->|"EVERYONE gets it"| tc2[subscriber]
    end
```

---

## 5. Anatomy of a message

```mermaid
flowchart TB
    subgraph msg["JMS Message"]
        subgraph headers["Headers (fixed, set by provider/producer)"]
            h1["JMSMessageID — unique id"]
            h2["JMSDestination — queue/topic"]
            h3["JMSDeliveryMode — PERSISTENT or NON_PERSISTENT"]
            h4["JMSTimestamp, JMSExpiration, JMSPriority 0-9"]
            h5["JMSCorrelationID, JMSReplyTo — request/reply"]
        end
        subgraph props["Properties (free-form key-value)"]
            p1["messageId = uuid (ours)"]
            p2["_event = order-created (converter type id)"]
            p3["seq = 42 (ours, bulk demo)"]
        end
        subgraph body["Body (one of five types)"]
            b1["TextMessage — String (JSON here)"]
            b2["BytesMessage / MapMessage / ObjectMessage / StreamMessage"]
        end
    end
```

Details worth knowing:

- **Properties are for routing and filtering** — selectors (§13) can only see headers + properties, never the body. That's why the type id (`_event`) travels as a property, not inside the JSON.
- **`JMSDeliveryMode`** — `PERSISTENT` (default): broker writes to KahaDB before acking the producer; survives restart. `NON_PERSISTENT`: memory only, faster, lost on broker crash.
- **`JMSExpiration`** — set via producer TTL. Expired messages are dropped (or sent to DLQ depending on policy).
- **`JMSPriority`** — 0–9; broker makes a best effort to deliver higher priority first.
- **`JMSCorrelationID` + `JMSReplyTo`** — the request/reply pattern over messaging: send with `JMSReplyTo=tempQueue`, responder copies your `JMSMessageID` into `JMSCorrelationID` of the answer.

---

## 6. Message consumption: prefetch and dispatch

ActiveMQ **pushes** messages to consumers (unlike Kafka's pull). To keep pipelines full, it pushes ahead of consumption — the **prefetch buffer**.

```mermaid
sequenceDiagram
    participant B as Broker
    participant C as Consumer (prefetch=1000)
    B->>C: dispatch m1..m1000 (fills prefetch window)
    C->>C: process m1
    C->>B: ack m1
    B->>C: dispatch m1001 (window slides)
    C->>C: process m2
    C->>B: ack m2
    B->>C: dispatch m1002
```

- Default prefetch: **1000** for queues, very large for topics.
- **Trade-off**: big prefetch = throughput (no round-trip per message); small prefetch = fairness. With prefetch 1000 and two consumers, the first may grab 1000 while the second sits idle — for slow tasks set prefetch to 1 so each consumer takes one at a time.
- Configure per connection URL (`tcp://host:61616?jms.prefetchPolicy.queuePrefetch=1`) or per destination.
- Our 34/33/33 round-robin split works because messages arrived faster than processing and the broker dealt them across the three *sessions* evenly.

---

## 7. Acknowledgement modes and transactions

A message is only *gone* from the broker when acknowledged. Who acks, and when, defines your delivery guarantee:

| Mode                  | Who acks                            | Guarantee                  | Notes                                              |
|-----------------------|-------------------------------------|----------------------------|----------------------------------------------------|
| `AUTO_ACKNOWLEDGE`    | Provider, after listener returns    | At-least-once              | Spring's default; redelivered if listener throws   |
| `CLIENT_ACKNOWLEDGE`  | Your code (`message.acknowledge()`) | At-least-once              | Acks *all* messages consumed so far in the session |
| `DUPS_OK_ACKNOWLEDGE` | Provider, lazily in batches         | At-least-once, dups likely | Fastest, use when idempotent                       |
| Transacted session    | `session.commit()`                  | All-or-nothing batch       | Rollback ⇒ everything redelivered                  |

```mermaid
flowchart TB
    d["broker dispatches message"] --> l["listener runs"]
    l -->|"returns normally"| a["ACK — broker deletes message"]
    l -->|throws| r["NO ACK — redelivery counter++"]
    r --> rd{"redeliveries <= max?"}
    rd -->|yes| d
    rd -->|no| dlq[["ActiveMQ.DLQ"]]
```

**Exactly-once does not exist** over the wire — you get at-least-once plus an idempotent consumer (dedupe on `JMSMessageID`/business key), or transactions within one broker. Design consumers to tolerate duplicates.

---

## 8. Competing consumers and round-robin

The scaling pattern this project demos with the bulk endpoint. N consumers on one queue; the broker distributes messages among them — with equal prefetch and processing speed, effectively **round-robin**.

```mermaid
flowchart LR
    p["Producer<br/>100 msgs"] --> q[["queue"]]
    q -->|"m1, m4, m7 … (~34)"| c1["consumer thread 1"]
    q -->|"m2, m5, m8 … (~33)"| c2["consumer thread 2"]
    q -->|"m3, m6, m9 … (~33)"| c3["consumer thread 3"]
```

- Scale out = add consumers; scale in = remove them. Broker rebalances automatically, nothing to configure.
- A slow consumer naturally receives less (its prefetch window stays full) — poor man's load-aware routing.
- Ordering across consumers is **lost** — see message groups (§13) when a sub-stream must stay ordered.
- In Spring this is one line: listener container `concurrency = "3-3"` (three sessions competing on the same queue).

Measured in this project (100-message burst): threads split 34/33/33 with `seq` alternating thread-1, thread-2, thread-3, thread-1…

---

## 9. Durable subscriptions

Plain topic subscribers miss messages published while they're offline. A **durable subscription** makes the broker remember the subscriber and buffer what it missed.

```mermaid
sequenceDiagram
    participant P as Publisher
    participant B as Broker
    participant S as Durable subscriber (clientId=app1, name=sub1)
    S->>B: subscribe (durable)
    B-->>S: m1
    S--xB: disconnects
    P->>B: publish m2, m3
    Note over B: m2, m3 stored for app1/sub1
    S->>B: reconnects with same clientId+name
    B-->>S: m2, m3 (catch-up)
```

- Identity = `clientId` + subscription name. Reconnect with the same pair → resume; different pair → new subscription, history lost.
- One active consumer per durable subscription (classic limitation) — you can't compete on one. That's exactly the problem **virtual topics** solve (§10): queue semantics per subscriber group, so groups can both catch up *and* scale out.
- JMS 2.0 "shared durable subscriptions" allow competing, but virtual topics remain the idiomatic ActiveMQ Classic answer.

---

## 10. Virtual topics

The best of both worlds and the core trick of this project's round-robin demo. Publish **once** to a topic; consume from **queues**.

```mermaid
flowchart LR
    p[Publisher] -->|"1 message"| vt(("topic:<br/>VirtualTopic.orders"))
    vt -->|copy| qa[["queue:<br/>Consumer.workerA.VirtualTopic.orders"]]
    vt -->|copy| qb[["queue:<br/>Consumer.workerB.VirtualTopic.orders"]]
    qa --> a1[consumer]
    qa --> a2[consumer]
    qa --> a3[consumer]
    qb --> b1[consumer]
    qb --> b2[consumer]
    qb --> b3[consumer]
```

How it works — **pure naming convention, zero broker config**:

1. Publisher sends to a topic named `VirtualTopic.<name>`.
2. Broker intercepts and copies each message into every queue matching `Consumer.<group>.VirtualTopic.<name>` that exists (a queue exists as soon as one consumer subscribed to it).
3. Each group queue behaves like a normal queue: retention, browsing, DLQ, competing consumers.

What each layer gives you:

| Layer                      | Semantics                | Kafka analogy                    |
|----------------------------|--------------------------|----------------------------------|
| `VirtualTopic.orders`      | fan-out to all groups    | the topic                        |
| `Consumer.workerA.*` queue | group gets every message | consumer group A                 |
| 3 consumers on that queue  | round-robin work sharing | partitions consumed within group |

Gotchas:

- Names are magic: default prefixes `VirtualTopic.` and `Consumer.` are configurable broker-side but the convention is the API.
- A group that has *never* connected gets nothing retroactively — the queue must exist (first consumer creates it) before messages are copied in.
- Copies are independent: workerA nacking a message into DLQ doesn't affect workerB's copy.

---

## 11. Persistence: KahaDB

Where persistent messages live between arrival and ack. KahaDB is a **write-ahead journal + index**, purpose-built for messaging (append-heavy, delete-on-ack).

```mermaid
flowchart LR
    subgraph broker["Broker"]
        cache["memory cache<br/>(hot messages)"]
    end
    subgraph kahadb["KahaDB directory"]
        j1["db-1.log (32MB journal)"]
        j2["db-2.log"]
        j3["db-N.log — append only"]
        idx["db.data — B-tree index<br/>(msgId to journal location)"]
    end
    producer -->|"persistent send"| broker
    broker -->|"append (sequential IO)"| j3
    broker --> cache
    cache -->|dispatch| consumer
    consumer -->|ack| broker
    broker -->|"mark deleted in index"| idx
```

- **Send path**: persistent message → appended to current journal file → fsync (configurable) → producer gets ack. Sequential writes = fast even on spinning disks.
- **Ack path**: message marked consumed in the index; journal files whose messages are *all* consumed are garbage-collected whole. One stuck message in a 32MB file keeps the entire file alive — a classic "why is my disk full" incident.
- Non-persistent messages skip the journal (memory, spill to temp store under pressure).
- The docker volume `activemq-data` in this project's compose file is exactly this directory — broker restarts keep queue contents.

---

## 12. Redelivery and dead-letter queues

When a listener throws, the message is redelivered; when it keeps failing, it's parked in a **dead-letter queue** instead of poisoning the consumer forever.

```mermaid
flowchart LR
    q[["queue: work"]] --> c[Consumer]
    c -->|"exception #1..6"| q
    c -->|"exception #7 — maxRedeliveries exceeded"| dlq[["ActiveMQ.DLQ"]]
    dlq --> ops["operator: inspect in console,<br/>fix cause, move back or discard"]
```

- Default policy: 6 redeliveries, then to the shared `ActiveMQ.DLQ`. Configure per destination: individual DLQs (`queue.work.DLQ`), backoff/exponential delay between attempts, whether non-persistent messages also DLQ.
- Redelivered messages carry `JMSRedelivered=true` and `JMSXDeliveryCount` — consumers can branch on it ("third attempt → log loudly").
- DLQ is a normal queue: browsable, consumable — build a small "retry" tool or use the console's Move operation.
- **Poison message** = message that always fails (bad JSON, impossible state). Without DLQ it would loop forever, blocking everything behind it at prefetch 1.

---

## 13. Selectors, exclusive consumers, message groups

Three routing refinements, all consumer-side:

**Selectors** — SQL92-ish filter over headers/properties. Broker delivers only matching messages.

```java
@JmsListener(destination = "orders", selector = "region = 'EU' AND amount > 100")
```

```mermaid
flowchart LR
    q[["queue: orders"]] -->|"region='EU'"| eu[EU consumer]
    q -->|"region='US'"| us[US consumer]
```

Caution: a queue with selective consumers can strand messages nobody selects.

**Exclusive consumer** — many connected, broker picks one to receive *everything*; on its death the next takes over. Cheap active/passive failover with strict ordering.

```java
// destination: work?consumer.exclusive=true
```

**Message groups** — ordered sub-streams inside a shared queue. Set `JMSXGroupID`; all messages of one group go to one consumer (sticky), different groups spread across consumers.

```mermaid
flowchart LR
    p[Producer] --> q[["queue"]]
    q -->|"JMSXGroupID=order-1 (all)"| c1[consumer 1]
    q -->|"JMSXGroupID=order-2 (all)"| c2[consumer 2]
    q -->|"JMSXGroupID=order-3 (all)"| c1
```

Per-order strict ordering **and** horizontal scaling — same idea as Kafka partition keys.

---

## 14. Scheduled and delayed delivery

Broker-side timer (enable with `schedulerSupport="true"`). Producer stamps delay properties; broker holds the message and enqueues at the right moment.

| Property                                        | Meaning                         |
|-------------------------------------------------|---------------------------------|
| `AMQ_SCHEDULED_DELAY`                           | deliver after N ms              |
| `AMQ_SCHEDULED_PERIOD` + `AMQ_SCHEDULED_REPEAT` | redeliver every period, N times |
| `AMQ_SCHEDULED_CRON`                            | cron expression                 |

```mermaid
sequenceDiagram
    participant P as Producer
    participant S as Broker scheduler store
    participant Q as Queue
    participant C as Consumer
    P->>S: send (AMQ_SCHEDULED_DELAY=60000)
    Note over S: held 60s
    S->>Q: enqueue at T+60s
    Q->>C: dispatch
```

Use for: retry-with-backoff queues, reminder events, SLA timers. (The console's **Scheduled** tab lists held messages.)

---

## 15. Memory limits and flow control

The broker protects itself from fast producers + slow consumers with a hierarchy of limits:

```mermaid
flowchart TB
    su["systemUsage"]
    su --> mu["memoryUsage — heap for messages (e.g. 70%)"]
    su --> st["storeUsage — max KahaDB disk (e.g. 100GB)"]
    su --> tu["tempUsage — spill space for non-persistent"]
    mu --> pd["per-destination memoryLimit (e.g. 10MB per queue)"]
    pd -->|"limit hit"| pfc{"producer flow control"}
    pfc -->|"on (default)"| block["producer send() BLOCKS until space"]
    pfc -->|off| spool["messages spool to disk, producers keep going"]
```

- **Producer flow control on** (default): a full queue back-pressures producers — `send()` blocks. Great for keeping the system honest, surprising when your REST thread hangs because a consumer died.
- **Flow control off**: broker pages messages to disk; producers stay fast until *disk* limits hit.
- Symptoms to recognize: producers mysteriously freezing → check destination memory limit and consumer health first.

---

## 16. High availability and networks of brokers

**Master/slave (shared store)** — the classic HA pair. Both brokers point at the same KahaDB; whoever holds the file lock is master, the other waits. Clients use a failover URL and reconnect automatically.

```mermaid
flowchart LR
    client["client<br/>failover:(tcp://b1:61616,tcp://b2:61616)"]
    m["broker 1 — MASTER<br/>(holds KahaDB lock)"]
    s["broker 2 — SLAVE<br/>(blocked on lock)"]
    disk[("shared KahaDB<br/>(NFS/SAN/JDBC)")]
    client --> m
    m --> disk
    s -.->|"waiting for lock"| disk
    m -. "crash: lock released,<br/>slave becomes master" .-> s
```

**Network of brokers** — scale *out*: brokers interconnect and forward messages to where the consumers are (store-and-forward). Used for geographic distribution or sheer connection count.

```mermaid
flowchart LR
    subgraph dc1["DC 1"]
        p[producers] --> b1((broker 1))
    end
    subgraph dc2["DC 2"]
        b2((broker 2)) --> c[consumers]
    end
    b1 ==>|"network connector<br/>(demand forwarding)"| b2
```

Rule of thumb: messages flow **toward demand** — a broker only forwards a queue's messages when the remote broker has consumers for it.

---

## 17. Spring Boot integration model

What `spring-boot-starter-activemq` wires for you, and how the pieces in this repo connect:

```mermaid
flowchart TB
    subgraph publisher["Publisher app"]
        ctrl[EventController] --> svc[EventPublisherService]
        svc --> jt[JmsTemplate]
        jt --> conv1["JacksonJsonMessageConverter<br/>(from activemq-common)"]
        jt --> cf1[ConnectionFactory]
    end
    subgraph consumer["Consumer app"]
        ann["@JmsListener methods"] --> lc["Listener containers<br/>(topic factory / queueListenerFactory)"]
        lc --> conv2["JacksonJsonMessageConverter"]
        lc --> cf2[ConnectionFactory]
    end
    cf1 --> broker((ActiveMQ<br/>:61616))
    broker --> cf2
```

- **`ConnectionFactory`** — auto-configured from `spring.activemq.*` properties. Production tip: wrap in a pooled factory (`spring.activemq.pool.enabled=true` with the pooled-jms dependency) — creating raw connections per send is expensive.
- **`JmsTemplate`** — thread-safe sender; `convertAndSend(dest, obj, postProcessor)` runs the object through the `MessageConverter` bean and lets the post-processor stamp properties (our `messageId`, `seq`).
- **`@JmsListener`** — each annotation gets a listener container from a factory. The container owns sessions/consumers (concurrency), invokes your method, acks on normal return, triggers redelivery on exception. Two factories in this project: default (topic mode, concurrency 1) and `queueListenerFactory` (queue mode, concurrency 3).
- **`MessageConverter`** — single bean shared by both directions; the `_event` type-id property maps records without leaking Java class names into the wire format.
- **`pub-sub-domain`** — the yml switch that decides whether unadorned destinations mean topics or queues; a container factory can override it (exactly what `queueListenerFactory` does).

---

## 18. Monitoring: console, JMX, advisory topics

**Web console** (`:8161/admin`) — Queues (pending/enqueued/dequeued, browse bodies, purge, move), Topics (counters only), Subscribers (durable subs), Connections, Scheduled, Send (inject test messages by hand).

**JMX** — every destination and connection is an MBean (`jconsole` → `org.apache.activemq` domain). The numbers that matter:

| Metric                           | Smell when…                                 |
|----------------------------------|---------------------------------------------|
| `QueueSize`                      | grows steadily → consumers dead or too slow |
| `ConsumerCount`                  | 0 on a queue that should have workers       |
| `EnqueueCount` vs `DequeueCount` | diverging → backlog forming                 |
| `MemoryPercentUsage`             | near 100 → producer flow control imminent   |
| `ExpiredCount`                   | messages dying of TTL unnoticed             |

**Advisory topics** — the broker narrates its own life on `ActiveMQ.Advisory.*` topics: consumer started/stopped, queue created, message DLQ'd, slow consumer detected. Subscribe like any topic — free building block for ops tooling.

```mermaid
flowchart LR
    b((broker)) -->|"ActiveMQ.Advisory.Consumer.Queue.work"| mon[monitoring app]
    b -->|"ActiveMQ.Advisory.MessageDLQd.*"| alert[alerting]
```

---

## 19. ActiveMQ vs Kafka vs RabbitMQ

|                        | ActiveMQ Classic                          | Kafka                                                    | RabbitMQ                         |
|------------------------|-------------------------------------------|----------------------------------------------------------|----------------------------------|
| Model                  | JMS broker (queues + topics)              | Distributed **log** (partitions, offsets)                | AMQP broker (exchanges → queues) |
| Message after consume  | Deleted on ack                            | **Retained** (retention window); consumers track offsets | Deleted on ack                   |
| Replay old messages    | No (gone once acked)                      | Yes — rewind offset                                      | No                               |
| Fan-out + work sharing | Virtual topics                            | Consumer groups (native)                                 | Exchange bound to N queues       |
| Ordering               | Per queue; message groups for keyed order | Per partition (key → partition)                          | Per queue                        |
| Throughput ceiling     | Tens of thousands msg/s                   | Millions msg/s (sequential log, zero-copy)               | Hundreds of thousands            |
| Latency                | Low (push)                                | Low-ish (batched pull)                                   | Low (push)                       |
| Protocol               | OpenWire/JMS + AMQP/STOMP/MQTT            | Custom binary                                            | AMQP 0-9-1                       |
| Delayed/scheduled msgs | Built-in scheduler                        | Not built-in                                             | Plugin / TTL+DLX tricks          |
| Best fit               | JMS shops, task queues, req/reply         | Event streaming, replay, big pipelines                   | Complex routing, multi-protocol  |

The virtual-topic pattern in this repo is ActiveMQ speaking Kafka's dialect: `VirtualTopic.orders` ≈ Kafka topic, group queue ≈ consumer group, competing consumers ≈ partition consumption — minus replay, because queues delete on ack.

---

## 20. How this project maps to all of the above

| Concept (section)               | Where it lives in this repo                                                                               |
|---------------------------------|-----------------------------------------------------------------------------------------------------------|
| Plain topic subscribers (§4)    | direct subscribers on `VirtualTopic.orders`: `OrderCreatedEventListeners` + `DurableOrderListener`                         |
| Typed messages, properties (§5) | `_event` type id, `messageId`, `seq` properties; JSON `TextMessage`                                       |
| Push + prefetch (§6)            | defaults; visible in even 34/33/33 spread                                                                 |
| Auto-ack / redelivery (§7)      | Spring default `AUTO_ACKNOWLEDGE`; throw in a listener to watch redelivery → `ActiveMQ.DLQ`               |
| Competing consumers (§8)        | `queueListenerFactory` concurrency 3-3                                                                    |
| Virtual topics (§10)            | `VirtualTopic.orders` → `Consumer.workerA/workerB.VirtualTopic.orders`; bulk endpoint                     |
| KahaDB (§11)                    | `activemq-data` docker volume                                                                             |
| Console (§18)                   | compose port 8161; worker queues browsable                                                                |
| Spring wiring (§17)             | `JmsEventConverterConfig` (common), `QueueListenerConfig` (consumer), `EventPublisherService` (publisher) |

Experiments to try next, ordered by effort:

1. Throw an exception for `seq % 10 == 0` in a worker listener → watch redelivery then `ActiveMQ.DLQ` fill (§12).
2. Set `?consumer.exclusive=true` on a worker queue destination → observe one thread taking everything (§13).
3. Stamp `JMSXGroupID = orderId` in the bulk publisher → per-order stickiness across the 3 consumers (§13).
4. Add `AMQ_SCHEDULED_DELAY` to one event (enable `schedulerSupport`) → delayed consumption (§14).
5. Stop the consumer, publish a burst, check console: worker queues hold messages (retention), plain topics dropped theirs (§3 vs §4).
