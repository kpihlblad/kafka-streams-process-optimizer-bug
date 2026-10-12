# Kafka Streams optimizer misplaces repartitions after `KStream.process()`

With `topology.optimization=all`, Kafka Streams treats `KStream.process()` as if it keeps the key. The optimizer can then move a repartition from after `process()` to before it. The outcome depends on the topology shape:

- **Repartitions in series:** `StreamsBuilder.build()` throws `StreamsException: Found a null keyChangingChild node for OptimizableRepartitionNode`.
- **Repartitions in sibling branches:** `build()` succeeds, but the topology has no repartition between a key-changing `process()` and the downstream aggregation. Records are aggregated under keys they are not partitioned by.

Both symptoms disappear with the internal config `__enable.process.processValue.fix__=true` ([KAFKA-19668](https://issues.apache.org/jira/browse/KAFKA-19668)). It defaults to `false`.

Neither symptom needs a broker. The tests build the topology and inspect `Topology.describe()`.

## Affected versions

| Kafka Streams | Exception | Silent variant |
|---|---|---|
| Apache 4.1.2, 4.2.0, 4.3.1 | yes | yes |
| Confluent 8.2.4-ccs, 8.2.4-ce, 8.3.2-ccs, 8.3.2-ce | yes | yes |

Trunk (4.5.0-SNAPSHOT, October 2026) was compared at the source level and not built. The relevant code in `InternalStreamsBuilder` and `KStreamImpl` matches 4.3.1.

## Symptom 1: exception at `build()`

```java
StreamsBuilder builder = new StreamsBuilder();
builder.<String, String>stream("in")
    .selectKey((k, v) -> v)     // key change
    .toTable()                  // implicit repartition 1
    .toStream()
    .process(PassThrough::new)  // forwards each record unchanged
    .toTable();                 // implicit repartition 2

builder.build(props);           // topology.optimization=all
```

```
org.apache.kafka.streams.errors.StreamsException: Found a null keyChangingChild node for OptimizableRepartitionNode{ ... repartitionTopic='KSTREAM-TOTABLE-0000000009-repartition' ... parentNodes=[KSTREAM-PROCESSOR-0000000008] }
    at org.apache.kafka.streams.kstream.internals.InternalStreamsBuilder.mergeRepartitionTopics(InternalStreamsBuilder.java:507)
    at org.apache.kafka.streams.kstream.internals.InternalStreamsBuilder.optimizeTopology(InternalStreamsBuilder.java:357)
    at org.apache.kafka.streams.kstream.internals.InternalStreamsBuilder.buildAndOptimizeTopology(InternalStreamsBuilder.java:310)
    at org.apache.kafka.streams.StreamsBuilder.build(StreamsBuilder.java:595)
```

Each control test changes one thing:

| Test | Change | Result |
|---|---|---|
| `minimal` | none | throws |
| `noOptimization` | `topology.optimization=none` | builds |
| `withoutSelectKey` | no `selectKey()` | builds |
| `repartitionInsteadOfFirstToTable` | `repartition()` instead of `toTable().toStream()` | builds |
| `withoutFirstToTable` | no `toTable().toStream()` before `process()` | builds |
| `selectKeyInsteadOfProcess` | `selectKey()` instead of `process()` | builds |
| `processValueFixEnabled` | `__enable.process.processValue.fix__=true` | builds |

Other variations, checked once and not kept as tests:

- A final `groupByKey().count()` instead of the final `toTable()` also throws.
- A first `groupByKey().count()` instead of the first `toTable()` builds.
- A foreign-key join, a table-table join or a `merge()` between the first `toTable()` and `process()` still throws. The bug was first seen in a topology with a foreign-key join. That topology is kept as the disabled nested class `ProcessOptimizerReproTest.Original`.

## Symptom 2: wrong topology, no exception

```java
KStream<String, String> rekeyed = builder.<String, String>stream("in")
    .selectKey((k, v) -> v, Named.as("rekey"));
rekeyed.groupByKey(Grouped.as("g1")).count(Materialized.as("c1"));
rekeyed.process(KeyChanger::new, Named.as("proc"))   // forwards with key "new-" + value
    .groupByKey(Grouped.as("g2"))
    .count(Materialized.as("c2"));
```

Branch 2 in `Topology.describe()`:

| Configuration | Branch 2 |
|---|---|
| No optimization | `rekey → proc → g2-repartition → count(c2)` |
| Optimization, default | `rekey → g1-repartition → proc → count(c2)` |
| Optimization, fix enabled | `rekey → proc → g2-repartition → count(c2)` |

With the default, `g2-repartition` is merged into `g1-repartition`, which sits upstream of `proc`. Records reach `c2` partitioned by the key from `rekey` but grouped by the key from `proc`. If the new key is not derived only from the old key, one key's aggregate is split across tasks. In every case the `c2` store and its changelog are partitioned by the wrong key, so interactive queries for `c2` can be routed to an instance that does not hold the key.

`fanOutSilentlyDropsRepartitionAfterProcess` asserts that `g2-repartition` exists and fails. `fanOutWithProcessValueFixEnabled` passes. `TopologyTestDriver` processes all partitions as one, so it cannot show wrong counts. The topology description is the evidence.

## Cause

`KStreamImpl.process()` sets `repartitionRequired=true` on the returned stream, so downstream operations add a repartition. It marks its graph node as key-changing only when `__enable.process.processValue.fix__` is set:

```java
if (builder.processProcessValueFixEnabled()) {
    processNode.setKeyChangingOperation(true);
    processNode.setValueChangingOperation(true);
}
```

`process()` is the only operation in `KStreamImpl` with this mismatch. `selectKey()`, `map()`, `flatMap()` and `KTable.toStream(KeyValueMapper)` set both.

During optimization:

1. `getKeyChangingParentNode` searches upstream from the repartition after `process()`. It passes `process()` and stops at `selectKey()`. Both repartitions are now assigned to `selectKey()`.
2. `mergeRepartitionTopics` merges all repartitions assigned to one key-changing node and inserts the merged repartition directly below it.
3. In series, the first repartition is moved under the merged node before the second is handled. The search for the second repartition's path back to `selectKey()` then runs through the moved node and returns `null`, and the optimizer throws.
4. In sibling branches, both paths stay intact, so the merge succeeds and the repartition ends up above `process()`.

## Why the fix is off by default

KAFKA-19668 found that `processValues()` was not marked value-changing. Programs migrated from `transformValues()` could get a different optimized topology and lose local state on upgrade. The fix, in 4.0.1, 4.1.1 and 4.2.0, marks both `process()` and `processValues()`. It is disabled by default because it also changes the optimized topology of existing applications.

[KAFKA-19688](https://issues.apache.org/jira/browse/KAFKA-19688) (KIP-1128) plans new `process()` and `processValues()` overloads with the fix enabled. Its draft PR, [#21003](https://github.com/apache/kafka/pull/21003), was closed as stale in March 2026. The ticket is marked Critical for 4.5.0.

The compatibility argument does not cover these cases. A topology that throws at `build()` cannot be running in production, and the silent variant's current topology is incorrect.

## Workarounds

1. **Enable the fix.** Pass the config to the `StreamsBuilder` constructor. Passing it to `build(props)` has no effect.

   ```java
   props.put(TopologyConfig.InternalConfig.ENABLE_PROCESS_PROCESSVALUE_FIX, true);
   StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(props)));
   ```

   The flag applies to every `process()` and `processValues()` in the topology. If the application already runs with optimization, compare `Topology.describe()` with and without the flag, or deploy under a new `application.id`. If `process()` keeps the key, the cost is one extra repartition topic.

2. **Use `processValues()`** if the processor keeps the key. This removes the extra repartition. It only works if every forward derives from the current input record. A processor that forwards from a punctuator, for example TTL tombstones built with `new Record<>(key, null, ts)`, cannot become a `FixedKeyProcessor`.

3. **Disable optimization** with `topology.optimization=none`. This turns off every other optimization too.

An explicit `repartition()` in place of the first `toTable()` avoids the exception but leaves `process()` misattributed. A later topology change can still produce symptom 2.

## Run

JDK 17 or later and Maven.

```bash
mvn test                           # Apache Kafka Streams 4.3.1
mvn test -Dkafka.version=4.1.2
mvn test -Dkafka.version=8.3.2-ce  # Confluent, from packages.confluent.io
```

Expected: `minimal` and `fanOutSilentlyDropsRepartitionAfterProcess` fail. The other tests pass. The four `Original` tests are skipped.

## Related issues

- [KAFKA-19668](https://issues.apache.org/jira/browse/KAFKA-19668): introduced `__enable.process.processValue.fix__`.
- [KAFKA-19688](https://issues.apache.org/jira/browse/KAFKA-19688): planned overloads with the fix enabled.
- [KAFKA-18692](https://issues.apache.org/jira/browse/KAFKA-18692): proposes merging `repartitionRequired` and `GraphNode.keyChangingOperation`, the two flags that disagree here.
- [KAFKA-9739](https://issues.apache.org/jira/browse/KAFKA-9739): same exception, different cause, fixed in 2.5.1 and 2.6.0.
- [KAFKA-20264](https://issues.apache.org/jira/browse/KAFKA-20264): fixed a match overwrite in `findParentNodeMatching`, in 4.3.1. Does not affect this case.
