# Kafka Streams optimizer misplaces repartitions after `process()`

With `topology.optimization=all`, the optimizer treats `KStream.process()` as key-preserving. It can move a repartition from after `process()` to before it:

- **In series:** `build()` throws `Found a null keyChangingChild node for OptimizableRepartitionNode`.
- **In sibling branches:** `build()` succeeds, but the aggregation after a key-changing `process()` has no repartition.

The internal config `__enable.process.processValue.fix__=true` ([KAFKA-19668](https://issues.apache.org/jira/browse/KAFKA-19668)) fixes both. It defaults to `false`.

Affected: Apache 4.1.2, 4.2.0, 4.3.1 and Confluent 8.2.4, 8.3.2 (`-ccs` and `-ce`). Trunk (4.5.0-SNAPSHOT, October 2026) has the same code as 4.3.1.

## Failure modes
There are two failure modes, one throwing an exception and one *silently* misses the repartition.

### Exception
```java
builder.<String, String>stream("in")
    .selectKey((k, v) -> v)
    .toTable()                  // repartition 1
    .toStream()
    .process(PassThrough::new)  // forwards records unchanged
    .toTable();                 // repartition 2
builder.build(props);           // throws
```

It builds if optimization is off, `selectKey()` or the first `toTable()` is removed, `process()` becomes `selectKey()`, or the fix is on. A join or `merge()` before `process()` still throws.

### Missing repartition

```java
KStream<String, String> rekeyed = builder.<String, String>stream("in")
    .selectKey((k, v) -> v, Named.as("rekey"));
rekeyed.groupByKey(Grouped.as("g1")).count(Materialized.as("c1"));
rekeyed.process(KeyChanger::new, Named.as("proc"))   // new key
    .groupByKey(Grouped.as("g2"))
    .count(Materialized.as("c2"));
```

| Configuration | Branch 2 |
|---|---|
| No optimization | `rekey → proc → g2-repartition → count(c2)` |
| Optimization | `rekey → g1-repartition → proc → count(c2)` |
| Optimization + fix | `rekey → proc → g2-repartition → count(c2)` |

`c2` aggregates by the new key but is partitioned by the *old* key.

## Cause

`process()` sets `repartitionRequired=true` but calls `setKeyChangingOperation(true)` only when the fix is on. The optimizer assigns the repartition after `process()` to the upstream `selectKey()` and merges it into the repartition there. In series, `mergeRepartitionTopics` loses the path back to `selectKey()` and throws. In branches, the merge succeeds.

The fix is off by default because it changes existing optimized topologies. [KAFKA-19688](https://issues.apache.org/jira/browse/KAFKA-19688) (KIP-1128) plans overloads with the fix on. Its PR, [#21003](https://github.com/apache/kafka/pull/21003), went stale.

## Workarounds

1. Enable the fix in the `StreamsBuilder` constructor. Setting it in `build(props)` has no effect.

   ```java
   props.put(TopologyConfig.InternalConfig.ENABLE_PROCESS_PROCESSVALUE_FIX, true);
   StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(props)));
   ```

   This can change the topology of a running app. Compare `Topology.describe()` first.

2. Use `processValues()` if the key never changes and nothing is forwarded from a punctuator. A key-preserving processor that forwards from a punctuator, such as TTL tombstones for its own stored keys, has no API to say so. It must use `process()` and pay for a repartition it doesn't need.
3. Set `topology.optimization=none`.

## Run

No broker needed. The tests build topologies and check `Topology.describe()`.

```bash
mvn test                           # 4.3.1
mvn test -Dkafka.version=8.3.2-ce  # Confluent
```

`minimal` and `fanOutSilentlyDropsRepartitionAfterProcess` fail. The rest pass.

## Related

[KAFKA-18692](https://issues.apache.org/jira/browse/KAFKA-18692) (unify the two flags), [KAFKA-9739](https://issues.apache.org/jira/browse/KAFKA-9739) (same exception, older cause).

## License

Apache License 2.0.
