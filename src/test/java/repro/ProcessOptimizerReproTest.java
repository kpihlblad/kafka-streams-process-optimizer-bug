package repro;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.Properties;
import java.util.function.Consumer;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.TableJoined;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * With topology.optimization=all, the optimizer treats KStream.process() as key-preserving unless
 * __enable.process.processValue.fix__ is set. See README.md.
 * <p>
 * Expected failures: {@link #minimal()} throws "Found a null keyChangingChild node", and
 * {@link #fanOutSilentlyDropsRepartitionAfterProcess()} finds no repartition after process().
 * Every other top-level test changes one thing and passes. The first reproducer is kept,
 * disabled, in {@link Original}.
 */
class ProcessOptimizerReproTest {

    @Test
    void minimal() {
        build(StreamsConfig.OPTIMIZE, b -> b.<String, String>stream("in")
            .selectKey((k, v) -> v)
            .toTable()
            .toStream()
            .process(PassThrough::new)
            .toTable());
    }

    @Test
    void noOptimization() {
        build(StreamsConfig.NO_OPTIMIZATION, b -> b.<String, String>stream("in")
            .selectKey((k, v) -> v)
            .toTable()
            .toStream()
            .process(PassThrough::new)
            .toTable());
    }

    @Test
    void withoutSelectKey() {
        build(StreamsConfig.OPTIMIZE, b -> b.<String, String>stream("in")
            .toTable()
            .toStream()
            .process(PassThrough::new)
            .toTable());
    }

    @Test
    void repartitionInsteadOfFirstToTable() {
        build(StreamsConfig.OPTIMIZE, b -> b.<String, String>stream("in")
            .selectKey((k, v) -> v)
            .repartition()
            .process(PassThrough::new)
            .toTable());
    }

    @Test
    void withoutFirstToTable() {
        build(StreamsConfig.OPTIMIZE, b -> b.<String, String>stream("in")
            .selectKey((k, v) -> v)
            .process(PassThrough::new)
            .toTable());
    }

    @Test
    void selectKeyInsteadOfProcess() {
        build(StreamsConfig.OPTIMIZE, b -> b.<String, String>stream("in")
            .selectKey((k, v) -> v)
            .toTable()
            .toStream()
            .selectKey((k, v) -> v)
            .toTable());
    }

    @Test
    void processValueFixEnabled() {
        Properties props = props(StreamsConfig.OPTIMIZE);
        props.put(TopologyConfig.InternalConfig.ENABLE_PROCESS_PROCESSVALUE_FIX, true);

        StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(props)));
        builder.<String, String>stream("in")
            .selectKey((k, v) -> v)
            .toTable()
            .toStream()
            .process(PassThrough::new)
            .toTable();
        builder.build(props);
    }

    /**
     * Same root cause, but the optimizer does not throw: it silently drops the repartition
     * that the key-changing process() requires before the second aggregation.
     */
    @Test
    void fanOutSilentlyDropsRepartitionAfterProcess() {
        assertEquals(true, fanOutTopology(false).contains("g2-repartition"),
            "optimized topology has no repartition between process() and the aggregation");
    }

    @Test
    void fanOutWithProcessValueFixEnabled() {
        assertEquals(true, fanOutTopology(true).contains("g2-repartition"));
    }

    private static String fanOutTopology(boolean processValueFix) {
        Properties props = props(StreamsConfig.OPTIMIZE);
        if (processValueFix) {
            props.put(TopologyConfig.InternalConfig.ENABLE_PROCESS_PROCESSVALUE_FIX, true);
        }
        StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(props)));
        KStream<String, String> rekeyed = builder.<String, String>stream("in")
            .selectKey((k, v) -> v, Named.as("rekey"));
        rekeyed.groupByKey(Grouped.as("g1")).count(Materialized.as("c1"));
        rekeyed.process(KeyChanger::new, Named.as("proc"))
            .groupByKey(Grouped.as("g2"))
            .count(Materialized.as("c2"));
        return builder.build(props).describe().toString();
    }

    static class KeyChanger implements Processor<String, String, String, String> {
        private ProcessorContext<String, String> ctx;

        @Override
        public void init(ProcessorContext<String, String> context) {
            ctx = context;
        }

        @Override
        public void process(Record<String, String> record) {
            ctx.forward(record.withKey("new-" + record.value()));
        }
    }

    private static Properties props(String optimization) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "repro");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, optimization);
        return props;
    }

    private static void build(String optimization, Consumer<StreamsBuilder> topology) {
        Properties props = props(optimization);

        StreamsBuilder builder = new StreamsBuilder();
        topology.accept(builder);
        builder.build(props);
    }

    static class PassThrough implements Processor<String, String, String, String> {
        private ProcessorContext<String, String> ctx;

        @Override
        public void init(ProcessorContext<String, String> context) {
            ctx = context;
        }

        @Override
        public void process(Record<String, String> record) {
            ctx.forward(record);
        }
    }

    /**
     * Original, larger reproducer. Kept for reference.
     */
    @Nested
    @Disabled("Original reproducer; superseded by the minimal tests above")
    class Original {

        @TempDir
        Path stateDir;

        @Test
        void optimizeWithFkJoinAndProcessToTable() {
            run(StreamsConfig.OPTIMIZE, true, true);
        }

        @Test
        void noOptimization() {
            run(StreamsConfig.NO_OPTIMIZATION, true, true);
        }

        @Test
        void optimizeWithoutFkJoin() {
            run(StreamsConfig.OPTIMIZE, false, true);
        }

        @Test
        void optimizeWithoutProcessToTable() {
            run(StreamsConfig.OPTIMIZE, true, false);
        }

        private void run(String optimization, boolean fkJoin, boolean processToTable) {
            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "repro");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
            props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
            props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, optimization);

            StreamsBuilder builder = new StreamsBuilder();

            // rekey + toTable (implicit repartition)
            KTable<String, String> status = builder.<String, String>stream("status-in")
                .selectKey((k, v) -> v, Named.as("rekey"))
                .toTable(Named.as("status-named"), Materialized.as("status-store"));

            KTable<String, String> actions = builder.<String, String>stream("actions-in")
                .toTable(Named.as("actions-named"), Materialized.as("actions-store"));

            // foreign-key join
            KTable<String, String> joined = fkJoin
                ? actions.leftJoin(status, v -> v, (a, s) -> a + "|" + s,
                    TableJoined.as("fkJoin"), Materialized.as("joined-store"))
                : actions;

            if (processToTable) {
                // process() (key-changing) + toTable (implicit repartition) + groupBy/count
                joined.toStream(Named.as("joined-to-stream"))
                    .process(PassThrough::new, Named.as("proc"))
                    .toTable(Named.as("mapped-named"), Materialized.as("mapped-store"))
                    .groupBy((k, v) -> KeyValue.pair(v, v), Grouped.as("grouped"))
                    .count(Named.as("count"), Materialized.as("count-store"))
                    .toStream(Named.as("count-to-stream"))
                    .mapValues(String::valueOf, Named.as("count-to-string"))
                    .to("out");
            } else {
                joined.toStream(Named.as("joined-to-stream")).to("out");
            }

            try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(props), props)) {
                driver.createInputTopic("actions-in", Serdes.String().serializer(), Serdes.String().serializer())
                    .pipeInput("k1", "o1");
                driver.createInputTopic("status-in", Serdes.String().serializer(), Serdes.String().serializer())
                    .pipeInput("x", "o1");
                var out = driver.createOutputTopic("out", Serdes.String().deserializer(), Serdes.String().deserializer());
                assertEquals(false, out.isEmpty());
            }
        }
    }
}
