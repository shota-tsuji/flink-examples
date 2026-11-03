package com.example.checkpointer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * Checkpointer pattern (data processing framework based).
 *
 * <p>Kafka(user-events) -> keyed state (update count per user) -> Redis HSET.
 *
 * <p>Flink periodically snapshots the Kafka offsets and the keyed state into durable storage. After
 * a failure the job resumes from the latest checkpoint instead of reprocessing the topic from the
 * beginning.
 */
public class CheckpointerJob {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String brokers = params.get("kafka.brokers", "kafka:9092");
        String topic = params.get("kafka.topic", "user-events");
        String groupId = params.get("kafka.group-id", "flink-checkpointer");
        String redisHost = params.get("redis.host", "redis");
        int redisPort = params.getInt("redis.port", 6379);
        long checkpointIntervalMs = params.getLong("checkpoint.interval-ms", 10_000L);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpointer: persist source offsets + operator state every interval.
        env.enableCheckpointing(checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // Keep checkpoint files after the job is cancelled so it can be restarted from them.
        checkpointConfig.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.setMinPauseBetweenCheckpoints(checkpointIntervalMs / 2);
        checkpointConfig.setCheckpointTimeout(60_000L);

        KafkaSource<UserEvent> source =
                KafkaSource.<UserEvent>builder()
                        .setBootstrapServers(brokers)
                        .setTopics(topic)
                        .setGroupId(groupId)
                        // Only used on a fresh start; on recovery offsets come from the checkpoint.
                        .setStartingOffsets(
                                OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                        .setValueOnlyDeserializer(new UserEventDeserializationSchema())
                        .build();

        // Stable uids let state be mapped back to operators when restoring from a checkpoint.
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-user-events")
                .uid("kafka-user-events")
                .keyBy(event -> event.userId)
                .process(new UserProfileFunction())
                .uid(UserProfileFunction.UID)
                .name(UserProfileFunction.UID)
                .addSink(new RedisHashSink(redisHost, redisPort))
                .uid("redis-hset-sink")
                .name("redis-hset-sink");

        env.execute("checkpointer-pattern");
    }
}
