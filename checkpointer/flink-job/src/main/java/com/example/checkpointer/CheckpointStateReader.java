package com.example.checkpointer;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.SavepointReader;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

/**
 * Prints the per-user update_count stored in a checkpoint (or savepoint), using the State
 * Processor API.
 *
 * <pre>
 * flink run -c com.example.checkpointer.CheckpointStateReader flink-checkpointer.jar \
 *     --checkpoint.path file:///opt/flink/checkpoints/&lt;job-id&gt;/chk-&lt;n&gt;
 * </pre>
 */
public class CheckpointStateReader {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String checkpointPath = params.getRequired("checkpoint.path");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        SavepointReader checkpoint =
                SavepointReader.read(env, checkpointPath, new HashMapStateBackend());
        DataStream<Tuple2<String, Long>> counts =
                checkpoint.readKeyedState(
                        OperatorIdentifier.forUid(UserProfileFunction.UID),
                        new UpdateCountReader());

        List<Tuple2<String, Long>> rows = new ArrayList<>();
        try (CloseableIterator<Tuple2<String, Long>> it =
                counts.executeAndCollect("read-checkpoint-state")) {
            it.forEachRemaining(rows::add);
        }
        rows.sort(Comparator.comparing((Tuple2<String, Long> row) -> Integer.parseInt(row.f0)));

        System.out.println("checkpoint: " + checkpointPath);
        System.out.println("user_id\tupdate_count");
        long total = 0;
        for (Tuple2<String, Long> row : rows) {
            System.out.println(row.f0 + "\t" + row.f1);
            total += row.f1;
        }
        System.out.println("total\t" + total);
    }

    /** Emits (user_id, update_count) for every key in the operator's keyed state. */
    static class UpdateCountReader extends KeyedStateReaderFunction<String, Tuple2<String, Long>> {

        private transient ValueState<Long> updateCount;

        @Override
        public void open(Configuration parameters) {
            updateCount = getRuntimeContext().getState(UserProfileFunction.updateCountDescriptor());
        }

        @Override
        public void readKey(String userId, Context ctx, Collector<Tuple2<String, Long>> out)
                throws Exception {
            out.collect(Tuple2.of(userId, updateCount.value()));
        }
    }
}
