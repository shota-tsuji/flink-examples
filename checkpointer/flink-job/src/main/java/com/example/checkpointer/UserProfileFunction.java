package com.example.checkpointer;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Counts events per user in keyed state. The state is part of every checkpoint, so after a
 * restart the count continues from the restored value instead of starting from zero.
 */
public class UserProfileFunction extends KeyedProcessFunction<String, UserEvent, UserProfile> {

    /** Operator uid; state in a checkpoint is looked up by it (see CheckpointStateReader). */
    public static final String UID = "user-profile-state";

    public static ValueStateDescriptor<Long> updateCountDescriptor() {
        return new ValueStateDescriptor<>("update-count", Types.LONG);
    }

    private transient ValueState<Long> updateCount;

    @Override
    public void open(OpenContext openContext) {
        updateCount = getRuntimeContext().getState(updateCountDescriptor());
    }

    @Override
    public void processElement(UserEvent event, Context ctx, Collector<UserProfile> out)
            throws Exception {
        Long current = updateCount.value();
        long next = current == null ? 1L : current + 1L;
        updateCount.update(next);
        out.collect(new UserProfile(event.userId, event.name, event.favoriteFood, next));
    }
}
