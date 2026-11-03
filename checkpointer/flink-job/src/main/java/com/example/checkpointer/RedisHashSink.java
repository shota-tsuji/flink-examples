package com.example.checkpointer;

import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.JedisPooled;

/**
 * Writes each profile as {@code HSET <user_id> name <name> favorite_food <food> update_count <n>}.
 *
 * <p>Redis is not transactional with Flink checkpoints, so after a recovery the records since the
 * last checkpoint are written again (at-least-once). HSET is an idempotent overwrite and
 * update_count comes from restored state, so the final Redis content is the same as with
 * exactly-once delivery.
 */
public class RedisHashSink extends RichSinkFunction<UserProfile> {

    private final String host;
    private final int port;

    private transient JedisPooled jedis;

    public RedisHashSink(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public void open(OpenContext openContext) {
        jedis = new JedisPooled(host, port);
    }

    @Override
    public void invoke(UserProfile profile, Context context) {
        // Any Redis error propagates and fails the task, triggering a restore from checkpoint.
        jedis.hset(
                profile.userId,
                Map.of(
                        "name", profile.name,
                        "favorite_food", profile.favoriteFood,
                        "update_count", Long.toString(profile.updateCount)));
    }

    @Override
    public void close() {
        if (jedis != null) {
            jedis.close();
        }
    }
}
