# Checkpointer pattern with Apache Flink

A local setup for testing the **Checkpointer** pattern (data processing framework based):
Flink records Kafka offsets and operator state in durable storage. After a failure, the job
resumes from the latest checkpoint and does not reprocess the topic from the beginning.

```
Go producer ──► Kafka (user-events, 3 partitions)
                  │
                  ▼
        Flink DataStream job (Java, Flink 1.20)
          KafkaSource ─► keyBy(user_id) ─► update_count in ValueState ─► Redis sink
                  │                                                          │
     checkpoint every 10s (EXACTLY_ONCE,                                     ▼
     RETAIN_ON_CANCELLATION) ─► volume                   HSET <user_id> name … favorite_food …
                                /opt/flink/checkpoints          update_count …
```

| Service         | Purpose                                                        |
|-----------------|----------------------------------------------------------------|
| `kafka`         | Single-node KRaft broker. `kafka:9092` inside, `localhost:29092` from the host |
| `kafka-init`    | Creates the `user-events` topic                                |
| `jobmanager`    | Flink JobManager, Web UI at http://localhost:8081              |
| `taskmanager`   | Flink TaskManager (2 slots)                                    |
| `job-submitter` | Runs `flink run` once and exits                                |
| `redis`         | Sink, `localhost:6379`                                         |
| `producer`      | Go sample producer: one JSON event every 500ms                 |

## Message and Redis schema

Kafka message (key = `user_id`):

```json
{"user_id":"3","name":"Carol","favorite_food":"ramen","ts":1730592000000}
```

Redis hash (key = `user_id`):

```
HSET 3 name Carol favorite_food ramen update_count 42
```

`update_count` is the number of events seen for the user. It is kept in Flink keyed state, so
it shows whether the state was restored from a checkpoint (it continues counting) or lost
(it would reset to 1).

## Run

```bash
docker compose up -d --build
docker compose logs -f job-submitter   # "Job has been submitted with JobID ..."
```

Check the results:

```bash
docker compose exec redis redis-cli HGETALL 3
docker compose exec redis redis-cli --scan          # all user keys
```

Look at the checkpoints in the UI (Job → Checkpoints) or on disk:

```bash
docker compose exec jobmanager ls -R /opt/flink/checkpoints | head
```

To run the producer from the host instead of in a container:

```bash
docker compose stop producer
cd producer && go run . -brokers localhost:29092 -count 100 -interval 100ms
```

## Test scenarios

### 1. TaskManager crash → automatic recovery

```bash
docker compose exec redis redis-cli HGET 3 update_count   # note the value
docker compose kill taskmanager
sleep 15
docker compose start taskmanager
```

The job goes to RESTARTING and then RUNNING again. In the logs you will see
`Restoring job ... from Checkpoint N`:

```bash
docker compose logs jobmanager | grep -i "restoring job"
```

`update_count` continues from where it was and does not reset to 1. The Kafka source resumes
from the offsets in the checkpoint. Events processed after that checkpoint are replayed and
written to Redis again. HSET overwrites the same values, so the result is correct.

### 2. Sink failure → restart loop until Redis is back

```bash
docker compose stop redis      # the sink throws, and the job restarts every 5s (fixed-delay)
sleep 30
docker compose start redis     # the job recovers from the last successful checkpoint
```

### 3. Cancel the job → restart from the retained checkpoint

`RETAIN_ON_CANCELLATION` keeps the checkpoint files after the job is cancelled, so a new job
can start from them. Run the steps in one shell, because later steps reuse the variables.

The `2>/dev/null` hides the harmless `WARNING: Unknown module: jdk.compiler` lines from the Flink
image. Drop it if a command fails and you need the error.

**Step 1. Cancel the running job**

```bash
# ID of the running streaming job (matched by name, so other jobs are ignored)
JOB_ID=$(docker compose exec -T jobmanager flink list -r 2>/dev/null \
  | awk '/checkpointer-pattern \(RUNNING\)/ {print $4; exit}')
echo "$JOB_ID"

docker compose exec -T jobmanager flink cancel "$JOB_ID" 2>/dev/null
```

**Step 2. Find the retained checkpoint and note Redis**

```bash
CHK=$(docker compose exec -T jobmanager sh -c "ls -d /opt/flink/checkpoints/$JOB_ID/chk-* | sort -V | tail -1")
echo "$CHK"

# No job writes to Redis now, so this value stays fixed
docker compose exec -T redis redis-cli HGET 3 update_count
```

**Step 3. Read the checkpoint: the value the new job will start from**

The cancelled job takes no new checkpoints, so `$CHK` is not deleted and you can read it in place
(see [Read the state inside a checkpoint](#read-the-state-inside-a-checkpoint)):

```bash
docker compose run --rm --no-deps job-submitter flink run -t local \
  -c com.example.checkpointer.CheckpointStateReader /opt/flink/usrlib/flink-checkpointer.jar \
  --checkpoint.path "file://$CHK" 2>/dev/null
```

User 3 in this output can be a little lower than the Redis value from step 2. The cancelled job
kept writing to Redis after its last checkpoint, and the sink's writes are not tied to
checkpoints.

**Step 4. Resume from the checkpoint**

```bash
NEW_JOB_ID=$(docker compose exec -T jobmanager flink run -d -s "$CHK" \
  /opt/flink/usrlib/flink-checkpointer.jar --kafka.brokers kafka:9092 --redis.host redis 2>/dev/null \
  | awk '/submitted with JobID/ {print $NF}')
echo "$NEW_JOB_ID"

# Flink logs a checkpoint passed with -s as a "savepoint"
docker compose logs jobmanager | grep "$NEW_JOB_ID" | grep "from savepoint"
```

**Step 5. Check Redis again**

```bash
docker compose exec -T redis redis-cli HGET 3 update_count
```

**Pass criteria**

- The log line from step 4 names the same `chk-N` as `$CHK`.
- User 3 from step 3 ≤ the Redis value from step 2.
- After the resume, Redis keeps growing from about the step 2 value. It never drops to 1. The new
  job re-reads the messages that came after the checkpoint, so it counts back up to the step 2
  value and continues from there.

Optional exactness check: stop the producer (`docker compose stop producer`), let the job catch up,
and compare each user's Kafka message count with Redis
(see [Count messages per user in Kafka](#count-messages-per-user-in-kafka)). They should be equal.
This only works if every job since the first message was restored from a checkpoint. If not,
start clean with `docker compose down -v && docker compose up -d --build`.

**Negative control**

Repeat the steps but leave out `-s "$CHK"` in step 4. The new job starts with empty state, so
Redis for user 3 drops to 1, 2, 3, … even though Redis still holds the old hash. The job reads
Kafka from the consumer group's committed offsets, which Flink commits on each checkpoint, so it
skips the earlier messages instead of re-counting them.

### Read the state inside a checkpoint

A checkpoint directory holds a binary `_metadata` file. The state here is small, so Flink stores it
inside `_metadata` instead of in separate files. `CheckpointStateReader` uses the
[State Processor API](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/libs/state_processor_api/)
to print the `update-count` keyed state for each user.

Only the 3 latest checkpoints are kept (`execution.checkpointing.num-retained`), and a new one
completes every 10s. Copy the checkpoint you want before Flink deletes it:

```bash
JOB_ID=$(docker compose exec -T jobmanager flink list -r 2>/dev/null \
  | awk '/checkpointer-pattern \(RUNNING\)/ {print $4; exit}')
CHK=$(docker compose exec -T jobmanager sh -c "ls -d /opt/flink/checkpoints/$JOB_ID/chk-* | sort -V | tail -1")
N=$(basename "$CHK")
docker compose exec -T jobmanager sh -c \
  "mkdir -p /opt/flink/checkpoints/inspect/$N && cp -r $CHK/. /opt/flink/checkpoints/inspect/$N/"
echo "copied $CHK"
```

To read a specific checkpoint instead (e.g. `chk-29` from a restore log line), set
`CHK=/opt/flink/checkpoints/$JOB_ID/chk-29` and run the last three lines.

Then read it. `-t local` runs the reader in a mini cluster inside a one-off container. The
session cluster's slots are all used by the streaming job, so a reader submitted there would
wait forever:

```bash
docker compose run --rm --no-deps job-submitter flink run -t local \
  -c com.example.checkpointer.CheckpointStateReader /opt/flink/usrlib/flink-checkpointer.jar \
  --checkpoint.path "file:///opt/flink/checkpoints/inspect/$N" 2>/dev/null
```

```
user_id  update_count
1        113
...
total    1219
```

To check a restore (scenario 1), read the `chk-N` named in the `Restoring job ... from
Checkpoint N` log line. Its counts are the values the restarted job continued from. Redis shows
the same values or higher, never 1. `total` equals the number of Kafka messages the job had
consumed when that checkpoint was taken.

### Count messages per user in Kafka

Compare these counts with `update_count` in Redis. They match only when the job has kept its
state since the first message. A job started without `-s` counts from 1 again. Use
`--max-messages` rather than `--timeout-ms`: the producer never pauses for long, so a timeout
never fires.

```bash
TOTAL=$(docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka:9092 --topic user-events | awk -F: '{s+=$3} END{print s}')
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic user-events --from-beginning \
  --property print.key=true --max-messages "$TOTAL" 2>/dev/null \
  | awk -F'\t' '{c[$1]++} END{for (k in c) print k, c[k]}' | sort -n

for k in $(seq 1 10); do
  echo "$k $(docker compose exec -T redis redis-cli HGET "$k" update_count </dev/null)"
done
```

`</dev/null` stops `docker compose exec` from reading stdin. Without it, a `while read` loop
around this command processes only its first line.

### Clean up

```bash
docker compose down -v   # -v also removes the checkpoint volume
```

## Job parameters

| Argument                   | Default              |
|----------------------------|----------------------|
| `--kafka.brokers`          | `kafka:9092`         |
| `--kafka.topic`            | `user-events`        |
| `--kafka.group-id`         | `flink-checkpointer` |
| `--redis.host`             | `redis`              |
| `--redis.port`             | `6379`               |
| `--checkpoint.interval-ms` | `10000`              |

Checkpoint storage, number of retained checkpoints and the restart strategy are set through
`FLINK_PROPERTIES` in `docker-compose.yml`.
