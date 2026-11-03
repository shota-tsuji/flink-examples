package com.example.checkpointer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Parses JSON user events. Malformed or incomplete records are logged and skipped. */
public class UserEventDeserializationSchema extends AbstractDeserializationSchema<UserEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(UserEventDeserializationSchema.class);

    private transient ObjectMapper mapper;

    @Override
    public void open(InitializationContext context) {
        mapper = new ObjectMapper();
    }

    @Override
    public UserEvent deserialize(byte[] message) {
        try {
            UserEvent event = mapper.readValue(message, UserEvent.class);
            if (event.userId == null || event.name == null || event.favoriteFood == null) {
                LOG.warn("Skipping incomplete event: {}", event);
                return null;
            }
            return event;
        } catch (Exception e) {
            LOG.warn("Skipping malformed event: {}", new String(message), e);
            // Returning null drops the record instead of failing the job.
            return null;
        }
    }
}
