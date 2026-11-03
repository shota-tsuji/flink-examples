package com.example.checkpointer;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Message produced to Kafka by the Go producer. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class UserEvent {

    @JsonProperty("user_id")
    public String userId;

    @JsonProperty("name")
    public String name;

    @JsonProperty("favorite_food")
    public String favoriteFood;

    @JsonProperty("ts")
    public long timestamp;

    public UserEvent() {}

    @Override
    public String toString() {
        return "UserEvent{userId=" + userId + ", name=" + name + ", favoriteFood=" + favoriteFood
                + ", timestamp=" + timestamp + "}";
    }
}
