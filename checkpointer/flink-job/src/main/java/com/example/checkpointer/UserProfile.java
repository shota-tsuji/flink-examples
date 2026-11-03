package com.example.checkpointer;

/** Latest profile of a user, written to Redis as a hash. */
public class UserProfile {

    public String userId;
    public String name;
    public String favoriteFood;
    /** Number of events seen for this user; kept in checkpointed keyed state. */
    public long updateCount;

    public UserProfile() {}

    public UserProfile(String userId, String name, String favoriteFood, long updateCount) {
        this.userId = userId;
        this.name = name;
        this.favoriteFood = favoriteFood;
        this.updateCount = updateCount;
    }
}
