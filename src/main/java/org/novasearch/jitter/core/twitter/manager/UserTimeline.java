package org.novasearch.jitter.core.twitter.manager;

import twitter4j.Status;
import twitter4j.User;

import java.util.LinkedHashMap;
import java.util.List;

public class UserTimeline {

    private User user;
    private LinkedHashMap<Long, Status> statuses;

    private long latestId = 1;

    public UserTimeline(User user) {
        this.user = user;
        this.statuses = new LinkedHashMap<>();
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    public LinkedHashMap<Long, Status> getStatuses() {
        return statuses;
    }

    public void setStatuses(LinkedHashMap<Long, Status> statuses) {
        this.statuses = statuses;
    }

    public void add(Status status) {
        statuses.put(status.getId(), status);
        if (latestId < status.getId()) {
            latestId = status.getId();
        }
    }

    public void addAll(List<Status> statuses) {
        for (Status status : statuses) {
            add(status);
        }
    }

    public int size() {
        return statuses.size();
    }

    public long getLatestId() {
        return latestId;
    }
}
