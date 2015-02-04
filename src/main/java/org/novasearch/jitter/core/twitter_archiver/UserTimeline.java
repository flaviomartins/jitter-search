package org.novasearch.jitter.core.twitter_archiver;

import java.util.LinkedHashMap;
import java.util.List;

public class UserTimeline {

    private String user;
    private LinkedHashMap<Long, Status> statuses;

    private long latestId = 1;

    public UserTimeline(String user) {
        this.user = user;
        this.statuses = new LinkedHashMap<>();
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
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
