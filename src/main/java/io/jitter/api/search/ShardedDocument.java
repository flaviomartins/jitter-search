package io.jitter.api.search;

public interface ShardedDocument {

    String[] getShardIds();

    void setShardIds(String[] shardIds);
}
