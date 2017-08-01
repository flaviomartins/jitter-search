package io.jitter.api.search;

public interface ShardedDocument {
    double getRsv();

    void setRsv(double rsv);
    
    String[] getShardIds();

    void setShardIds(String[] shardIds);
}
