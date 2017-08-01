package io.jitter.api.search;

import io.jitter.core.document.DocVector;

public abstract class AbstractDocument implements Document, ShardedDocument {
    private DocVector docVector;

    public abstract String getId();

    public abstract void setId(String id);

    public abstract double getRsv();

    public abstract void setRsv(double score);

    public abstract String getText();

    public abstract void setText(String text);

    public DocVector getDocVector() {
        return docVector;
    }

    public void setDocVector(DocVector docVector) {
        this.docVector = docVector;
    }

    public abstract String[] getShardIds();

    public abstract void setShardIds(String[] shardIds);
}
