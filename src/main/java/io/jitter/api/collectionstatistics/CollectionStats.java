package io.jitter.api.collectionstatistics;

import java.io.IOException;

public interface CollectionStats {
    int docFreq(String term);

    long totalTermFreq(String term);

    int numDocs();

    long getSumDocFreq();

    long getSumTotalTermFreq();

    int numTerms() throws IOException;
}
