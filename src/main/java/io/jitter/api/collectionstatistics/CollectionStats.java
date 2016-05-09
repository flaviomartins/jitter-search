package io.jitter.api.collectionstatistics;

import java.io.IOException;

public interface CollectionStats {
    int getDF(String term);

    long getCF(String term);

    double getIDF(String term);

    int getCollectionSize();

    long getCumulativeDocumentFrequency();

    long getCumulativeCollectionFrequency();

    int getTotalTerms() throws IOException;
}
