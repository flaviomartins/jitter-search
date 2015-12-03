package io.jitter.api.collectionstatistics;

public interface CollectionStats {
    int getDF(String term);

    long getCF(String term);

    double getIDF(String term);

    int getCollectionSize();

    long getCumulativeDocumentFrequency();

    long getCumulativeCollectionFrequency();
}
