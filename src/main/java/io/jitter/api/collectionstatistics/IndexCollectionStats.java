package io.jitter.api.collectionstatistics;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

import java.io.*;

public class IndexCollectionStats implements CollectionStats {
    private static final Logger LOG = Logger.getLogger(IndexCollectionStats.class);

    private final IndexReader indexReader;
    private final String field;

    public IndexCollectionStats(IndexReader indexReader, String field) {
        this.indexReader = indexReader;
        this.field = field;
    }

    public int getDF(String term) {
        try {
            return indexReader.docFreq(new Term(term));
        } catch (IOException e) {
            return 10;
        }
    }

    public long getCF(String term) {
        try {
            return indexReader.totalTermFreq(new Term(term));
        } catch (IOException e) {
            return 10;
        }
    }

    public double getIDF(String term) {
        return Math.log(1.0 + (double)getCollectionSize() / (double)getDF(term));
    }

    public int getCollectionSize() {
        return indexReader.numDocs();
    }

    public long getCumulativeDocumentFrequency() {
        try {
            return indexReader.getSumDocFreq(field);
        } catch (IOException e) {
            return -1;
        }
    }

    public long getCumulativeCollectionFrequency() {
        try {
            return indexReader.getSumTotalTermFreq(field);
        } catch (IOException e) {
            return -1;
        }
    }

}
