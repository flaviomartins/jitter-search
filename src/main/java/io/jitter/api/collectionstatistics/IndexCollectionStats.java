package io.jitter.api.collectionstatistics;

import cc.twittertools.index.IndexStatuses;
import org.apache.log4j.Logger;
import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRef;

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

    public int getTotalTerms() throws IOException {
        Terms terms = MultiFields.getTerms(indexReader, IndexStatuses.StatusField.TEXT.name);
        TermsEnum termEnum = terms.iterator(null);

        int termCnt = 0;
        while (termEnum.next() != null) {
            termCnt++;
        }
        return termCnt;
    }

}
