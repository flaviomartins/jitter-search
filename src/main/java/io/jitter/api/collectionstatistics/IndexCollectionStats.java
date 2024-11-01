package io.jitter.api.collectionstatistics;

import org.apache.lucene.index.*;

import java.io.*;

public class IndexCollectionStats implements CollectionStats {

    private final IndexReader indexReader;
    private final String field;

    public IndexCollectionStats(IndexReader indexReader, String field) {
        this.indexReader = indexReader;
        this.field = field;
    }

    @Override
    public int docFreq(String term) {
        try {
            return indexReader.docFreq(new Term(field, term));
        } catch (IOException e) {
            return 1;
        }
    }

    @Override
    public long totalTermFreq(String term) {
        try {
            return indexReader.totalTermFreq(new Term(field, term));
        } catch (IOException e) {
            return 1;
        }
    }

    @Override
    public int numDocs() {
        return indexReader.numDocs();
    }

    @Override
    public long getSumDocFreq() {
        try {
            return indexReader.getSumDocFreq(field);
        } catch (IOException e) {
            return -1;
        }
    }

    @Override
    public long getSumTotalTermFreq() {
        try {
            return indexReader.getSumTotalTermFreq(field);
        } catch (IOException e) {
            return -1;
        }
    }

    @Override
    public int numTerms() throws IOException {
        Terms terms = MultiFields.getTerms(indexReader, field);
        TermsEnum termEnum = terms.iterator();

        int termCnt = 0;
        while (termEnum.next() != null) {
            termCnt++;
        }
        return termCnt;
    }

}
