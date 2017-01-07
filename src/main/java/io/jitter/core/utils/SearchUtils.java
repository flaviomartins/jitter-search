package io.jitter.core.utils;

import cc.twittertools.index.IndexStatuses;
import cc.twittertools.util.QueryLikelihoodModel;
import com.google.common.collect.Lists;
import io.jitter.api.collectionstatistics.CollectionStats;
import io.jitter.api.search.Document;
import io.jitter.core.document.DocVector;
import io.jitter.core.rerank.DocumentComparator;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.*;

public class SearchUtils {

    public static List<Document> getDocs(IndexSearcher indexSearcher, CollectionStats collectionStats, QueryLikelihoodModel qlModel, TopDocs topDocs, String query, int n, boolean filterRT, boolean computeQLScores) throws IOException {
        IndexReader indexReader = indexSearcher.getIndexReader();

        int count = 0;
        List<Document> topDocuments = Lists.newArrayList();
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            if (count >= n)
                break;

            org.apache.lucene.document.Document hit = indexSearcher.doc(scoreDoc.doc);

            long retweeted_status_id = 0;
            if (hit.get(IndexStatuses.StatusField.RETWEETED_STATUS_ID.name) != null) {
                retweeted_status_id = (Long) hit.getField(IndexStatuses.StatusField.RETWEETED_STATUS_ID.name).numericValue();
            }

            // Throw away retweets.
            if (filterRT && retweeted_status_id != 0) {
                continue;
            }

            Document doc = new Document(hit);

            // Throw away retweets.
            if (filterRT && StringUtils.startsWithIgnoreCase(doc.getText(), "RT ")) {
                continue;
            }

            DocVector docVector = doc.getDocVector();
            if (computeQLScores && docVector == null) {
                DocVector newDocVector = null;
                try {
                    newDocVector = buildDocVector(indexReader, scoreDoc.doc);
                } catch (IOException e) {
                    newDocVector = buildDocVector(IndexStatuses.ANALYZER, doc.getText());
                }
                doc.setDocVector(newDocVector);
            }

            topDocuments.add(doc);
            count += 1;
        }

        if (computeQLScores) {
            return computeQLScores(collectionStats, qlModel, topDocuments, query, n, filterRT, computeQLScores);
        } else {
            return topDocuments;
        }
    }

    public static List<Document> getDocs(IndexSearcher indexSearcher, CollectionStats collectionStats, QueryLikelihoodModel qlModel, TopDocs topDocs, String query, int n, boolean filterRT) throws IOException {
        return getDocs(indexSearcher, collectionStats, qlModel, topDocs, query, n, filterRT, true);
    }

    public static List<Document> computeQLScores(CollectionStats collectionStats, QueryLikelihoodModel qlModel, List<Document> topDocuments, String query, int n, boolean filterRT, boolean computeQLScores) throws IOException {
        Map<String, Float> weights = null;
        HashMap<String, Long> ctfs = null;
        long sumTotalTermFreq = -1;
        if (computeQLScores && qlModel != null) {
            weights = qlModel.parseQuery(IndexStatuses.ANALYZER, query);
            ctfs = new HashMap<>();
            for(String queryTerm: weights.keySet()) {
                long ctf = collectionStats.totalTermFreq(queryTerm);
                ctfs.put(queryTerm, ctf);
            }
            sumTotalTermFreq = collectionStats.getSumTotalTermFreq();
        }

        int count = 0;
        for (Document doc : topDocuments) {
            if (count >= n)
                break;

            DocVector docVector = doc.getDocVector();
            if (computeQLScores && docVector == null) {
                DocVector newDocVector = buildDocVector(IndexStatuses.ANALYZER, doc.getText());
                docVector = newDocVector;
                doc.setDocVector(newDocVector);
            }

            if (computeQLScores && qlModel != null && weights != null && docVector != null) {
                doc.rsv = qlModel.computeQLScore(weights, ctfs, docVector.vector, sumTotalTermFreq);
            } else {
//                doc.rsv = doc.rsv;
            }

            count += 1;
        }

        if (computeQLScores) {
            Comparator<Document> comparator = new DocumentComparator(true);
            Collections.sort(topDocuments, comparator);
        }

        return topDocuments;
    }

    private static LinkedHashMap<String, Integer> getTermsMap(IndexReader indexReader) throws IOException {
        Terms terms = MultiFields.getTerms(indexReader, IndexStatuses.StatusField.TEXT.name);
        TermsEnum termEnum = terms.iterator(null);

        LinkedHashMap<String, Integer> termsMap = new LinkedHashMap<>();
        BytesRef bytesRef;
        int pos = 0;
        while ((bytesRef = termEnum.next()) != null) {
            String term = bytesRef.utf8ToString();
            termsMap.put(term, pos++);
        }

        return termsMap;
    }

    private static DocVector buildDocVector(IndexReader indexReader, int doc) throws IOException {
        DocVector docVector = new DocVector();
        Terms termVector = indexReader.getTermVector(doc, IndexStatuses.StatusField.TEXT.name);
        TermsEnum termsEnum = termVector.iterator(null);
        BytesRef bytesRef;
        while ((bytesRef = termsEnum.next()) != null) {
            String term = bytesRef.utf8ToString();
            int freq = (int) termsEnum.totalTermFreq();
            docVector.setTermFreq(term, freq);
        }
        return docVector;
    }

    private static DocVector buildDocVector(Analyzer analyzer, String text) throws IOException {
        DocVector docVector = new DocVector();
        List<String> docTerms = AnalyzerUtils.analyze(analyzer, text);
        for (String t : docTerms) {
            if (!t.isEmpty()) {
                Integer n = docVector.vector.get(t);
                n = (n == null) ? 1 : ++n;
                docVector.vector.put(t, n);
            }
        }
        return docVector;
    }
}
