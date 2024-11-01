package io.jitter.core.utils;

import cc.twittertools.index.IndexStatuses;
import cc.twittertools.util.QueryLikelihoodModel;
import com.google.common.collect.Lists;
import io.jitter.api.collectionstatistics.CollectionStats;
import io.jitter.api.search.StatusDocument;
import io.jitter.api.search.Document;
import io.jitter.core.document.DocVector;
import io.jitter.core.rerank.DocumentComparator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
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

    public static List<StatusDocument> getDocs(IndexSearcher indexSearcher, Analyzer analyzer, CollectionStats collectionStats, QueryLikelihoodModel qlModel, TopDocs topDocs, String query, int limit, boolean filterRT, boolean computeQLScores) throws IOException {
        IndexReader indexReader = indexSearcher.getIndexReader();

        int count = 0;
        LongOpenHashSet seenSet = new LongOpenHashSet();
        List<StatusDocument> topDocuments = Lists.newArrayList();
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            if (count >= limit)
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

            StatusDocument doc = new StatusDocument(hit);
            doc.setRsv(scoreDoc.score);

            // Throw away retweets.
            if (filterRT && StringUtils.startsWithIgnoreCase(doc.getText(), "RT ")) {
                continue;
            }

            DocVector docVector = doc.getDocVector();
            if (computeQLScores && docVector == null) {
                DocVector newDocVector;
                try {
                    newDocVector = buildDocVector(indexReader, scoreDoc.doc);
                } catch (IOException e) {
                    newDocVector = buildDocVector(analyzer, doc.getText());
                }
                doc.setDocVector(newDocVector);
            }

            if (seenSet.contains(Long.parseLong(doc.getId())))
                continue;

            seenSet.add(Long.parseLong(doc.getId()));

            topDocuments.add(doc);
            count += 1;
        }

        if (computeQLScores) {
            return computeQLScores(analyzer, collectionStats, qlModel, topDocuments, query, limit);
        } else {
            return topDocuments;
        }
    }

    public static List<StatusDocument> getDocs(IndexSearcher indexSearcher, Analyzer analyzer, CollectionStats collectionStats, QueryLikelihoodModel qlModel, TopDocs topDocs, String query, int limit, boolean filterRT) throws IOException {
        return getDocs(indexSearcher, analyzer, collectionStats, qlModel, topDocs, query, limit, filterRT, true);
    }

    public static List<StatusDocument> computeQLScores(Analyzer analyzer, CollectionStats collectionStats, QueryLikelihoodModel qlModel, List<StatusDocument> topDocuments, String query, int limit) throws IOException {
        Map<String, Float> weights = null;
        HashMap<String, Long> ctfs = null;
        long sumTotalTermFreq = -1;
        if (qlModel != null) {
            weights = qlModel.parseQuery(analyzer, query);
            ctfs = new HashMap<>();
            for(String queryTerm: weights.keySet()) {
                long ctf = collectionStats.totalTermFreq(queryTerm);
                ctfs.put(queryTerm, ctf);
            }
            sumTotalTermFreq = collectionStats.getSumTotalTermFreq();
        }

        for (StatusDocument doc : topDocuments) {
            DocVector docVector = doc.getDocVector();
            if (docVector == null && doc.getText() != null) {
                DocVector newDocVector = buildDocVector(analyzer, doc.getText());
                docVector = newDocVector;
                doc.setDocVector(newDocVector);
            }

            if (qlModel != null && weights != null && docVector != null) {
                doc.setRsv(qlModel.computeQLScore(weights, ctfs, docVector.vector, sumTotalTermFreq));
            }
        }

        Comparator<Document> comparator = new DocumentComparator(true);
        topDocuments.sort(comparator);

        return topDocuments.subList(0, Math.min(limit, topDocuments.size()));
    }

    private static LinkedHashMap<String, Integer> getTermsMap(IndexReader indexReader) throws IOException {
        Terms terms = MultiFields.getTerms(indexReader, IndexStatuses.StatusField.TEXT.name);
        TermsEnum termEnum = terms.iterator();

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
        DocVector docVector = null;
        Terms termVector = indexReader.getTermVector(doc, IndexStatuses.StatusField.TEXT.name);
        if (termVector != null) {
            docVector = new DocVector();
            TermsEnum termsEnum = termVector.iterator();
            BytesRef bytesRef;
            while ((bytesRef = termsEnum.next()) != null) {
                String term = bytesRef.utf8ToString();
                int freq = (int) termsEnum.totalTermFreq();
                docVector.setTermFreq(term, freq);
            }
        }
        return docVector;
    }

    public static DocVector buildDocVector(Analyzer analyzer, String text) throws IOException {
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
