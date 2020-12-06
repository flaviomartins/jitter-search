package io.jitter.core.utils;

import cc.twittertools.util.QueryLikelihoodModel;
import com.google.common.collect.Lists;
import io.jitter.api.collectionstatistics.CollectionStats;
import io.jitter.api.search.Document;
import io.jitter.api.wikipedia.WikipediaDocument;
import io.jitter.core.document.DocVector;
import io.jitter.core.rerank.DocumentComparator;
import io.jitter.core.wikipedia.WikipediaManager;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.*;

public class WikipediaSearchUtils {

    public static List<WikipediaDocument> getDocs(IndexSearcher indexSearcher, CollectionStats collectionStats, QueryLikelihoodModel qlModel, TopDocs topDocs, String query, int limit, boolean computeQLScores) throws IOException {
        IndexReader indexReader = indexSearcher.getIndexReader();

        int count = 0;
        List<WikipediaDocument> topDocuments = Lists.newArrayList();
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            if (count >= limit)
                break;

            org.apache.lucene.document.Document hit = indexSearcher.doc(scoreDoc.doc);

            WikipediaDocument doc = new WikipediaDocument(hit);
            doc.setRsv(scoreDoc.score);

            DocVector docVector = doc.getDocVector();
            if (computeQLScores && docVector == null) {
                DocVector newDocVector;
                try {
                    newDocVector = buildDocVector(indexReader, scoreDoc.doc);
                } catch (IOException e) {
                    newDocVector = buildDocVector(WikipediaManager.ANALYZER, doc.getText());
                }
                doc.setDocVector(newDocVector);
            }

            topDocuments.add(doc);
            count += 1;
        }

        if (computeQLScores) {
            return computeQLScores(collectionStats, qlModel, topDocuments, query, limit);
        } else {
            return topDocuments;
        }
    }

    public static List<WikipediaDocument> getDocs(IndexSearcher indexSearcher, CollectionStats collectionStats, QueryLikelihoodModel qlModel, TopDocs topDocs, String query, int limit) throws IOException {
        return getDocs(indexSearcher, collectionStats, qlModel, topDocs, query, limit, true);
    }

    public static List<WikipediaDocument> computeQLScores(CollectionStats collectionStats, QueryLikelihoodModel qlModel, List<WikipediaDocument> topDocuments, String query, int limit) throws IOException {
        Map<String, Float> weights = null;
        HashMap<String, Long> ctfs = null;
        long sumTotalTermFreq = -1;
        if (qlModel != null) {
            weights = qlModel.parseQuery(WikipediaManager.ANALYZER, query);
            ctfs = new HashMap<>();
            for(String queryTerm: weights.keySet()) {
                long ctf = collectionStats.totalTermFreq(queryTerm);
                ctfs.put(queryTerm, ctf);
            }
            sumTotalTermFreq = collectionStats.getSumTotalTermFreq();
        }

        for (Document doc : topDocuments) {
            DocVector docVector = doc.getDocVector();
            if (docVector == null && doc.getText() != null) {
                DocVector newDocVector = buildDocVector(WikipediaManager.ANALYZER, doc.getText());
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
        Terms terms = MultiTerms.getTerms(indexReader, WikipediaManager.TEXT_FIELD);
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
        Terms termVector = indexReader.getTermVector(doc, WikipediaManager.TEXT_FIELD);
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
