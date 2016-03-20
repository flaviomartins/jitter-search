package io.jitter.core.utils;

import cc.twittertools.index.IndexStatuses;
import com.google.common.collect.Lists;
import io.jitter.api.search.Document;
import io.jitter.core.document.DocVector;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

public class SearchUtils {

    public static List<Document> getDocs(IndexSearcher indexSearcher, TopDocs topDocs, int n, boolean filterRT) throws IOException {
        IndexReader indexReader = indexSearcher.getIndexReader();

        LinkedHashMap<String, Integer> termsMap = getTermsMap(indexReader);

        int count = 0;
        List<Document> docs = Lists.newArrayList();
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
            doc.rsv = scoreDoc.score;
            DocVector docVector = buildDocVector(indexReader, termsMap, scoreDoc.doc);
            doc.setDocVector(docVector);

            docs.add(doc);
            count += 1;
        }

        return docs;
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

    private static DocVector buildDocVector(IndexReader indexReader, LinkedHashMap termsMap, int doc) throws IOException {
        Terms vector = indexReader.getTermVector(doc, IndexStatuses.StatusField.TEXT.name);
        TermsEnum termsEnum = null;
        termsEnum = vector.iterator(termsEnum);
        BytesRef text;
        DocVector docVector = new DocVector(termsMap);
        while ((text = termsEnum.next()) != null) {
            String term = text.utf8ToString();
            int freq = (int) termsEnum.totalTermFreq();
            docVector.setEntry(term, freq);
        }
        return docVector;
    }
}
