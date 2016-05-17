package io.jitter.core.utils;

import cc.twittertools.index.IndexStatuses;
import com.google.common.collect.Lists;
import io.jitter.api.search.Document;
import io.jitter.core.document.DocVector;
import org.apache.commons.lang.StringUtils;
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

    public static List<Document> getDocs(IndexSearcher indexSearcher, TopDocs topDocs, int n, boolean filterRT, boolean buildDocVector) throws IOException {
        IndexReader indexReader = indexSearcher.getIndexReader();

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

            // Throw away retweets.
            if (filterRT && StringUtils.startsWithIgnoreCase(doc.getText(), "RT ")) {
                continue;
            }

            if (buildDocVector) {
                DocVector docVector = buildDocVector(indexReader, scoreDoc.doc);
                doc.setDocVector(docVector);
            }

            docs.add(doc);
            count += 1;
        }

        return docs;
    }

    public static List<Document> getDocs(IndexSearcher indexSearcher, TopDocs topDocs, int n, boolean filterRT) throws IOException {
        return getDocs(indexSearcher, topDocs, n, filterRT, false);
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
        Terms termVector = indexReader.getTermVector(doc, IndexStatuses.StatusField.TEXT.name);
        TermsEnum termsEnum = termVector.iterator(null);

        BytesRef bytesRef;
        DocVector docVector = new DocVector();
        while ((bytesRef = termsEnum.next()) != null) {
            String term = bytesRef.utf8ToString();
            int freq = (int) termsEnum.totalTermFreq();
            docVector.setTermFreq(term, freq);
        }
        return docVector;
    }
}
