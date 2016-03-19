package io.jitter.core.utils;

import cc.twittertools.index.IndexStatuses;
import com.google.common.collect.Lists;
import io.jitter.api.search.Document;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;
import java.util.List;

public class SearchUtils {

    public static List<Document> getDocs(IndexSearcher indexSearcher, TopDocs rs, int n, boolean filterRT) throws IOException {
        int count = 0;

        List<Document> docs = Lists.newArrayList();

        for (ScoreDoc scoreDoc : rs.scoreDocs) {
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

            Document p = new Document(hit);
            p.rsv = scoreDoc.score;
            docs.add(p);

            count += 1;
        }

        return docs;
    }
}
