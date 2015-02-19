package org.novasearch.jitter.core.selection;

import cc.twittertools.index.IndexStatuses;
import com.google.common.collect.Lists;
import io.dropwizard.lifecycle.Managed;
import org.apache.log4j.Logger;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NoSuchDirectoryException;
import org.apache.lucene.util.Version;
import org.novasearch.jitter.api.search.Document;
import org.novasearch.jitter.core.selection.methods.SelectionMethod;
import org.novasearch.jitter.core.selection.methods.SelectionMethodFactory;
import org.novasearch.jitter.core.twitter.manager.TwitterManager;
import org.novasearch.jitter.core.twitter.archiver.TwitterArchiver;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class SelectionManager implements Managed {
    private static final Logger logger = Logger.getLogger(SelectionManager.class);

    private static final QueryParser QUERY_PARSER =
            new QueryParser(Version.LUCENE_43, IndexStatuses.StatusField.TEXT.name, IndexStatuses.ANALYZER);

    private DirectoryReader reader;
    private IndexSearcher searcher;

    private final String indexPath;
    private final String method;
    private final String twitterMode;
    private final boolean removeDuplicates;
    private Map<String, List<String>> topics;
    private TwitterArchiver twitterArchiver;
    private TwitterManager twitterManager;

    public SelectionManager(String indexPath, String method, String twitterMode, boolean removeDuplicates, Map<String, List<String>> topics) {
        this.indexPath = indexPath;
        this.method = method;
        this.twitterMode = twitterMode;
        this.removeDuplicates = removeDuplicates;
        this.topics = topics;
    }

    @Override
    public void start() throws Exception {
        try {
            reader = DirectoryReader.open(FSDirectory.open(new File(indexPath)));
            searcher = new IndexSearcher(reader);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    @Override
    public void stop() throws Exception {
        reader.close();
    }

    public String getMethod() {
        return method;
    }

    public String getIndexPath() {
        return indexPath;
    }

    public TwitterArchiver getTwitterArchiver() {
        return twitterArchiver;
    }

    public TwitterManager getTwitterManager() {
        return twitterManager;
    }

    public String getTwitterMode() {
        return twitterMode;
    }

    public Map<String, List<String>> getTopics() {
        return topics;
    }

    public void setTopics(Map<String, List<String>> topics) {
        this.topics = topics;
    }

    public boolean isRemoveDuplicates() {
        return removeDuplicates;
    }

    public void setTwitterArchiver(TwitterArchiver twitterArchiver) {
        this.twitterArchiver = twitterArchiver;
    }

    public void setTwitterManager(TwitterManager twitterManager) {
        this.twitterManager = twitterManager;
    }

    public SortedMap<String, Float> getRanked(List<Document> results) {
        SelectionMethod selectionMethod = SelectionMethodFactory.getMethod(method);
        return getSortedMap(selectionMethod.getRanked(results));
    }

    public SortedMap<String, Float> getRanked(SelectionMethod selectionMethod, List<Document> results) {
        return getSortedMap(selectionMethod.getRanked(results));
    }

    public SortedMap<String, Float> getRankedTopics(SelectionMethod selectionMethod, List<Document> results) {
        Map<String, Float> ranked = selectionMethod.getRanked(results);
        Map<String, Float> map = new HashMap<>();
        for (String topic : topics.keySet()) {
            float sum = 0;
            for (String collection : topics.get(topic)) {
                for (String col : ranked.keySet()) {
                    if (col.equalsIgnoreCase(collection))
                        sum += ranked.get(col);
                }
            }
            if (sum != 0)
                map.put(topic, sum);
        }
        return getSortedMap(map);
    }

    private SortedMap<String, Float> getSortedMap(Map<String, Float> map) {
        SelectionComparator comparator = new SelectionComparator(map);
        TreeMap<String, Float> sortedMap = new TreeMap<>(comparator);
        sortedMap.putAll(map);
        return sortedMap;
    }

    public List<Document> search(String query, int n) throws IOException, ParseException {
        int numResults = n > 10000 ? 10000 : n;
        Query q = QUERY_PARSER.parse(query);

        List<Document> results = Lists.newArrayList();

        TopDocs rs = getSearcher().search(q, numResults);
        for (ScoreDoc scoreDoc : rs.scoreDocs) {
            org.apache.lucene.document.Document hit = getSearcher().doc(scoreDoc.doc);

            Document p = new Document();
            p.id = (Long) hit.getField(IndexStatuses.StatusField.ID.name).numericValue();
            p.screen_name = hit.get(IndexStatuses.StatusField.SCREEN_NAME.name);
            p.epoch = (Long) hit.getField(IndexStatuses.StatusField.EPOCH.name).numericValue();
            p.text = hit.get(IndexStatuses.StatusField.TEXT.name);
            p.rsv = scoreDoc.score;

            if (hit.get(IndexStatuses.StatusField.FOLLOWERS_COUNT.name) != null) {
                p.followers_count = (Integer) hit.getField(IndexStatuses.StatusField.FOLLOWERS_COUNT.name).numericValue();
            }

            if (hit.get(IndexStatuses.StatusField.STATUSES_COUNT.name) != null) {
                p.statuses_count = (Integer) hit.getField(IndexStatuses.StatusField.STATUSES_COUNT.name).numericValue();
            }

            if (hit.get(IndexStatuses.StatusField.LANG.name) != null) {
                p.lang = hit.get(IndexStatuses.StatusField.LANG.name);
            }

            if (hit.get(IndexStatuses.StatusField.IN_REPLY_TO_STATUS_ID.name) != null) {
                p.in_reply_to_status_id = (Long) hit.getField(IndexStatuses.StatusField.IN_REPLY_TO_STATUS_ID.name).numericValue();
            }

            if (hit.get(IndexStatuses.StatusField.IN_REPLY_TO_USER_ID.name) != null) {
                p.in_reply_to_user_id = (Long) hit.getField(IndexStatuses.StatusField.IN_REPLY_TO_USER_ID.name).numericValue();
            }

            if (hit.get(IndexStatuses.StatusField.RETWEETED_STATUS_ID.name) != null) {
                p.retweeted_status_id = (Long) hit.getField(IndexStatuses.StatusField.RETWEETED_STATUS_ID.name).numericValue();
            }

            if (hit.get(IndexStatuses.StatusField.RETWEETED_USER_ID.name) != null) {
                p.retweeted_user_id = (Long) hit.getField(IndexStatuses.StatusField.RETWEETED_USER_ID.name).numericValue();
            }

            if (hit.get(IndexStatuses.StatusField.RETWEET_COUNT.name) != null) {
                p.retweeted_count = (Integer) hit.getField(IndexStatuses.StatusField.RETWEET_COUNT.name).numericValue();
            }

            results.add(p);
        }
        return results;
    }

    public void index() throws IOException {
        if ("archiver".equals(twitterMode)) {
            logger.info("archiver index");
            twitterArchiver.index(indexPath, removeDuplicates);
        } else if ("standard".equals(twitterMode)) {
            logger.info("standard index");
            twitterManager.index(indexPath, removeDuplicates);
        } else {
            logger.error("Invalid Twitter mode");
        }
    }

    public IndexSearcher getSearcher() throws IOException {
        try {
            if (reader == null) {
                reader = DirectoryReader.open(FSDirectory.open(new File(indexPath)));
                searcher = new IndexSearcher(reader);
                searcher.setSimilarity(new LMDirichletSimilarity(2500.0f));
            } else {
                DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
                if (newReader != null) {
                    reader.close();
                    reader = newReader;
                    searcher = new IndexSearcher(reader);
                    searcher.setSimilarity(new LMDirichletSimilarity(2500.0f));
                }
            }
        } catch (IndexNotFoundException e) {
            logger.error(e);
        }
        return searcher;
    }
}
