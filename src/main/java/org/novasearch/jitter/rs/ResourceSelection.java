package org.novasearch.jitter.rs;

import cc.twittertools.index.IndexStatuses;
import com.google.common.collect.Lists;
import org.apache.log4j.Logger;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.novasearch.jitter.api.search.Document;
import org.novasearch.jitter.twitter.TwitterManager;
import twitter4j.Status;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class ResourceSelection {
    private static final Logger logger = Logger.getLogger(ResourceSelection.class);

    private static QueryParser QUERY_PARSER =
            new QueryParser(Version.LUCENE_43, IndexStatuses.StatusField.TEXT.name, IndexStatuses.ANALYZER);

    public static enum StatusField {
        ID("id"),
        SCREEN_NAME("screen_name"),
        EPOCH("epoch"),
        TEXT("text"),
        LANG("lang"),
        IN_REPLY_TO_STATUS_ID("in_reply_to_status_id"),
        IN_REPLY_TO_USER_ID("in_reply_to_user_id"),
        FOLLOWERS_COUNT("followers_count"),
        FRIENDS_COUNT("friends_count"),
        STATUSES_COUNT("statuses_count"),
        RETWEETED_STATUS_ID("retweeted_status_id"),
        RETWEETED_USER_ID("retweeted_user_id"),
        RETWEET_COUNT("retweet_count");

        public final String name;

        StatusField(String s) {
            name = s;
        }
    };

    private IndexReader reader;
    private IndexSearcher searcher;

    private final String index;
    private final TwitterManager twitterManager;

    public ResourceSelection(String index, TwitterManager twitterManager) throws IOException {
        this.index = index;
        this.twitterManager = twitterManager;
    }

    public String getIndex() {
        return index;
    }

    public TwitterManager getTwitterManager() {
        return twitterManager;
    }

    public void close() {

    }

    public List<Document> search(String queryText, int queryLimit) {
        File indexPath = new File(index);
        List<Document> results = Lists.newArrayList();
        try {
            reader = DirectoryReader.open(FSDirectory.open(indexPath));
            searcher = new IndexSearcher(reader);
            searcher.setSimilarity(new LMDirichletSimilarity(2500.0f));
            Query q = QUERY_PARSER.parse(queryText);
            int numResults = queryLimit > 10000 ? 10000 : queryLimit;

            TopDocs rs = searcher.search(q, numResults);
            for (ScoreDoc scoreDoc : rs.scoreDocs) {
                org.apache.lucene.document.Document hit = searcher.doc(scoreDoc.doc);

                Document p = new Document();
                p.id = (Long) hit.getField(IndexStatuses.StatusField.ID.name).numericValue();
                p.screen_name = hit.get(IndexStatuses.StatusField.SCREEN_NAME.name);
                p.epoch = (Long) hit.getField(IndexStatuses.StatusField.EPOCH.name).numericValue();
                p.text = hit.get(IndexStatuses.StatusField.TEXT.name);
                p.rsv = scoreDoc.score;

                p.followers_count = (Integer) hit.getField(IndexStatuses.StatusField.FOLLOWERS_COUNT.name).numericValue();
                p.statuses_count = (Integer) hit.getField(IndexStatuses.StatusField.STATUSES_COUNT.name).numericValue();

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
        } catch (Exception e) {
            e.printStackTrace();
        }
        return results;
    }

    public void index() throws IOException {
        File indexPath = new File(index);
        Directory dir = FSDirectory.open(indexPath);
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_43, IndexStatuses.ANALYZER);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

        final FieldType textOptions = new FieldType();
        textOptions.setIndexed(true);
        textOptions.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        textOptions.setStored(true);
        textOptions.setTokenized(true);

        IndexWriter writer = new IndexWriter(dir, config);
        int cnt = 0;
        try {
            twitterManager.start();

            for (String screenName : twitterManager.getUsers()) {
                List<Status> userTimeline = twitterManager.getUserTimeline(screenName);
                for (Status status : userTimeline) {
                    cnt++;
                    org.apache.lucene.document.Document doc = new org.apache.lucene.document.Document();
                    doc.add(new LongField(StatusField.ID.name, status.getId(), Field.Store.YES));
                    doc.add(new LongField(StatusField.EPOCH.name, status.getCreatedAt().getTime(), Field.Store.YES));
                    doc.add(new TextField(StatusField.SCREEN_NAME.name, status.getUser().getScreenName(), Field.Store.YES));

                    doc.add(new Field(StatusField.TEXT.name, status.getText(), textOptions));

                    doc.add(new IntField(StatusField.FRIENDS_COUNT.name, status.getUser().getFollowersCount(), Field.Store.YES));
                    doc.add(new IntField(StatusField.FOLLOWERS_COUNT.name, status.getUser().getFriendsCount(), Field.Store.YES));
                    doc.add(new IntField(StatusField.STATUSES_COUNT.name, status.getUser().getStatusesCount(), Field.Store.YES));

                    long inReplyToStatusId = status.getInReplyToStatusId();
                    if (inReplyToStatusId > 0) {
                        doc.add(new LongField(StatusField.IN_REPLY_TO_STATUS_ID.name, inReplyToStatusId, Field.Store.YES));
                        doc.add(new LongField(StatusField.IN_REPLY_TO_USER_ID.name, status.getInReplyToUserId(), Field.Store.YES));
                    }

                    String lang = status.getLang();
                    if (!lang.equals("unknown")) {
                        doc.add(new TextField(StatusField.LANG.name, status.getLang(), Field.Store.YES));
                    }

                    if (status.isRetweet()) {
                        long retweetStatusId = status.getRetweetedStatus().getId();
                        if (retweetStatusId > 0) {
                            doc.add(new LongField(StatusField.RETWEETED_STATUS_ID.name, retweetStatusId, Field.Store.YES));
                            doc.add(new LongField(StatusField.RETWEETED_USER_ID.name, status.getRetweetedStatus().getUser().getId(), Field.Store.YES));
                            doc.add(new IntField(StatusField.RETWEET_COUNT.name, status.getRetweetCount(), Field.Store.YES));
                            if (status.getRetweetCount() < 0 || status.getRetweetedStatus().getId() < 0) {
                                logger.warn("Error parsing retweet fields of " + status.getId());
                            }
                        }
                    }

                    writer.addDocument(doc);
                    if (cnt % 200 == 0) {
                        logger.info(cnt + " statuses indexed");
                    }
                }
                logger.info(String.format("Total of %s statuses added", cnt));
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            writer.close();
            dir.close();
        }
    }
}
