package org.novasearch.jitter.core.selection;

import cc.twittertools.index.IndexStatuses;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import io.dropwizard.lifecycle.Managed;
import org.apache.log4j.Logger;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
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
import org.novasearch.jitter.core.selection.methods.SelectionMethod;
import org.novasearch.jitter.core.selection.methods.SelectionMethodFactory;
import org.novasearch.jitter.core.twitter.TwitterManager;
import org.novasearch.jitter.core.twitter.UserTimeline;
import org.novasearch.jitter.core.twitter_archiver.TwitterArchiver;
import org.novasearch.jitter.core.utils.TweetUtils;
import twitter4j.Status;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class SelectionManager implements Managed {
    private static final Logger logger = Logger.getLogger(SelectionManager.class);

    private static final QueryParser QUERY_PARSER =
            new QueryParser(Version.LUCENE_43, IndexStatuses.StatusField.TEXT.name, IndexStatuses.ANALYZER);
    public static final int EXPECTED_COLLECTION_SIZE = 4000;

    private IndexReader reader;
    private IndexSearcher searcher;

    private final String index;
    private final String method;
    private final String twitterMode;
    private final boolean removeDuplicates;
    private Map<String, List<String>> topics;
    private TwitterArchiver twitterArchiver;
    private TwitterManager twitterManager;

    public SelectionManager(String index, String method, String twitterMode, boolean removeDuplicates, Map<String, List<String>> topics) {
        this.index = index;
        this.method = method;
        this.twitterMode = twitterMode;
        this.removeDuplicates = removeDuplicates;
        this.topics = topics;
    }

    @Override
    public void start() throws Exception {
        searcher = getSearcher();
    }

    @Override
    public void stop() throws Exception {
        reader.close();
    }

    public String getMethod() {
        return method;
    }

    public String getIndex() {
        return index;
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
            indexArchiver();
        } else if ("standard".equals(twitterMode)) {
            logger.info("standard index");
            indexManager();
        } else {
            logger.error("Invalid Twitter mode");
        }
    }

    public void indexArchiver() throws IOException {
        File indexPath = new File(index);
        Directory dir = FSDirectory.open(indexPath);
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_43, IndexStatuses.ANALYZER);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

        final FieldType textOptions = new FieldType();
        textOptions.setIndexed(true);
        textOptions.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        textOptions.setStored(true);
        textOptions.setTokenized(true);

        Funnel<String> tweetFunnel = new Funnel<String>() {
            @Override
            public void funnel(String tweetText, PrimitiveSink into) {
                into.putString(TweetUtils.removeAll(tweetText), Charsets.UTF_8);
            }
        };

        int cnt = 0;
        try (IndexWriter writer = new IndexWriter(dir, config)) {
            for (String screenName : twitterArchiver.getUsers()) {
                org.novasearch.jitter.core.twitter_archiver.UserTimeline userTimeline = twitterArchiver.getUserTimeline(screenName);
                if (userTimeline == null)
                    break;
                LinkedHashMap<Long, org.novasearch.jitter.core.twitter_archiver.Status> statuses = userTimeline.getStatuses();
                if (userTimeline.getStatuses() == null)
                    break;

                BloomFilter<String> bloomFilter = null;
                if (removeDuplicates) {
                    bloomFilter = BloomFilter.create(tweetFunnel, EXPECTED_COLLECTION_SIZE);
                }
                for (org.novasearch.jitter.core.twitter_archiver.Status status : statuses.values()) {
                    org.apache.lucene.document.Document doc = new org.apache.lucene.document.Document();
                    doc.add(new LongField(IndexStatuses.StatusField.ID.name, status.getId(), Field.Store.YES));
                    doc.add(new LongField(IndexStatuses.StatusField.EPOCH.name, status.getEpoch(), Field.Store.YES));
                    doc.add(new TextField(IndexStatuses.StatusField.SCREEN_NAME.name, status.getScreenName(), Field.Store.YES));

                    doc.add(new Field(IndexStatuses.StatusField.TEXT.name, status.getText(), textOptions));

                    if (removeDuplicates) {
                        if (bloomFilter.mightContain(status.getText())) {
//                            logger.debug(status.getScreenName() + " duplicate: " + status.getText());
                            continue;
                        }
                    }

                    cnt++;
                    writer.addDocument(doc);

                    if (removeDuplicates) {
                        bloomFilter.put(status.getText());
                    }

                    if (cnt % 1000 == 0) {
                        logger.debug(cnt + " statuses indexed");
                    }
                }
                logger.info(String.format("Total of %s statuses added", cnt));
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            dir.close();
        }
    }

    public void indexManager() throws IOException {
        File indexPath = new File(index);
        Directory dir = FSDirectory.open(indexPath);
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_43, IndexStatuses.ANALYZER);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

        final FieldType textOptions = new FieldType();
        textOptions.setIndexed(true);
        textOptions.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        textOptions.setStored(true);
        textOptions.setTokenized(true);

        int cnt = 0;
        try (IndexWriter writer = new IndexWriter(dir, config)) {
            for (String screenName : twitterManager.getUsers()) {
                UserTimeline userTimeline = twitterManager.getUserTimeline(screenName);
                if (userTimeline == null)
                    break;
                LinkedHashMap<Long, Status> statuses = userTimeline.getStatuses();
                if (userTimeline.getStatuses() == null)
                    break;
                for (Status status : statuses.values()) {
                    cnt++;
                    org.apache.lucene.document.Document doc = new org.apache.lucene.document.Document();
                    doc.add(new LongField(IndexStatuses.StatusField.ID.name, status.getId(), Field.Store.YES));
                    doc.add(new LongField(IndexStatuses.StatusField.EPOCH.name, status.getCreatedAt().getTime(), Field.Store.YES));
                    doc.add(new TextField(IndexStatuses.StatusField.SCREEN_NAME.name, status.getUser().getScreenName(), Field.Store.YES));

                    doc.add(new Field(IndexStatuses.StatusField.TEXT.name, status.getText(), textOptions));

                    doc.add(new IntField(IndexStatuses.StatusField.FRIENDS_COUNT.name, status.getUser().getFollowersCount(), Field.Store.YES));
                    doc.add(new IntField(IndexStatuses.StatusField.FOLLOWERS_COUNT.name, status.getUser().getFriendsCount(), Field.Store.YES));
                    doc.add(new IntField(IndexStatuses.StatusField.STATUSES_COUNT.name, status.getUser().getStatusesCount(), Field.Store.YES));

                    long inReplyToStatusId = status.getInReplyToStatusId();
                    if (inReplyToStatusId > 0) {
                        doc.add(new LongField(IndexStatuses.StatusField.IN_REPLY_TO_STATUS_ID.name, inReplyToStatusId, Field.Store.YES));
                        doc.add(new LongField(IndexStatuses.StatusField.IN_REPLY_TO_USER_ID.name, status.getInReplyToUserId(), Field.Store.YES));
                    }

                    String lang = status.getLang();
                    if (!lang.equals("unknown")) {
                        doc.add(new TextField(IndexStatuses.StatusField.LANG.name, status.getLang(), Field.Store.YES));
                    }

                    if (status.isRetweet()) {
                        long retweetStatusId = status.getRetweetedStatus().getId();
                        if (retweetStatusId > 0) {
                            doc.add(new LongField(IndexStatuses.StatusField.RETWEETED_STATUS_ID.name, retweetStatusId, Field.Store.YES));
                            doc.add(new LongField(IndexStatuses.StatusField.RETWEETED_USER_ID.name, status.getRetweetedStatus().getUser().getId(), Field.Store.YES));
                            doc.add(new IntField(IndexStatuses.StatusField.RETWEET_COUNT.name, status.getRetweetCount(), Field.Store.YES));
                            if (status.getRetweetCount() < 0 || status.getRetweetedStatus().getId() < 0) {
                                logger.warn("Error parsing retweet fields of " + status.getId());
                            }
                        }
                    }

                    writer.addDocument(doc);
                    if (cnt % 1000 == 0) {
                        logger.debug(cnt + " statuses indexed");
                    }
                }
                logger.info(String.format("Total of %s statuses added", cnt));
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            dir.close();
        }
    }

    public IndexSearcher getSearcher() throws IOException {
        try {
            if (searcher == null) {
                reader = DirectoryReader.open(FSDirectory.open(new File(index)));
                searcher = new IndexSearcher(reader);
                searcher.setSimilarity(new LMDirichletSimilarity(2500.0f));
            }
        } catch (IndexNotFoundException e) {
            logger.warn(e);
        }
        return searcher;
    }
}
