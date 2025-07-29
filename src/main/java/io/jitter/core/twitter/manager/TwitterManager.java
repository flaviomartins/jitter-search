package io.jitter.core.twitter.manager;

import cc.twittertools.corpus.data.JsonStatusCorpusReader;
import cc.twittertools.corpus.data.StatusStream;
import io.dropwizard.lifecycle.Managed;
import io.jitter.core.analysis.LowercaseKeywordAnalyzer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static cc.twittertools.index.IndexStatuses.StatusField;
import static org.apache.lucene.document.Field.Store;

public class TwitterManager implements Managed {

    private final static Logger logger = LoggerFactory.getLogger(TwitterManager.class);

    // The factory instance is re-usable and thread safe.
    private final Twitter twitter = TwitterFactory.getSingleton();

    public TwitterManager() {
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {

    }

    @SuppressWarnings("UnusedParameters")
    public void index(String collection, String indexPath, Analyzer analyzer) throws IOException {
        long startTime = System.currentTimeMillis();
        File file = new File(collection);
        if (!file.isDirectory()) {
            logger.error("Error: " + file + " does not exist!");
            return;
        }

        StatusStream stream = new JsonStatusCorpusReader(file);

        Directory dir = FSDirectory.open(Paths.get(indexPath));
        
        Map<String, Analyzer> fieldAnalyzers = new HashMap<>();
        fieldAnalyzers.put(StatusField.SCREEN_NAME.name, new LowercaseKeywordAnalyzer());
        PerFieldAnalyzerWrapper perFieldAnalyzerWrapper = new PerFieldAnalyzerWrapper(analyzer, fieldAnalyzers);

        IndexWriterConfig config = new IndexWriterConfig(perFieldAnalyzerWrapper);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

        final FieldType textOptions = new FieldType();
        textOptions.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        textOptions.setStored(true);
        textOptions.setTokenized(true);
        textOptions.setStoreTermVectors(true);
        
        final FieldType screenNameOptions = new FieldType();
        screenNameOptions.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
        screenNameOptions.setStored(true);
        screenNameOptions.setTokenized(true);

        AtomicLong counter = new AtomicLong();
        Status status;
        int commitEvery = 1000;
        try (IndexWriter writer = new IndexWriter(dir, config)) {
            while ((status = stream.next()) != null) {
                Document doc = new Document();
                long id = status.getId();
                doc.add(new LongPoint(StatusField.ID.name, id));
                doc.add(new StoredField(StatusField.ID.name, id));

                doc.add(new LongPoint(StatusField.EPOCH.name, status.getCreatedAt().getTime() / 1000L));
                doc.add(new StoredField(StatusField.EPOCH.name, status.getCreatedAt().getTime() / 1000L));

                doc.add(new Field(StatusField.SCREEN_NAME.name, status.getUser().getScreenName(), screenNameOptions));

                doc.add(new Field(StatusField.TEXT.name, status.getText(), textOptions));

                doc.add(new IntPoint(StatusField.FRIENDS_COUNT.name, status.getUser().getFriendsCount()));
                doc.add(new StoredField(StatusField.FRIENDS_COUNT.name, status.getUser().getFriendsCount()));

                doc.add(new IntPoint(StatusField.FOLLOWERS_COUNT.name, status.getUser().getFollowersCount()));
                doc.add(new StoredField(StatusField.FOLLOWERS_COUNT.name, status.getUser().getFollowersCount()));

                doc.add(new IntPoint(StatusField.STATUSES_COUNT.name, status.getUser().getStatusesCount()));
                doc.add(new StoredField(StatusField.STATUSES_COUNT.name, status.getUser().getStatusesCount()));

                long inReplyToStatusId = status.getInReplyToStatusId();
                if (inReplyToStatusId > 0) {
                    doc.add(new LongPoint(StatusField.IN_REPLY_TO_STATUS_ID.name, inReplyToStatusId));
                    doc.add(new StoredField(StatusField.IN_REPLY_TO_STATUS_ID.name, inReplyToStatusId));

                    doc.add(new LongPoint(StatusField.IN_REPLY_TO_USER_ID.name, status.getInReplyToUserId()));
                    doc.add(new StoredField(StatusField.IN_REPLY_TO_USER_ID.name, status.getInReplyToUserId()));
                }

                String lang = status.getLang();
                if (lang != null) {
                    doc.add(new TextField(StatusField.LANG.name, lang, Store.YES));
                }

                int retweetedStatusRetweetCount = -1;
                Status retweetedStatus = status.getRetweetedStatus();
                if (retweetedStatus != null) {
                    retweetedStatusRetweetCount = retweetedStatus.getRetweetCount();
                    doc.add(new LongPoint(StatusField.RETWEETED_STATUS_ID.name, retweetedStatus.getId()));
                    doc.add(new StoredField(StatusField.RETWEETED_STATUS_ID.name, retweetedStatus.getId()));

                    doc.add(new LongPoint(StatusField.RETWEETED_USER_ID.name, retweetedStatus.getUser().getId()));
                    doc.add(new StoredField(StatusField.RETWEETED_USER_ID.name, retweetedStatus.getUser().getId()));
                }

                int retweetCount = status.getRetweetCount();
                doc.add(new IntPoint(StatusField.RETWEET_COUNT.name, Math.max(retweetCount, retweetedStatusRetweetCount)));
                doc.add(new StoredField(StatusField.RETWEET_COUNT.name, Math.max(retweetCount, retweetedStatusRetweetCount)));

                writer.addDocument(doc);
                if (counter.incrementAndGet() % commitEvery == 0) {
                    logger.debug("{} {} statuses indexed", indexPath, counter.get());
                    writer.commit();
                }
            }

            logger.info(String.format(Locale.ENGLISH, "Total of %s statuses added", counter.get()));
            logger.info("Total elapsed time: " + (System.currentTimeMillis() - startTime) + "ms");
        } catch (Exception e) {
            logger.error("{}", e.getMessage());
        } finally {
            dir.close();
            stream.close();
        }
    }

}
