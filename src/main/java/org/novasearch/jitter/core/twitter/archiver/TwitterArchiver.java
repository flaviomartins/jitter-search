package org.novasearch.jitter.core.twitter.archiver;

import cc.twittertools.index.IndexStatuses;
import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import io.dropwizard.lifecycle.Managed;
import org.apache.commons.io.FilenameUtils;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.novasearch.jitter.core.utils.TweetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TwitterArchiver implements Managed {
    final static Logger logger = LoggerFactory.getLogger(TwitterArchiver.class);

    public static final int EXPECTED_COLLECTION_SIZE = 4000;

    private final String collectionPath;
    private final List<String> screenNames;

    private final Map<String, UserTimeline> userTimelines;

    public TwitterArchiver(String collectionPath, List<String> screenNames) {
        this.collectionPath = collectionPath;
        this.screenNames = screenNames;
        this.userTimelines = new LinkedHashMap<>();
    }

    @Override
    public void start() throws Exception {
        load();
    }

    @Override
    public void stop() throws Exception {

    }

    public UserTimeline getUserTimeline(String screenName) {
        return userTimelines.get(screenName);
    }

    public void loadFromFile(String screenName) throws IOException {
        FileStatusReader fileStatusReader = new FileStatusReader(new File(FilenameUtils.concat(collectionPath, screenName.toLowerCase())));

        UserTimeline timeline;
        if (userTimelines.get(screenName) != null) {
            timeline = userTimelines.get(screenName);
        } else {
            timeline = new UserTimeline(screenName);
            userTimelines.put(screenName, timeline);
        }

        Status status;
        int cnt = 0;
        while (true) {
            status = fileStatusReader.next();
            if (status == null) {
                break;
            }
            if (status.getId() <= timeline.getLatestId()) {
                continue;
            }
            timeline.add(status);
            cnt++;
        }
        fileStatusReader.close();
        if (cnt > 0) {
            logger.info("loaded " + cnt + " " + screenName.toLowerCase());
        }
    }

    public void index(String index, boolean removeDuplicates) throws IOException {
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
            for (String screenName : screenNames) {
                org.novasearch.jitter.core.twitter.archiver.UserTimeline userTimeline = getUserTimeline(screenName);
                if (userTimeline == null)
                    break;
                LinkedHashMap<Long, org.novasearch.jitter.core.twitter.archiver.Status> statuses = userTimeline.getStatuses();
                if (userTimeline.getStatuses() == null)
                    break;

                BloomFilter<String> bloomFilter = null;
                if (removeDuplicates) {
                    bloomFilter = BloomFilter.create(tweetFunnel, EXPECTED_COLLECTION_SIZE);
                }
                for (org.novasearch.jitter.core.twitter.archiver.Status status : statuses.values()) {
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

    public void load() throws IOException {
        logger.info("Loading...");
        for (String screenName : screenNames) {
            loadFromFile(screenName);
        }
    }

    public List<String> getUsers() {
        return screenNames;
    }

}
