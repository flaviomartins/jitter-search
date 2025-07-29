package io.jitter.core.shards;

import cc.twittertools.index.IndexStatuses;
import com.google.common.collect.ImmutableSortedSet;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class ShardStatsBuilder {

    private static final Logger logger = LoggerFactory.getLogger(ShardStatsBuilder.class);

    private final IndexReader reader;
    private final Map<String, ImmutableSortedSet<String>> topics;
    
    private Map<String, String> reverseTopicMap;
    private ShardStats collectionsShardStats;
    private ShardStats topicsShardStats;

    public ShardStatsBuilder(IndexReader reader, Map<String, ImmutableSortedSet<String>> topics) throws IOException {
        this.reader = reader;
        this.topics = topics;
        reverseMap(topics);
    }

    public Map<String, String> getReverseTopicMap() {
        return reverseTopicMap;
    }

    public ShardStats getCollectionsShardStats() {
        return collectionsShardStats;
    }

    public void setCollectionsShardStats(ShardStats collectionsShardStats) {
        this.collectionsShardStats = collectionsShardStats;
    }

    public ShardStats getTopicsShardStats() {
        return topicsShardStats;
    }

    public void setTopicsShardStats(ShardStats topicsShardStats) {
        this.topicsShardStats = topicsShardStats;
    }

    public void reverseMap(Map<String, ImmutableSortedSet<String>> topics) {
        Map<String, String> reverseMap = new HashMap<>();
        for (Map.Entry<String, ImmutableSortedSet<String>> entry : topics.entrySet()) {
            String topic = entry.getKey();
            for (String col : entry.getValue()) {
                reverseMap.put(col.toLowerCase(Locale.ROOT), topic.toLowerCase(Locale.ROOT));
            }
        }
        this.reverseTopicMap = reverseMap;
    }

    public void collectStats() throws IOException {
        Map<String, Integer> collectionsSizes = new HashMap<>();
        Map<String, Integer> topicsSizes = new HashMap<>();

        Terms terms = MultiTerms.getTerms(reader, IndexStatuses.StatusField.SCREEN_NAME.name);
        TermsEnum termEnum = terms.iterator();

        int colCnt = 0;
        BytesRef bytesRef;
        while ((bytesRef = termEnum.next()) != null) {
            String term = bytesRef.utf8ToString();
            int docFreq = termEnum.docFreq();
            if (term.isEmpty()) {
                continue;
            }

            String collection = term.toLowerCase(Locale.ROOT);

            if (reverseTopicMap.containsKey(collection)) {
                collectionsSizes.put(collection, docFreq);
                logger.debug("collection {}: {} = {}", collection, "#d", docFreq);
            } else {
                logger.warn("collection {}: {} = {}", collection, "#d", docFreq);
            }

            colCnt++;
        }
        logger.info("total collections: " + colCnt);

        for (String topic : topics.keySet()) {
            int docFreq = 0;
            for (String collection : topics.get(topic)) {
                Integer sz = collectionsSizes.get(collection);
                if (sz != null)
                    docFreq += sz;
            }

            topicsSizes.put(topic, docFreq);
            logger.info("topic {}: {} = {}", topic, "#d", docFreq);
        }
        logger.info("total topics: " + topics.size());

        collectionsShardStats = new ShardStats(collectionsSizes);
        topicsShardStats = new ShardStats(topicsSizes);

        logger.info("total docs: " + collectionsShardStats.getTotalDocs() + " - " + topicsShardStats.getTotalDocs());
    }
}
