package io.jitter.core.wikipedia;

import io.jitter.core.shards.ShardStats;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class WikipediaShardStatsBuilder {

    private static final Logger logger = LoggerFactory.getLogger(WikipediaShardStatsBuilder.class);

    private final IndexReader reader;
    private final PetscanCsvCategoryMapper categoryMapper;

    private ShardStats collectionsShardStats;
    private ShardStats topicsShardStats;

    public WikipediaShardStatsBuilder(IndexReader reader, PetscanCsvCategoryMapper categoryMapper) throws IOException {
        this.reader = reader;
        this.categoryMapper = categoryMapper;
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

    public void collectStats() throws IOException {
        Map<String, Integer> collectionsSizes = new HashMap<>();
        Map<String, Integer> topicsSizes = new HashMap<>();
        Map<String, IntSet> topicPostings = new HashMap<>();

        Terms terms = MultiFields.getTerms(reader, WikipediaSelectionManager.CATEGORIES_FIELD);
        TermsEnum termEnum = terms.iterator();

        int colCnt = 0;
        BytesRef bytesRef;
        while ((bytesRef = termEnum.next()) != null) {
            String term = bytesRef.utf8ToString();
            int docFreq = termEnum.docFreq();
            if (term.isEmpty()) {
                continue;
            }

            String collection = term.trim();

            collectionsSizes.put(collection, docFreq);
            logger.debug("collection {}: {} = {}", collection, "#d", docFreq);

            Set<String> catTopics = categoryMapper.getMap().get(collection);
            if (catTopics != null ) {
                PostingsEnum postingsEnum = termEnum.postings(null, PostingsEnum.NONE);
                int docId;
                while ((docId = postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    for (String catTopic : catTopics) {
                        if (topicPostings.get(catTopic) == null) {
                            topicPostings.put(catTopic, new IntOpenHashSet());
                        }
                        topicPostings.get(catTopic).add(docId);
                    }
                }
            }

            colCnt++;
        }
        logger.info("total collections: " + colCnt);

        for (String topic : topicPostings.keySet()) {
            topicsSizes.put(topic, topicPostings.get(topic).size());
            logger.info("topic {}: {} = {}", topic, "#d", topicsSizes.get(topic));
        }
        logger.info("total topics: " + topicsSizes.size());

        collectionsShardStats = new ShardStats(collectionsSizes, reader.numDocs());
        topicsShardStats = new ShardStats(topicsSizes, reader.numDocs());

        logger.info("total docs: " + collectionsShardStats.getTotalDocs() + " - " + topicsShardStats.getTotalDocs());
    }
}
