package org.novasearch.jitter.core.selection.taily;

import cc.twittertools.index.IndexStatuses;
import org.apache.log4j.Logger;
import org.apache.lucene.index.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.File;
import java.io.IOException;

public class Taily {
    private static final Logger logger = Logger.getLogger(Taily.class);

    private String indexPath;

    public Taily(String indexPath) {
        this.indexPath = indexPath;
    }

    public void buildCorpus() throws IOException {
        //TODO: read db path
        String dbPath = "bdb";

        // TODO: read terms list

        FeatureStore store = new FeatureStore(dbPath, false);

        DirectoryReader indexReader = DirectoryReader.open(FSDirectory.open(new File(indexPath)));

        // go through all indexes and collect ctf and df statistics.
        long totalTermCount = 0;
        long totalDocCount = 0;

        logger.info("Starting index");

        // TODO: field list?
        Terms terms = MultiFields.getTerms(indexReader, IndexStatuses.StatusField.TEXT.name);
        TermsEnum termEnum = terms.iterator(null);

        int termCount = 0;
        BytesRef bytesRef;
        while ((bytesRef = termEnum.next()) != null) {
            collectCorpusStats(termEnum, store);
            termCount++;
        }

        // add the total term length of shard
        totalTermCount += termCount;
        // add the shard size (# of docs)
        totalDocCount += indexReader.numDocs();

        // add collection global features needed for shard ranking
        store.putFeature(FeatureStore.TERM_SIZE_FEAT_SUFFIX, totalTermCount, FeatureStore.FREQUENT_TERMS+1);
        logger.debug(FeatureStore.TERM_SIZE_FEAT_SUFFIX + " " + totalTermCount);
        store.putFeature(FeatureStore.SIZE_FEAT_SUFFIX, totalDocCount, FeatureStore.FREQUENT_TERMS + 1);
        logger.debug(FeatureStore.SIZE_FEAT_SUFFIX + " " + totalDocCount);

        store.close();
    }

    // innards of buildcorpus
    private void collectCorpusStats(TermsEnum termEnum, FeatureStore store) throws IOException {
        double ctf = termEnum.totalTermFreq();
        double df = termEnum.docFreq();

        // store df feature for term
        String dfFeatKey = termEnum.term().utf8ToString() + FeatureStore.SIZE_FEAT_SUFFIX;
        store.addValFeature(dfFeatKey, df, (int) ctf);
        logger.debug(dfFeatKey + " " + df);

        // store ctf feature for term
        String ctfFeatKey = termEnum.term().utf8ToString() + FeatureStore.TERM_SIZE_FEAT_SUFFIX;
        store.addValFeature(ctfFeatKey, ctf, (int) ctf);
        logger.debug(ctfFeatKey + " " + ctf);
    }

    public static void main(String[] args) throws IOException {
        Taily taily = new Taily("/home/fmartins/Data/cmu/twitter/collectionindex");
        taily.buildCorpus();

        String dbPath = "bdb";
        // TODO: read terms list
        FeatureStore store = new FeatureStore(dbPath, false);
        System.out.println(store.getFeature("#t"));
    }

}
