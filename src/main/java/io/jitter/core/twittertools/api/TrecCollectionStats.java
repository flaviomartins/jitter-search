package io.jitter.core.twittertools.api;

import io.jitter.api.collectionstatistics.CollectionStats;
import io.jitter.core.taily.FeatureStore;
import io.jitter.core.taily.JeFeatureStore;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

public class TrecCollectionStats implements CollectionStats {
    private static final Logger LOG = Logger.getLogger(TrecCollectionStats.class);

    public static final Pattern TAB_PATTERN = Pattern.compile("\\t", Pattern.DOTALL);
    public static final String CORPUS_DBENV = "corpus";

    private static final int TERM_COLUMN = 0;
    private static final int DF_COLUMN = 1;
    private static final int CF_COLUMN = 2;

    public static final int DEFAULT_COLLECTION_SIZE = 242432449;
    public static final int DEFAULT_TERMS_SIZE = 131628185;

    private FeatureStore corpusStore;

    private int numDocs = DEFAULT_COLLECTION_SIZE;

    private long sumDocFreq;
    private long sumTotalTermFreq;

    public TrecCollectionStats(String pathToStatsFile, String statsDb) {
        Path statsStorePath = Paths.get(statsDb, CORPUS_DBENV);
        if (!Files.isDirectory(statsStorePath)) {
            LOG.info("creating stats database...");
            corpusStore = new JeFeatureStore(statsStorePath.toString(), false);
            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(new File(pathToStatsFile))), "UTF-8"));
                String line = reader.readLine();
                for (int i = 0; line != null; i++) {
                    String[] toks = TAB_PATTERN.split(line);
                    if (toks == null || toks.length != 3) {
                        LOG.error("bad stats line");
                        continue;
                    }

                    String term = toks[TERM_COLUMN];
                    long df = Integer.parseInt(toks[DF_COLUMN]);
                    long cf = Long.parseLong(toks[CF_COLUMN]);

                    // if reading first line
                    if (i == 0) {
                        sumDocFreq = df;
                        sumTotalTermFreq = cf;

                        // store totals
                        String dfFeatKey = FeatureStore.SIZE_FEAT_SUFFIX;
                        corpusStore.putFeature(dfFeatKey, sumDocFreq, sumTotalTermFreq);

                        String ctfFeatKey = FeatureStore.TERM_SIZE_FEAT_SUFFIX;
                        corpusStore.putFeature(ctfFeatKey, sumTotalTermFreq, sumTotalTermFreq);

                        line = reader.readLine();
                        continue;
                    }

                    if (!term.isEmpty()) {
                        // get and store shard df feature for term
                        String dfFeatKey = term + FeatureStore.SIZE_FEAT_SUFFIX;
                        corpusStore.putFeature(dfFeatKey, df, cf);

                        // store ctf feature for term
                        String ctfFeatKey = term + FeatureStore.TERM_SIZE_FEAT_SUFFIX;
                        corpusStore.putFeature(ctfFeatKey, cf, cf);

                        line = reader.readLine();
                    }
                }
            } catch (Exception e) {
                LOG.error("died trying to read stats file: " + pathToStatsFile);
                System.exit(-1);
            }
            corpusStore.close();
        }

        corpusStore = new JeFeatureStore(statsStorePath.toString(), true);

        String dfFeatKey = FeatureStore.SIZE_FEAT_SUFFIX;
        sumDocFreq = (long) corpusStore.getFeature(dfFeatKey);

        String ctfFeatKey = FeatureStore.TERM_SIZE_FEAT_SUFFIX;
        sumTotalTermFreq = (long) corpusStore.getFeature(ctfFeatKey);
    }

    @Override
    public int docFreq(String term) {
        String dfFeatKey = term + FeatureStore.SIZE_FEAT_SUFFIX;
        int df = (int) corpusStore.getFeature(dfFeatKey);
        if (df == -1) {
            return 1;
        }
        return df;
    }

    @Override
    public long totalTermFreq(String term) {
        String ctfFeatKey = term + FeatureStore.TERM_SIZE_FEAT_SUFFIX;
        long cf = (long) corpusStore.getFeature(ctfFeatKey);
        if (cf == -1) {
            return 1;
        }
        return cf;
    }

    @Override
    public int numDocs() {
        return numDocs;
    }

    public void setNumDocs(int numDocs) {
        this.numDocs = numDocs;
    }

    @Override
    public long getSumDocFreq() {
        return sumDocFreq;
    }

    public void setSumDocFreq(int sumDocFreq) {
        this.sumDocFreq = sumDocFreq;
    }

    @Override
    public long getSumTotalTermFreq() {
        return sumTotalTermFreq;
    }

    public void setSumTotalTermFreq(int sumTotalTermFreq) {
        this.sumTotalTermFreq = sumTotalTermFreq;
    }

    @Override
    public int numTerms() {
        return DEFAULT_TERMS_SIZE;
    }

}
