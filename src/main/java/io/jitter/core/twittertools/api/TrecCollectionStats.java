package io.jitter.core.twittertools.api;

import io.jitter.api.collectionstatistics.CollectionStats;
import io.jitter.core.taily.FeatureStore;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Locale;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

public class TrecCollectionStats implements CollectionStats {
    private static final Logger LOG = Logger.getLogger(TrecCollectionStats.class);

    public static final Pattern TAB_PATTERN = Pattern.compile("\\t", Pattern.DOTALL);
    public static final String CORPUS_DBENV = "corpus";

    private static final int TERM_COLUMN = 0;
    private static final int DF_COLUMN = 1;
    private static final int CF_COLUMN = 2;

    public static final int DEFAULT_COLLECTION_SIZE = 243271538;
    public static final int DEFAULT_TERM_SIZE = 5861050;

    private FeatureStore corpusStore;

    private int collectionSize = DEFAULT_COLLECTION_SIZE;

    private int cumulativeDocumentFrequency;
    private int cumulativeCollectionFrequency;

    public TrecCollectionStats(String pathToStatsFile, String statsDb) {
        File statsStorePath = new File(statsDb + "/" + CORPUS_DBENV);
        if (!statsStorePath.isDirectory()) {
            corpusStore = new FeatureStore(statsDb + "/" + CORPUS_DBENV, false);
            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(new File(pathToStatsFile)))));
                String line = reader.readLine();
                for (int i = 0; line != null; i++) {
                    String[] toks = TAB_PATTERN.split(line);
                    if (toks == null || toks.length != 3) {
                        LOG.error("bad stats line");
                        continue;
                    }

                    String term = toks[TERM_COLUMN];
                    int df = Integer.parseInt(toks[DF_COLUMN]);
                    int cf = Integer.parseInt(toks[CF_COLUMN]);

                    // if reading first line
                    if (i == 0) {
                        cumulativeDocumentFrequency = df;
                        cumulativeCollectionFrequency = cf;
                        
                        String dfFeatKey = FeatureStore.SIZE_FEAT_SUFFIX;
                        corpusStore.putFeature(dfFeatKey, df, cf);

                        String ctfFeatKey = FeatureStore.TERM_SIZE_FEAT_SUFFIX;
                        corpusStore.putFeature(ctfFeatKey, df, cf);

                        line = reader.readLine();
                        continue;
                    }

                    // get and store shard df feature for term
                    String dfFeatKey = term.toLowerCase(Locale.ROOT) + FeatureStore.SIZE_FEAT_SUFFIX;
                    corpusStore.putFeature(dfFeatKey, df, cf);

                    // store ctf feature for term
                    String ctfFeatKey = term.toLowerCase(Locale.ROOT) + FeatureStore.TERM_SIZE_FEAT_SUFFIX;
                    corpusStore.putFeature(ctfFeatKey, cf, cf);

                    line = reader.readLine();
                }
            } catch (Exception e) {
                LOG.error("died trying to read stats file: " + pathToStatsFile);
                System.exit(-1);
            }
            corpusStore.close();
        }

        corpusStore = new FeatureStore(statsDb + "/" + CORPUS_DBENV, true);

        String dfFeatKey = FeatureStore.SIZE_FEAT_SUFFIX;
        cumulativeDocumentFrequency = (int) corpusStore.getFeature(dfFeatKey);

        String ctfFeatKey = FeatureStore.TERM_SIZE_FEAT_SUFFIX;
        cumulativeCollectionFrequency = (int) corpusStore.getFeature(ctfFeatKey);
    }

    public int docFreq(String term) {
        String dfFeatKey = term.toLowerCase(Locale.ROOT) + FeatureStore.SIZE_FEAT_SUFFIX;
        int df = (int) corpusStore.getFeature(dfFeatKey);
        if (df == -1) {
            return 9;
        }
        return df;
    }

    public long totalTermFreq(String term) {
        String ctfFeatKey = term.toLowerCase(Locale.ROOT) + FeatureStore.TERM_SIZE_FEAT_SUFFIX;
        int cf = (int) corpusStore.getFeature(ctfFeatKey);
        if (cf == -1) {
            return 9;
        }
        return cf;
    }

    public double idf(String term) {
        return Math.log(1.0 + (double) numDocs() / (double) docFreq(term));
    }

    public int numDocs() {
        return collectionSize;
    }

    public void setCollectionSize(int collectionSize) {
        this.collectionSize = collectionSize;
    }

    public long getSumDocFreq() {
        return cumulativeDocumentFrequency;
    }

    public void setCumulativeDocumentFrequency(int cumulativeDocumentFrequency) {
        this.cumulativeDocumentFrequency = cumulativeDocumentFrequency;
    }

    public long getSumTotalTermFreq() {
        return cumulativeCollectionFrequency;
    }

    public void setCumulativeCollectionFrequency(int cumulativeCollectionFrequency) {
        this.cumulativeCollectionFrequency = cumulativeCollectionFrequency;
    }

    public int numTerms() {
        return DEFAULT_TERM_SIZE;
    }

}
