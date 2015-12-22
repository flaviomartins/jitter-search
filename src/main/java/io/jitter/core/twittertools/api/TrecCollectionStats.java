package io.jitter.core.twittertools.api;

import io.jitter.api.collectionstatistics.CollectionStats;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

public class TrecCollectionStats implements CollectionStats {
    private static final Logger LOG = Logger.getLogger(TrecCollectionStats.class);

    public static final Pattern TAB_PATTERN = Pattern.compile("\\t", Pattern.DOTALL);

    private static final int TERM_COLUMN = 0;
    private static final int DF_COLUMN = 1;
    private static final int CF_COLUMN = 2;

    private static final int DEFAULT_COLLECTION_SIZE = 243271538;

    private Map<String, Integer> documentFrequency;
    private Map<String, Integer> collectionFrequency;

    private int collectionSize = DEFAULT_COLLECTION_SIZE;

    private int cumulativeDocumentFrequency;
    private int cumulativeCollectionFrequency;

    public TrecCollectionStats(String pathToStatsFile) {
        try {

            documentFrequency = new HashMap<>(5861050);
            collectionFrequency = new HashMap<>(5861050);

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
                    continue;
                }

                documentFrequency.put(term, df);
                collectionFrequency.put(term, cf);

                line = reader.readLine();
            }
        } catch (Exception e) {
            LOG.error("died trying to read stats file: " + pathToStatsFile);
            System.exit(-1);
        }
    }

    public int getDF(String term) {
        if (!documentFrequency.containsKey(term.toLowerCase())) {
            return 10;
        }
        return documentFrequency.get(term.toLowerCase());
    }

    public long getCF(String term) {
        if (!collectionFrequency.containsKey(term.toLowerCase())) {
            return 10;
        }
        return collectionFrequency.get(term.toLowerCase());
    }

    public double getIDF(String term) {
        return Math.log(1.0 + (double)getCollectionSize() / (double)getDF(term));
    }

    public int getCollectionSize() {
        return collectionSize;
    }

    public void setCollectionSize(int collectionSize) {
        this.collectionSize = collectionSize;
    }

    public long getCumulativeDocumentFrequency() {
        return cumulativeDocumentFrequency;
    }

    public void setCumulativeDocumentFrequency(int cumulativeDocumentFrequency) {
        this.cumulativeDocumentFrequency = cumulativeDocumentFrequency;
    }

    public long getCumulativeCollectionFrequency() {
        return cumulativeCollectionFrequency;
    }

    public void setCumulativeCollectionFrequency(int cumulativeCollectionFrequency) {
        this.cumulativeCollectionFrequency = cumulativeCollectionFrequency;
    }

}
