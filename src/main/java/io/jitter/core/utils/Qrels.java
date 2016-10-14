package io.jitter.core.utils;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class Qrels {
    private static final Logger LOG = Logger.getLogger(Qrels.class);

    public static final Pattern SPACE_PATTERN = Pattern.compile(" ", Pattern.DOTALL);

    private static final int QUERY_COLUMN = 0;
    private static final int DOCNO_COLUMN = 2;
    private static final int REL_COLUMN = 3;
    private static final int MIN_REL = 0;

    private Map<String, Map<String, Integer>> rel;

    public Qrels(String pathToQrelsFile) {
        try {

            rel = new HashMap<>();

            List<String> lines = FileUtils.readLines(new File(pathToQrelsFile), "UTF-8");
            for (String line : lines) {
                String[] toks = SPACE_PATTERN.split(line);
                if (toks == null || toks.length != 4) {
                    LOG.error("bad qrels line");
                    continue;
                }
                String query = toks[QUERY_COLUMN];
                String docno = toks[DOCNO_COLUMN];
                int r = Integer.parseInt(toks[REL_COLUMN]);
                if (r >= MIN_REL) {
                    Map<String, Integer> relDocs;
                    if (!rel.containsKey(query)) {
                        relDocs = new HashMap<>();
                    } else {
                        relDocs = rel.get(query);
                    }
                    relDocs.put(docno, r);
                    rel.put(query, relDocs);
                }
            }
        } catch (Exception e) {
            LOG.error("died trying to read qrel file: " + pathToQrelsFile);
            System.exit(-1);
        }
    }

    public boolean isRel(String query, String docno) {
        if (!rel.containsKey(query)) {
            LOG.error("no relevant documents found for query " + query);
            return false;
        }
        return rel.get(query).containsKey(docno);
    }

    public int getRel(String query, String docno) {
        if (!rel.containsKey(query)) {
            LOG.error("no relevant documents found for query " + query);
            return 0;
        }
        if (!rel.get(query).containsKey(docno)) {
            return -1;
        }
        return rel.get(query).get(docno);
    }

    public String getRelString(String query, String docno) {
        int rel = getRel(query, docno);
        return rel != -1?String.valueOf(rel):"?";
    }

    public Set<String> getRelDocs(String query) {
        if (!rel.containsKey(query)) {
            LOG.error("no relevant documents found for query " + query);
            return null;
        }
        return rel.get(query).keySet();
    }

    public double numRel(String query) {
        if (!rel.containsKey(query)) {
            LOG.error("no relevant documents found for query " + query);
            return 0.0;
        }
        return (double) rel.get(query).size();
    }
}
