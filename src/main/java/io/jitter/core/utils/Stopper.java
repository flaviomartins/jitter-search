package io.jitter.core.utils;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class Stopper {
    private static final Logger LOG = Logger.getLogger(Stopper.class);

    public static final Pattern SPACE_PATTERN = Pattern.compile(" ", Pattern.DOTALL);
    private Set<String> stopwords;


    public Stopper() {
        stopwords = new HashSet<>();
    }

    public Stopper(String pathToStoplist) {
        try {
            stopwords = new HashSet<>();

            // assume our stoplist has one stopword per line
            List<String> lines = FileUtils.readLines(new File(pathToStoplist), "UTF-8");
            stopwords.addAll(lines);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    public String apply(String text) {
        StringBuilder b = new StringBuilder();
        String[] tokens = SPACE_PATTERN.split(text);
        for (String tok : tokens) {
            if (!isStopWord(tok))
                b.append(tok).append(" ");
        }
        return b.toString().trim();
    }

    public void addStopword(String term) {
        stopwords.add(term);
    }

    public boolean isStopWord(String term) {
        return (stopwords.contains(term));
    }

    public Set<String> asSet() {
        return stopwords;
    }
}
