package org.novasearch.jitter.core.utils;

import com.twitter.Extractor;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;

import java.util.List;

public class TweetUtils {
    private static final String NUMER_PATTERN = "(\\d+[:., ]?)+";
    private static final String MICROSYNTAX_PATTERN = "/(by|cc|for|tip|thx|ty|ht|oh)";
    private static final String RT_PATTERN = "^/?RT:?\\s*:?|\\s/?RT:?\\s*:?|\\(RT:?.*\\)";
    private static final String VIA_PATTERN = "^/?via:?\\s*|\\s/?via:?\\s*|\\(via:?.*\\)|\\[via:?.*\\]";

    public static List<String> extractURLs(String text) {
        Extractor extractor = new Extractor();
        return extractor.extractURLs(text);
    }

    public static List<Extractor.Entity> extractURLsWithIndices(String text) {
        Extractor extractor = new Extractor();
        return extractor.extractURLsWithIndices(text);
    }

    public static List<Extractor.Entity> extractEntitiesWithIndices(String text) {
        Extractor extractor = new Extractor();
        return extractor.extractEntitiesWithIndices(text);
    }

    public static String stripNumbers(String text) {
        return text.replaceAll(NUMER_PATTERN, " ");
    }

    public static String removeMicrosyntax(String text) {
        return text.replaceAll(MICROSYNTAX_PATTERN, " ");
    }

    public static String removeRT(String text) {
        return text.replaceAll(RT_PATTERN, " ");
    }

    public static String removeVia(String text) {
        return text.replaceAll(VIA_PATTERN, " ");
    }

    public static String removeEntities(String text) {
        List<Extractor.Entity> entities = extractEntitiesWithIndices(text);
        return replaceEntities(text, entities);
    }

    public static String removeURLs(String text) {
        List<Extractor.Entity> entities = extractURLsWithIndices(text);
        return replaceEntities(text, entities);
    }

    public static String removePatterns(String text) {
        text = removeMicrosyntax(text);
        text = removeRT(text);
        text = removeVia(text);
        return text;
    }

    public static String removeAll(String text) {
        text = removeEntities(text);
        text = removePatterns(text);
        return text;
    }

    public static String clean(String text) {
        text = removeAll(text);
        text = removePatterns(text);
        text = unescapeHtml(text);
        text = stripNumbers(text);
        return text;
    }

    public static String replaceEntities(String text, List<Extractor.Entity> entities) {
        StringBuilder buf = new StringBuilder(text);
        for (Extractor.Entity e : entities) {
            int length = e.getEnd() - e.getStart();
            buf.replace(e.getStart(), e.getEnd(), StringUtils.repeat(" ", length));
        }
        return buf.toString();
    }

    public static String unescapeHtml(String text) {
        return StringEscapeUtils.unescapeHtml(text);
    }
}
