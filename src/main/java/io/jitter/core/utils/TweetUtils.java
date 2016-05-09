package io.jitter.core.utils;

import com.twitter.Extractor;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.regex.Pattern;

public class TweetUtils {
    private static final String NUMBERS = "(\\d+[:., ]?)+";
    private static final String MICROSYNTAX = "/(by|cc|for|tip|thx|ty|ht|oh)";
    private static final String RT = "^/?RT:?\\s*:?|\\s/?RT:?\\s*:?|\\(RT:?.*\\)";
    private static final String VIA = "^/?via:?\\s*|\\s/?via:?\\s*|\\(via:?.*\\)|\\[via:?.*\\]";

    private static final Pattern NUMBERS_PATTERN;
    private static final Pattern MICROSYNTAX_PATTERN;
    private static final Pattern RT_PATTERN;
    private static final Pattern VIA_PATTERN;

    private static final Extractor EXTRACTOR;

    static {
        synchronized (TweetUtils.class) {
            NUMBERS_PATTERN = Pattern.compile(NUMBERS);
            MICROSYNTAX_PATTERN = Pattern.compile(MICROSYNTAX);
            RT_PATTERN = Pattern.compile(RT);
            VIA_PATTERN = Pattern.compile(VIA);
            EXTRACTOR = new Extractor();
        }
    }

    public static List<String> extractURLs(String text) {
        Extractor extractor = new Extractor();
        return extractor.extractURLs(text);
    }

    public static String stripNumbers(String text) {
        return NUMBERS_PATTERN.matcher(text).replaceAll(" ");
    }

    public static String removeMicrosyntax(String text) {
        return MICROSYNTAX_PATTERN.matcher(text).replaceAll(" ");
    }

    public static String removeRT(String text) {
        return RT_PATTERN.matcher(text).replaceAll(" ");
    }

    public static String removeVia(String text) {
        return VIA_PATTERN.matcher(text).replaceAll(" ");
    }

    public static String removeEntities(String text) {
        List<Extractor.Entity> entities = EXTRACTOR.extractEntitiesWithIndices(text);
        return replaceEntities(text, entities);
    }

    public static String removeURLs(String text) {
        List<Extractor.Entity> entities = EXTRACTOR.extractURLsWithIndices(text);
        return replaceEntities(text, entities);
    }

    public static String removeMentionsOrLists(String text) {
        List<Extractor.Entity> entities = EXTRACTOR.extractMentionsOrListsWithIndices(text);
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
//        text = removeAll(text);
        text = removeMentionsOrLists(text);
        text = removeURLs(text);
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
