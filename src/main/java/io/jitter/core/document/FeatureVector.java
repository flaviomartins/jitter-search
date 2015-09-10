package io.jitter.core.document;


import io.jitter.core.analysis.StopperTweetAnalyzer;
import io.jitter.core.utils.*;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;

import java.text.DecimalFormat;
import java.util.*;


/**
 * Simple container mapping term->count pairs grabbed from an input text.
 *
 * @author Miles Efron
 */
@SuppressWarnings("deprecation")
public class FeatureVector {
    private static final int MIN_TERM_LENGTH = 2;
    private static Analyzer analyzer;
    private Map<String, Double> features;
    private final Stopper stopper;
    private double length = 0.0;


    // CONSTRUCTORS
    public FeatureVector(String text, Stopper stopper) {
        this.stopper = stopper;
        if (stopper == null || stopper.asSet().size() == 0) {
            analyzer = new StopperTweetAnalyzer(Version.LUCENE_43, CharArraySet.EMPTY_SET, false, false, true);
        } else {
            CharArraySet charArraySet = new CharArraySet(Version.LUCENE_43, stopper.asSet(), true);
            analyzer = new StopperTweetAnalyzer(Version.LUCENE_43, charArraySet, false, false, true);
        }
        features = new HashMap<>();

        // Remove entities and microsyntax, unescape html
        String cleanText = TweetUtils.clean(text);

        List<String> terms = AnalyzerUtils.analyze(analyzer, cleanText);
        for (String term : terms) {
            if (term.length() < MIN_TERM_LENGTH)
                continue;
            length += 1.0;
            Double val = features.get(term);
            if (val == null) {
                features.put(term, 1.0);
            } else {
                double v = val + 1.0;
                features.put(term, v);
            }
        }
    }

    public FeatureVector(Stopper stopper) {
        this.stopper = stopper;
        if (stopper == null || stopper.asSet().size() == 0) {
            analyzer = new StopperTweetAnalyzer(Version.LUCENE_43, CharArraySet.EMPTY_SET, false, false, true);
        } else {
            CharArraySet charArraySet = new CharArraySet(Version.LUCENE_43, stopper.asSet(), true);
            analyzer = new StopperTweetAnalyzer(Version.LUCENE_43, charArraySet, false, false, true);
        }
        features = new HashMap<>();
    }


    // MUTATORS

    /**
     * Add all the terms in a string to this vector
     *
     * @param text a space-delimited string where we want to add each word.
     */
    public void addText(String text) {
        List<String> terms = AnalyzerUtils.analyze(analyzer, text);
        for (String term : terms) {
            addTerm(term);
        }
    }

    /**
     * Add a term to this vector.  if it's already here, increment its count.
     */
    public void addTerm(String term) {
        if (stopper != null && stopper.isStopWord(term))
            return;

        Double freq = features.get(term);
        if (freq == null) {
            features.put(term, 1.0);
        } else {
            double f = freq;
            features.put(term, f + 1.0);
        }
        length += 1.0;
    }


    /**
     * Add a term to this vector with this weight.  if it's already here, supplement its weight.
     */
    public void addTerm(String term, double weight) {
        Double w = features.get(term);
        if (w == null) {
            features.put(term, weight);
        } else {
            double f = w;
            features.put(term, f + weight);
        }
        length += weight;
    }

    /**
     * in case we want to override the derived length.
     */
    public void setLength(double length) {
        this.length = length;
    }

    public void pruneToSize(int k) {
        List<KeyValuePair> kvpList = getOrderedFeatures();

        Iterator<KeyValuePair> it = kvpList.iterator();

        Map<String, Double> newMap = new HashMap<>(k);
        int i = 0;
        while (it.hasNext()) {
            KeyValuePair kvp = it.next();
            newMap.put(kvp.getKey(), kvp.getScore());
            if (i++ > k)
                break;
        }

        features = newMap;

    }

    public void normalizeToOne() {
        Map<String, Double> f = new HashMap<>(features.size());

        for (String feature : features.keySet()) {
            double obs = features.get(feature);
            f.put(feature, obs / length);
        }

        features = f;
    }


    // ACCESSORS

    public Set<String> getFeatures() {
        return features.keySet();
    }

    public double getLength() {
        return length;
    }

    public int getDimensions() {
        return features.size();
    }

    public double getFeatureWeight(String feature) {
        Double w = features.get(feature);
        return (w == null) ? 0.0 : w;
    }

    public Iterator<String> iterator() {
        return features.keySet().iterator();
    }

    public boolean containsKey(String key) {
        return features.containsKey(key);
    }

    public double getVectorNorm() {
        double norm = 0.0;
        for (String s : features.keySet()) {
            norm += Math.pow(features.get(s), 2.0);
        }
        return Math.sqrt(norm);
    }


    // VIEWING

    @Override
    public String toString() {
        return this.toString(features.size());
    }

    private List<KeyValuePair> getOrderedFeatures() {
        List<KeyValuePair> kvpList = new ArrayList<>(features.size());
        for (String feature : features.keySet()) {
            double value = features.get(feature);
            KeyValuePair keyValuePair = new KeyValuePair(feature, value);
            kvpList.add(keyValuePair);
        }
        ScorableComparator comparator = new ScorableComparator(true);
        Collections.sort(kvpList, comparator);

        return kvpList;
    }

    public String toString(int k) {
        DecimalFormat format = new DecimalFormat("#.#########");
        StringBuilder b = new StringBuilder();
        List<KeyValuePair> kvpList = getOrderedFeatures();
        Iterator<KeyValuePair> it = kvpList.iterator();
        int i = 0;
        while (it.hasNext() && i++ < k) {
            KeyValuePair pair = it.next();
            b.append(format.format(pair.getScore())).append(" ").append(pair.getKey()).append("\n");
        }
        return b.toString();

    }


    // UTILS
    public static FeatureVector interpolate(FeatureVector x, FeatureVector y, double xWeight) {
        FeatureVector z = new FeatureVector(null);
        Set<String> vocab = new HashSet<>();
        vocab.addAll(x.getFeatures());
        vocab.addAll(y.getFeatures());
        for (String feature : vocab) {
            double weight = xWeight * x.getFeatureWeight(feature) + (1.0 - xWeight) * y.getFeatureWeight(feature);
            z.addTerm(feature, weight);
        }
        return z;
    }


    public static void main(String[] args) {
        String text = "This. This is NOT a test, nor is it better than 666!";

        Stopper stopper = new Stopper();
        stopper.addStopword("this");
        stopper.addStopword("is");
        stopper.addStopword("better");

        List<String> terms = AnalyzerUtils.analyze(analyzer, text);
        for (String term : terms) {
            System.out.println(term);
        }
    }


}
