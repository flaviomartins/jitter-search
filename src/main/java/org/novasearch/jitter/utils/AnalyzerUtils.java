package org.novasearch.jitter.utils;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;

public class AnalyzerUtils {

    public static List<String> analyze(Analyzer analyzer, String text) {
        List<String> result = new LinkedList<String>();
        try {
            TokenStream stream;
            stream = analyzer.tokenStream("text", new StringReader(text));

            CharTermAttribute charTermAttribute = stream.addAttribute(CharTermAttribute.class);
            stream.reset();
            while (stream.incrementToken()) {
                String term = charTermAttribute.toString();
                result.add(term);
            }
        } catch (IOException e) {
            // do nothing
        }
        return result;
    }

}
