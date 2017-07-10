package io.jitter.api.wikipedia;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.jitter.api.search.AbstractDocument;
import io.jitter.api.search.Document;
import io.jitter.core.document.DocVector;
import io.jitter.core.wikipedia.WikipediaManager;

@JsonIgnoreProperties(ignoreUnknown = true, value = {"dataPoint", "docVector", "features"})
public class WikipediaDocument extends AbstractDocument implements Document {

    public String id; // required
    public double rsv; // required
    public String title; // required
    public String text; // required
    private DocVector docVector;

    public WikipediaDocument(WikipediaDocument other) {
        id = other.id;
        rsv = other.rsv;
        title = other.title;
        text = other.text;
        docVector = other.docVector;
    }

    public WikipediaDocument(org.apache.lucene.document.Document hit) {
        this.id = hit.get(WikipediaManager.ID_FIELD);
        this.title = hit.get(WikipediaManager.TITLE_FIELD);
        this.text = hit.get(WikipediaManager.TEXT_FIELD);
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }

    @Override
    public double getRsv() {
        return rsv;
    }

    @Override
    public void setRsv(double rsv) {
        this.rsv = rsv;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public DocVector getDocVector() {
        return docVector;
    }

    public void setDocVector(DocVector docVector) {
        this.docVector = docVector;
    }
}
