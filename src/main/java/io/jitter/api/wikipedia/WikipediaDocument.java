package io.jitter.api.wikipedia;

import io.jitter.core.document.DocVector;
import io.jitter.core.wikipedia.WikipediaManager;

//@JsonIgnoreProperties(ignoreUnknown = true, value = {"docVector"})
public class WikipediaDocument {

    public String id; // required
    public double rsv; // required
    public String title; // required
    public String text; // required
    private DocVector docVector;

    public WikipediaDocument(WikipediaDocument other) {
        id = other.id;
        rsv = other.rsv;
        title = other.title;
        docVector = other.docVector;
    }

    public WikipediaDocument(org.apache.lucene.document.Document hit) {
        this.id = hit.get(WikipediaManager.ID_FIELD);
        this.title = hit.get(WikipediaManager.TITLE_FIELD);
        this.text = hit.get(WikipediaManager.TEXT_FIELD);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public double getRsv() {
        return rsv;
    }

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
