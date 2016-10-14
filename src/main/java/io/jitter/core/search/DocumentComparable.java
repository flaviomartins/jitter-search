package io.jitter.core.search;

import io.jitter.api.search.Document;

import java.util.Objects;

public class DocumentComparable implements Comparable<DocumentComparable> {

    private final Document document;

    public DocumentComparable(Document document) {
        this.document = document;
    }

    public Document getDocument() {
        return document;
    }

    @Override
    public int compareTo(DocumentComparable other) {
        if (document.rsv > other.document.rsv) {
            return -1;
        } else if (document.rsv < other.document.rsv) {
            return 1;
        } else {
            if (document.id > other.document.id) {
                return -1;
            } else if (document.id < other.document.id) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    @Override
    public boolean equals(Object other) {
        return other != null && other.getClass() == this.getClass() && ((DocumentComparable) other).document.id == this.document.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.document.id);
    }
}
