package org.novasearch.jitter.core;

import org.novasearch.jitter.api.search.Document;

public class DocumentComparable implements Comparable<DocumentComparable> {
    private Document document;

    public DocumentComparable(Document document) {
        this.document = document;
    }

    public Document getDocument() {
        return document;
    }

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

    public boolean equals(Object other) {
        if (other == null) {
            return false;
        } if (other.getClass() != this.getClass()) {
            return false;
        }

        return ((DocumentComparable) other).document.id == this.document.id;
    }
}
