package io.jitter.core.search;

import io.jitter.api.search.Document;

import javax.validation.constraints.NotNull;

public class DocumentComparable implements Comparable<DocumentComparable> {
    @NotNull
    private final Document document;

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
        return other != null && other.getClass() == this.getClass() && ((DocumentComparable) other).document.id == this.document.id;
    }
}
