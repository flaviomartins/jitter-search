package org.novasearch.jitter.rs;

import org.novasearch.jitter.rs.methods.CRCSEXP;
import org.novasearch.jitter.rs.methods.CRCSLIN;
import org.novasearch.jitter.rs.methods.ReDDE;
import org.novasearch.jitter.rs.methods.Votes;

public class ResourceSelectionMethodFactory {

    public static ResourceSelectionMethod getMethod(String method) {
        if ("Votes".equalsIgnoreCase(method)) {
            return new Votes();
        } else if ("ReDDE".equalsIgnoreCase(method)) {
            return new ReDDE();
        } else if ("CRCSLIN".equalsIgnoreCase(method)) {
            return new CRCSLIN();
        } else if ("CRCSEXP".equalsIgnoreCase(method)) {
            return new CRCSEXP();
        }
        return new Votes();
    }

}
