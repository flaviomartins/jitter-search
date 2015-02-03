package org.novasearch.jitter.rs;

import org.novasearch.jitter.rs.methods.*;

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
        } else if ("CRCSISR".equalsIgnoreCase(method)) {
            return new CRCSISR();
        } else if ("CRCSLOGISR".equalsIgnoreCase(method)) {
            return new CRCSLOGISR();
        }
        // default
        return new Votes();
    }

}
