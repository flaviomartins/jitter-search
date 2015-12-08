package io.jitter.core.utils;

/**
 * Used to report exit status of classes which call System.exit().
 *
 * @see io.jitter.core.utils.NoExitSecurityManager
 *
 */
public class ExitException extends SecurityException {

    /** Status code */
    private int status;

    /**
     * Constructs an exit exception.
     * @param status the status code returned via System.exit()
     */
    public ExitException(int status) {
        super("ExitException: status " + status);
        this.status = status;
    }

    /**
     * Constructs an exit exception.
     * @param msg the message to be displayed.
     * @param status the status code returned via System.exit()
     */
    public ExitException(String msg, int status) {
        super(msg);
        this.status = status;
    }

    /**
     * The status code returned by System.exit()
     *
     * @return the status code returned by System.exit()
     */
    public int getStatus() {
        return status;
    }
}
