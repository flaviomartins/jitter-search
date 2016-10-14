package io.jitter.core.utils;

import java.security.Permission;

/**
 * This is intended as a replacement for the default system manager.
 * The goal is to intercept System.exit calls and make it throw an
 * exception instead so that a System.exit in a task does not
 * fully terminate Ant.
 *
 * @see io.jitter.core.utils.ExitException
 */
public class NoExitSecurityManager extends SecurityManager {

    /**
     * Override SecurityManager#checkExit.
     * This throws an ExitException(status) exception.
     *
     * @param status the exit status
     */
    @Override
    public void checkExit(int status) {
        throw new ExitException(status);
    }

    /**
     * Override SecurityManager#checkPermission.
     * This does nothing.
     *
     * @param perm the requested permission.
     */
    @Override
    public void checkPermission(Permission perm) {
        // no permission here
    }

    /**
     * Override SecurityManager#checkPermission.
     * This does nothing.
     *
     * @param perm    the requested permission.
     * @param context a system-dependent security context.
     */
    @Override
    public void checkPermission(Permission perm, Object context) {
        // no permission here
    }
}
