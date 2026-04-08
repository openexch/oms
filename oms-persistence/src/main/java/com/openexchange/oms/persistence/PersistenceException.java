package com.openexchange.oms.persistence;

/**
 * Unchecked exception wrapping SQL and persistence failures.
 * Allows callers to handle persistence errors without checked exception pollution.
 */
public class PersistenceException extends RuntimeException {

    public PersistenceException(String message, Throwable cause) {
        super(message, cause);
    }

    public PersistenceException(String message) {
        super(message);
    }
}
