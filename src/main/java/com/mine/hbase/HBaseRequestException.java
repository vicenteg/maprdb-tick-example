package com.mine.hbase;

/**
 *
 * @author jscott
 */
public class HBaseRequestException extends Exception {

	/**
	 * Creates a new instance of
	 * <code>HBaseRequestException</code> without detail message.
	 */
	public HBaseRequestException() {
	}

	/**
	 * Constructs an instance of
	 * <code>HBaseRequestException</code> with the specified detail message.
	 *
	 * @param msg the detail message.
	 */
	public HBaseRequestException(String msg) {
		super(msg);
	}

	/**
	 * Constructs a new exception with the specified detail message and cause. <p>Note that the
	 * detail message associated with
	 * {@code cause} is <i>not</i> automatically incorporated in this exception's detail message.
	 *
	 * @param message the detail message (which is saved for later retrieval by the {@link #getMessage()}
	 * method).
	 * @param cause the cause (which is saved for later retrieval by the
	 *         {@link #getCause()} method). (A <tt>null</tt> value is permitted, and indicates that the
	 * cause is nonexistent or unknown.)
	 * @since 1.4
	 */
	public HBaseRequestException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * Constructs a new exception with the specified cause and a detail message of <tt>(cause==null
	 * ? null : cause.toString())</tt> (which typically contains the class and detail message of
	 * <tt>cause</tt>). This constructor is useful for exceptions that are little more than wrappers
	 * for other throwables (for example, {@link
	 * java.security.PrivilegedActionException}).
	 *
	 * @param cause the cause (which is saved for later retrieval by the
	 *         {@link #getCause()} method). (A <tt>null</tt> value is permitted, and indicates that the
	 * cause is nonexistent or unknown.)
	 * @since 1.4
	 */
	public HBaseRequestException(Throwable cause) {
		super(cause);
	}
}
