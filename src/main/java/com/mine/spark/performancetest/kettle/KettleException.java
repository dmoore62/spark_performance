package com.mine.spark.performancetest.kettle;

public class KettleException extends Exception {
  private static final long serialVersionUID = -2260895195255402040L;

  /**
   * Constructs a new throwable with null as its detail message.
   */
  public KettleException() {
    super();
  }

  /**
   * Constructs a new throwable with the specified detail message.
   *
   * @param message
   *          - the detail message. The detail message is saved for later retrieval by the getMessage() method.
   */
  public KettleException( String message ) {
    super( message );
  }

  /**
   * Constructs a new throwable with the specified cause and a detail message of (cause==null ? null : cause.toString())
   * (which typically contains the class and detail message of cause).
   *
   * @param cause
   *          the cause (which is saved for later retrieval by the getCause() method). (A null value is permitted, and
   *          indicates that the cause is nonexistent or unknown.)
   */
  public KettleException( Throwable cause ) {
    super( cause );
  }

  /**
   * Constructs a new throwable with the specified detail message and cause.
   *
   * @param message
   *          the detail message (which is saved for later retrieval by the getMessage() method).
   * @param cause
   *          the cause (which is saved for later retrieval by the getCause() method). (A null value is permitted, and
   *          indicates that the cause is nonexistent or unknown.)
   */
  public KettleException( String message, Throwable cause ) {
    super( message, cause );
  }

  /**
   * get the messages back to it's origin cause.
   */
  @Override
  public String getMessage() {
    return super.getMessage();
  }

  public String getSuperMessage() {
    return super.getMessage();
  }

}
