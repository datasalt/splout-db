package com.splout.db.common;

public class CommonProperties {

  /**
   * If enabled, instead of throwing an exception when maxResults is reached, a cursorId is returned
   * to continue iterating.
   */
  public final static String ENABLE_CURSORS = "common.cursors.enable";
  /**
   * TTL for server-side cursors to expire, in seconds.
   */
  public final static String CURSORS_TIMEOUT = "common.cursors.timeout";
}
