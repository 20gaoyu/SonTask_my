package com.ctg.odp.zookeeper;

/**
 * Interface to support the aborting of a given server or client.
 * <p>
 * This is used primarily for ZooKeeper usage when we could get an unexpected
 * and fatal exception, requiring an abort.
 * <p>
 * Implemented by the Master, RegionServer, and TableServers (client).
 */

public interface Abortable {
  /**
   * Abort the server or client.
   * @param why Why we're aborting.
   * @param e Throwable that caused abort. Can be null.
   */
  public void abort(String why, Throwable e);
  
  /**
   * Check if the server or client was aborted. 
   * @return true if the server or client was aborted, false otherwise
   */
  public boolean isAborted();
}