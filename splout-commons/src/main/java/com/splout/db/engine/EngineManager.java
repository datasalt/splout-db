package com.splout.db.engine;

import com.splout.db.common.QueryResult;
import org.apache.commons.configuration.Configuration;

import java.io.File;
import java.util.List;

/*
 * #%L
 * Splout SQL commons
 * %%
 * Copyright (C) 2012 Datasalt Systems S.L.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

/**
 * Contract for implementing several engine interfaces.
 */
public interface EngineManager {

  @SuppressWarnings("serial")
  public static class EngineException extends Exception {

    public EngineException(String message) {
      super(message);
    }

    public EngineException(Throwable underlying) {
      super(underlying);
    }

    public EngineException(String message, Throwable underlying) {
      super(message, underlying);
    }
  }

  /**
   * Exception to be thrown in cases where the exception is unexpected,
   * so they are high likelihood that it would work in another replica.
   * For example, this exception should be throw in cases like underlying
   * database corruption, non existing partition file, etc.
   */
  public static class ShouldRetryInReplicaException extends EngineException {
    public ShouldRetryInReplicaException(String message) {
      super(message);
    }

    public ShouldRetryInReplicaException(Throwable underlying) {
      super(underlying);
    }

    public ShouldRetryInReplicaException(String message, Throwable underlying) {
      super(message, underlying);
    }
  }

  /**
   * Exception to be thrown in cases where the exception is expected, and
   * we know any other replica would thrown the same exception. For example,
   * it should be thrown in cases like syntax error or timeout exceptions.
   */
  public static class ShouldNotRetryInReplicaException extends EngineException {
    public ShouldNotRetryInReplicaException(String message) {
      super(message);
    }

    public ShouldNotRetryInReplicaException(Throwable underlying) {
      super(underlying);
    }

    public ShouldNotRetryInReplicaException(String message, Throwable underlying) {
      super(message, underlying);
    }
  }

  public static class SyntaxErrorException extends ShouldNotRetryInReplicaException {
    public SyntaxErrorException(String message) {
      super(message);
    }

    public SyntaxErrorException(Throwable underlying) {
      super(underlying);
    }

    public SyntaxErrorException(String message, Throwable underlying) {
      super(message, underlying);
    }
  }

  /**
   * To be thrown when query has been externally interrupted. It usually happens
   * when a query timeouts.
   */
  public static class QueryInterruptedException extends ShouldNotRetryInReplicaException {
    public QueryInterruptedException(String message) {
      super(message);
    }

    public QueryInterruptedException(Throwable underlying) {
      super(underlying);
    }

    public QueryInterruptedException(String message, Throwable underlying) {
      super(message, underlying);
    }
  }

  /**
   * To be thrown when query query results size is greater than limits.
   */
  public static class TooManyResultsException extends ShouldNotRetryInReplicaException {

    public TooManyResultsException(String message) {
      super(message);
    }

    public TooManyResultsException(Throwable underlying) {
      super(underlying);
    }

    public TooManyResultsException(String message, Throwable underlying) {
      super(message, underlying);
    }
  }

  public void init(File dbFile, Configuration config, List<String> initStatements) throws EngineException;

  /**
   * Executes a SQL command.
   *
   * Be carefully to properly throw a {@link com.splout.db.engine.EngineManager.ShouldRetryInReplicaException}
   * or {@link com.splout.db.engine.EngineManager.ShouldNotRetryInReplicaException}
   */
  public QueryResult exec(String query) throws EngineException;

  /**
   * Given a query returns an object of type {@link com.splout.db.common.QueryResult}, up to
   * maxResults. Usually the engine should throw an exception if maxResults is
   * reached.
   *
   * Be carefully to properly throw a {@link com.splout.db.engine.EngineManager.ShouldRetryInReplicaException}
   * or {@link com.splout.db.engine.EngineManager.ShouldNotRetryInReplicaException}
   */
  public QueryResult query(String query, int maxResults) throws EngineException;

  /**
   * For supporting reading big datasets from the engine through a streaming API
   *
   * Be carefully to properly throw a {@link com.splout.db.engine.EngineManager.ShouldRetryInReplicaException}
   * or {@link com.splout.db.engine.EngineManager.ShouldNotRetryInReplicaException}
   */
  public void streamQuery(StreamingIterator visitor) throws EngineException;
  
  public void close();
}
