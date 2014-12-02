package com.splout.db.engine;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splout.db.common.QueryResult;

/**
 * Abstraction that permits serializing classes like {@link ResultAndCursorId}
 * and {@link QueryResult} in a binary format.
 * By default we use Kryo.
 */
public class ResultSerializer {

  public static ThreadLocal<Kryo> localKryo = new ThreadLocal<Kryo>() {

    protected Kryo initialValue() {
      return new Kryo();
    };
  };

  @SuppressWarnings("serial")
  public static class SerializationException extends Exception {

    public SerializationException(String why) {
      super(why);
    }

    public SerializationException(Throwable t) {
      super(t);
    }

    public SerializationException(String why, Throwable t) {
      super(why, t);
    }
  }

  public static ByteBuffer serialize(ResultAndCursorId result) throws SerializationException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    Output output = new Output(stream);
    localKryo.get().writeObject(output, result);
    return ByteBuffer.wrap(output.getBuffer(), 0, output.position());
  }

  public static ResultAndCursorId deserialize(ByteBuffer serialized) throws SerializationException {
    return ResultSerializer.localKryo.get().readObject(new Input(serialized.array(), serialized.position(), serialized.remaining()),
        ResultAndCursorId.class);
  }
}
