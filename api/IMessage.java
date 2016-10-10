package in.dream_lab.goffish;

import org.apache.hadoop.io.Writable;

public interface IMessage <K extends Writable> {
  enum MessageType {
    VERTEX,
    CUSTOM_MESSAGE,
    MESSAGE_LIST
  }
  
  MessageType getMessageType();        // Enum: vertex, custom-message, messagelist.
  
  K getSubgraphID();
  
  Writable getMessage();
}
