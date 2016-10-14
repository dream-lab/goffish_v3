package in.dream_lab.goffish;

import java.util.Collection;

import org.apache.hadoop.io.Writable;

/* Defines remote vertex interface. 
 *
 */
public interface IRemoteVertex<V extends Writable, E extends Writable, I extends Writable, J extends Writable, K extends Writable>  extends IVertex<V, E, I, J> {
  
}
