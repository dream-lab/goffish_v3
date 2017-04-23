/**


 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package in.dream_lab.goffish.hama;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import in.dream_lab.goffish.hama.api.IControlMessage;

class ControlMessage implements IControlMessage {

  // Maybe use flags to support multiple transmission types? eg:broadcast and
  // partition msg
  private IControlMessage.TransmissionType transmissionType;
  private Text vertexValues = new Text("");
  private List<BytesWritable> extraInfo;
  // Id of -1 signifies not set
  private int sourcePartitionID;

  public ControlMessage() {
    transmissionType = IControlMessage.TransmissionType.NORMAL;
    extraInfo = Lists.newArrayList();
    sourcePartitionID = -1;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeEnum(out, transmissionType);
    out.writeInt(sourcePartitionID);
    out.writeInt(extraInfo.size());
    for (BytesWritable info : extraInfo) {
      info.write(out);
    }

    if (isPartitionMessage()) {
      // out.writeInt(sourcePartitionID);
    } else if (isVertexMessage()) {
      vertexValues.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    transmissionType = WritableUtils.readEnum(in, IControlMessage.TransmissionType.class);
    sourcePartitionID = in.readInt();
    extraInfo = Lists.newArrayList();
    int extraInfoSize;
    extraInfoSize = in.readInt();
    while (extraInfoSize-- > 0) {
      BytesWritable info = new BytesWritable();
      info.readFields(in);
      extraInfo.add(info);
    }
    if (isPartitionMessage()) {
      // sourcePartitionID = in.readInt();
    } else if (isVertexMessage()) {
      vertexValues.readFields(in);
    }
  }

  @Override
  public TransmissionType getTransmissionType() {
    return transmissionType;
  }

  public void setTransmissionType(
      IControlMessage.TransmissionType transmissionType) {
    this.transmissionType = transmissionType;
  }

  public void setPartitionID(int partitionID) {
    this.sourcePartitionID = partitionID;
  }
  
  public int getPartitionID() {
    return this.sourcePartitionID;
  }

  @Deprecated
  // remove this and just use extrainfo
  public void setVertexValues(String vertex) {
    this.vertexValues = new Text(vertex);
  }

  @Deprecated
  public String getVertexValues() {
    return vertexValues.toString();
  }

  public void addextraInfo(byte b[]) {
    BytesWritable info = new BytesWritable(b);
    this.extraInfo.add(info);
  }

  public Iterable<BytesWritable> getExtraInfo() {
    return extraInfo;
  }

  public boolean isNormalMessage() {
    return transmissionType == IControlMessage.TransmissionType.NORMAL;
  }

  public boolean isPartitionMessage() {
    return transmissionType == IControlMessage.TransmissionType.PARTITION;
  }

  public boolean isVertexMessage() {
    return transmissionType == IControlMessage.TransmissionType.VERTEX;
  }

  public boolean isBroadcastMessage() {
    return transmissionType == IControlMessage.TransmissionType.BROADCAST;
  }

  // to be removed after clearing dependencies from LongTextjsonReader
  @Override
  public String toString() {
    if (isPartitionMessage()) {
      return String.valueOf(sourcePartitionID);
    } else if (isVertexMessage()) {
      return vertexValues.toString();
    } else {
      return null;
    }
  }

}
