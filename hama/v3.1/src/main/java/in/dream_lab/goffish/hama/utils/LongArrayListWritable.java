/**
 *  Copyright 2017 DREAM:Lab, Indian Institute of Science, Bangalore
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may
 *  not use this file except in compliance with the License. You may obtain
 *  a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
*/

package in.dream_lab.goffish.hama.utils;

import org.apache.hadoop.io.LongWritable;

/**
 * 
 */
public class LongArrayListWritable extends ArrayListWritable<LongWritable> {

  /** Default constructor for reflection */
  public LongArrayListWritable() {
    super();
  }

  /** Set storage type for this ArrayListWritable */
  @Override
  @SuppressWarnings("unchecked")
  public void setClass() {
    setClass(LongWritable.class);
  }
}
