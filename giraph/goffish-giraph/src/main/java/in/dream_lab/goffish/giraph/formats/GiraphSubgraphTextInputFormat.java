package in.dream_lab.goffish.giraph.formats;

import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.io.formats.GiraphTextInputFormat;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by anirudh on 02/11/16.
 */
public class GiraphSubgraphTextInputFormat extends GiraphTextInputFormat {

  /** Class logger. */
  private static final Logger LOG =
      Logger.getLogger(GiraphFileInputFormat.class);

  private List<InputSplit> getSplits(JobContext job, List<FileStatus> files)
      throws IOException {

    // generate splits
    List<InputSplit> splits = new ArrayList<InputSplit>();

    for (FileStatus file: files) {
      Path path = file.getPath();
      FileSystem fs = path.getFileSystem(job.getConfiguration());
      long length = file.getLen();
      BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
      splits.add(new FileSplit(path, 0, length, blkLocations[0].getHosts()));
    }
    return splits;
  }

  @Override
  public List<InputSplit> getVertexSplits(JobContext job) throws IOException {
    List<FileStatus> files = listVertexStatus(job);
    List<InputSplit> splits = getSplits(job, files);
    // Save the number of input files in the job-conf
    job.getConfiguration().setLong(NUM_VERTEX_INPUT_FILES, files.size());
    LOG.debug("Total # of vertex splits: " + splits.size());
    return splits;
  }
}
