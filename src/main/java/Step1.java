
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Step1 {
  private static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * The Input:
     * Google 1gram database
     * n-gram /T year /T occurrences /T pages /T books
     * program   1991    3   2   1
     * program   1986    4   2   1
     * <p>
     * The Output:
     * For each line from the input it creates a line with the word and its occurrences.
     * <p>
     * T n-gram /T occurrences
     * program 		3
     * *		        4
     */

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] line = value.toString().split("\t");
      IntWritable occurrences = new IntWritable(Integer.parseInt(line[2]));
      Text word = new Text(line[0]);
      context.write(word, occurrences);
    }
  }

  /**
   * Input:
   *      The input is the sorted output of the mapper.
   *      Maybe output from different mappers.
   *      Template:
   *              T n-gram /T occurrences
   *                program  		3
   *                program       4
   *
   * Output:
   *      Combines all the same occurrences by key.
   *              T n-gram   	T occurrences
   *               program       		7
   */
  private static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int C_0 = 0;
      for (IntWritable value: values) {
          C_0 += value.get();
      }
      context.write(key, new IntWritable(C_0));
    }
  }

  private static class PartitionerClass extends Partitioner<Text,IntWritable> {
    @Override
    public int getPartition(Text key, IntWritable value, int numPartitions){
      return Math.abs(key.hashCode()) % numPartitions;
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "Step1");
      job.setJarByClass(Step1.class);
      job.setMapperClass(Map.class);
      job.setPartitionerClass(PartitionerClass.class);
      job.setCombinerClass(Reduce.class);
      job.setReducerClass(Reduce.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(IntWritable.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      FileInputFormat.addInputPath(job, new Path(args[0]));           //input
      FileOutputFormat.setOutputPath(job, new Path(args[1]));         //output
      System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

