
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

public class Step3 {

  private static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] line = value.toString().split("\t");
      String[] words = line[0].split(" ");
      String newKey = words[0] + "\t" + words[1] + "\t" + words[2];
      IntWritable occurrences = new IntWritable(Integer.parseInt(line[2]));
      context.write(new Text(newKey), occurrences);
    }
  }

  private static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int occurrences = 0;
      for (IntWritable value : values) {
        occurrences += value.get();
      }
      context.write(key, new IntWritable(occurrences));
    }
  }

  private static class PartitionerClass extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text key, IntWritable value, int numPartitions) {
      return Math.abs(key.hashCode()) % numPartitions;
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, args[0]);
    job.setJarByClass(Step3.class);
    job.setMapperClass(Step3.Map.class);
    job.setPartitionerClass(Step3.PartitionerClass.class);
    job.setCombinerClass(Step3.Reduce.class);
    job.setReducerClass(Step3.Reduce.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[1]));           //input
    FileOutputFormat.setOutputPath(job, new Path(args[2]));         //output
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
