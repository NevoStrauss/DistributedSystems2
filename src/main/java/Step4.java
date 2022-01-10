import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;
import org.apache.hadoop.fs.Path;

public class Step4 {

  private static class Map extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] words = key.toString().split("\t");
      IntWritable occurrences = new IntWritable(Integer.parseInt(value.toString()));
      String w1 = words[0];
      String w2 = words[1];
      if (words.length > 2) { //output of step 3
        String w3 = words[2];
        context.write(new Text(w1 + "\t" + w2), new Text(w1 + "\t" + w2 + "\t" + w3 + "\t" + occurrences));
        context.write(new Text(w2 + "\t" + w3), new Text(w1 + "\t" + w2 + "\t" + w3 + "\t" + occurrences));
      } else { //output of step 2
        context.write(new Text(w1 + "\t" + w2), new Text(occurrences.toString()));
      }
    }
  }

  public static class Reduce extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      String occurrencesOfW_1W_2 = "";
      for (Text value : values) {
        String[] curr = value.toString().split("\t");
        if (curr.length == 1) {
          occurrencesOfW_1W_2 = curr[0];
          break;
        }
      }
      for (Text value : values) {
        String[] curr = value.toString().split("\t");
        if (curr.length > 1) {
          Text newKey = new Text(curr[0] + "\t" + curr[1] + "\t" + curr[2]);
          Text newValue = new Text(key + "\t" + occurrencesOfW_1W_2);
          context.write(newKey, newValue);
          break;
        }
      }
    }
  }

  private static class PartitionerClass extends Partitioner<Text, Text> {
    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
      return Math.abs(key.hashCode()) % numPartitions;
    }

  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, args[0]);
    job.setJarByClass(Step4.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setPartitionerClass(Step4.PartitionerClass.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    String input1 = args[1];
    String input2 = args[2];
    String output4 = args[3];
    MultipleInputs.addInputPath(job, new Path(input1), TextInputFormat.class);
    MultipleInputs.addInputPath(job, new Path(input2), TextInputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(output4));
    job.waitForCompletion(true);
  }
}

