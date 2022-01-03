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
    public void map (LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
      String[] strings = value.toString().split("\t");
      String[] words = strings[0].split(" ");
      IntWritable occurrences = new IntWritable(Integer.parseInt(strings[1]));
      String w1 = words[0];
      String w2 = words[1];
      if(words.length>2){
        String w3 = words[2];
        context.write(new Text(w1+" "+w2), new Text(w1+" "+w2+" "+w3+"\t"+occurrences));
        context.write(new Text(w2+" "+w3), new Text(w1+" "+w2+" "+w3+"\t"+occurrences));
      }
      else{
        context.write(new Text(w1+" "+w2) ,new Text(occurrences.toString()));
      }
    }
  }

  public static class Reduce extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      String newKey;
      String occurrences;
      for (Text value: values) {
        String[] strings = value.toString().split("\t");
        newKey = strings[0];
        if(strings.length>1){
          occurrences = strings[1];
        }
        else {
          newKey
        }
      }

    }
  }

  private static class PartitionerClass extends Partitioner<Text, Text> {
    @Override
    public int getPartition(Text key, Text value, int numPartitions){

      return Math.abs(key.hashCode()) % numPartitions;
    }

  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);
    job.setJarByClass(Step4.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setPartitionerClass(Step4.PartitionerClass.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    String input1="/outputStep2/";
    String input2="/outputStep3/";
    String output4="/outputStep4/";
    MultipleInputs.addInputPath(job, new Path(input1), TextInputFormat.class);
    MultipleInputs.addInputPath(job, new Path(input2), TextInputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(output4));
    job.waitForCompletion(true);
  }
}
