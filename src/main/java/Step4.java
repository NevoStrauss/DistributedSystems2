import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;

public class Step4 {

  private static class Map extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] words = key.toString().split("\t");
      if(words.length >= 2){
        int occurrences = Integer.parseInt(value.toString());
        String w1 = words[0];
        String w2 = words[1];
        Text newKey = new Text();
        Text newValue = new Text();
        newKey.set(String.format("%s", w1 + "\t" + w2));

        if (words.length > 2) { //output of step 3
          String w3 = words[2];
          Text newKey1 = new Text();
          String newKey1Str = String.format("%s\t%s", w2, w3);
          newKey1.set(String.format("%s", newKey1Str));
          String newValueStr = String.format("%s\t%s\t%s\t%d", w1,w2,w3,occurrences);
          newValue.set(String.format("%s", newValueStr));
          context.write(newKey, newValue);
          context.write(newKey1, newValue);
        } else { //output of step 2
          newValue.set(String.format("%d", occurrences));
          context.write(newKey, newValue);
        }
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

  public static void main(String[] args) throws Exception {
    String step2output = "/output2/";
    String step3output = "/output3/";
    String output4 = "/output4/";
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);
    job.setJarByClass(Step4.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    MultipleInputs.addInputPath(job, new Path(step2output), TextInputFormat.class);
    MultipleInputs.addInputPath(job, new Path(step3output), TextInputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(output4));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

