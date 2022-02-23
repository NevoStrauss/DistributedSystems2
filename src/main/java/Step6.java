import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;

public class Step6 {
  private static class Map extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] strings = value.toString().split("\t");
      if (strings.length >= 2) {
        Text newKey = new Text();
        Text newValue = new Text();
        String newKeyStr = String.format("%s %s", strings[0], strings[1]);
        newKey.set(String.format("%s", newKeyStr));
        newValue.set(String.format("%s", ""));
        context.write(newKey, newValue);
      }
    }

  }

  private static class Reduce extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      String newKeyStr = key.toString();
      Text newKey = new Text();
      Text newValue = new Text();
      newKey.set(String.format("%s", newKeyStr));
      newValue.set(String.format("%s", ""));
      context.write(newKey, newValue);
    }

  }

  private static class CompareClass extends WritableComparator {
    protected CompareClass() {
      super(Text.class, true);
    }
    @Override
    public int compare(WritableComparable key1, WritableComparable key2) {
      String[] strings1 = key1.toString().split(" ");
      String[] strings2 = key2.toString().split(" ");
      if(strings1.length >= 4 && strings2.length >= 4){
        if (strings1[0].equals(strings2[0]) && strings1[1].equals(strings2[1])) {
          if (Double.parseDouble(strings1[3]) > (Double.parseDouble(strings2[3]))) {
            return -1;
          } else
            return 1;
        }
        return (strings1[0] + " " + strings1[1]).compareTo(strings2[0] + " " + strings2[1]);

      }
      return 0;
    }
  }


  public static void main(String[] args) throws Exception {
    String output = "s3://dsp-ass2output/output";
    String step5output = "/output5/";
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);
    job.setJarByClass(Step6.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setSortComparatorClass(CompareClass.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    SequenceFileInputFormat.addInputPath(job, new Path(step5output));
    FileOutputFormat.setOutputPath(job, new Path(output));
    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}