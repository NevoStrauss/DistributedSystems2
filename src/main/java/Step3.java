
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;

public class Step3 {

  private static class Map extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] line = value.toString().split("\t");
      if(line.length >= 2){
        String[] words = line[0].split(" ");
        if(words.length >= 3){
          String w1 = words[0];
          String w2 = words[1];
          String w3 = words[2];
          String newKeyStr = String.format("%s\t%s\t%s", w1,w2,w3);
          Text newKey = new Text();
          Text newValue = new Text();
          newKey.set(String.format("%s",newKeyStr));
          int occurrences = Integer.parseInt(line[2]);
          newValue.set(String.format("%d", occurrences));
          context.write(newKey, newValue);
        }
      }
    }
  }

  private static class Reduce extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      int occurrences = 0;
      for (Text value : values) {
        occurrences += Long.parseLong(value.toString());
      }
      Text newKey = new Text();
      Text newValue = new Text();
      newKey.set(String.format("%s", key.toString()));
      newValue.set(String.format("%d", occurrences));
      context.write(newKey, newValue);
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);
    job.setJarByClass(Step3.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setCombinerClass(Reduce.class);
    job.setOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    SequenceFileInputFormat.addInputPath(job, new Path("s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"));
    FileOutputFormat.setOutputPath(job, new Path("/output3/"));         //output
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
