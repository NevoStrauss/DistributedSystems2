import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
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
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class Step5 {

  private static class Map extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      context.write(new Text(key.toString()), value);
    }
  }

  public static class Reduce extends Reducer<Text, Text, Text, Text> {
    public static Long C0 = 0L;
    public static HashMap<String, Double> output1Map = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      String[] words = key.toString().split("\t");
      double N1 = output1Map.get(words[2]);
      double N2 = 0;
      double N3 = 0;
      double C1 = output1Map.get(words[1]);
      double C2 = 0;
      for (Text value : values) {
        String[] strings = value.toString().split("\t");
        if (strings.length == 1){ // value from output 3
          N3 = Double.parseDouble(strings[0]);
        }
        else{
           if (strings[0].equals(words[0])){
             C2 = Double.parseDouble(strings[3]);
           }
          else{
            N2 = Double.parseDouble(strings[3]);
           }
        }
      }
      double K2 = (Math.log(N2+1) + 1) / (Math.log(N2+1) + 2);
      double K3 = (Math.log(N3+1) + 1) / (Math.log(N3+1) + 2);
      double newValue = K3 * (N3/C2) + (1 - K3) * K2 * (N2/C1) + (1-K3) * (1-K2) * (N1/C0);
      context.write(key, new Text(Double.toString(newValue)));
    }


    @Override
    public void setup(Reducer.Context context) throws IOException {
      FileSystem fileSystem = FileSystem.get(context.getConfiguration());
      RemoteIterator<LocatedFileStatus> it = fileSystem.listFiles(new Path("/output1"), false);
      while (it.hasNext()) {
        LocatedFileStatus fileStatus = it.next();
        if (fileStatus.getPath().getName().startsWith("part")) {
          FSDataInputStream InputStream = fileSystem.open(fileStatus.getPath());
          BufferedReader reader = new BufferedReader(new InputStreamReader(InputStream, StandardCharsets.UTF_8));
          String line;
          String[] ones;
          while ((line = reader.readLine()) != null) {
            ones = line.split("\t");
            if (ones[0].equals("*")) {
              C0 = Long.parseLong(ones[1]);
            } else {
              output1Map.put(ones[0], (double) Long.parseLong(ones[1]));
            }
          }
          reader.close();
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
    job.setJarByClass(Step5.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setPartitionerClass(Step5.PartitionerClass.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    String input1 = args[1];
    String input2 = args[2];
    String output = args[3];
    MultipleInputs.addInputPath(job, new Path(input1), TextInputFormat.class);
    MultipleInputs.addInputPath(job, new Path(input2), TextInputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(output));
    job.waitForCompletion(true);
  }

}
