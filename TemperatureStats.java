import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TemperatureStats {

    public static class TemperatureMapper extends Mapper<Object, Text, Text, FloatWritable> {
        private Text place = new Text();
        private FloatWritable temperature = new FloatWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            if (tokens.length == 4) { // Ensure that the line is properly formatted
                String placeName = tokens[0].trim(); // Extracting the place name
                float temp = Float.parseFloat(tokens[2].trim()); // Extracting the temperature
                place.set(placeName);
                temperature.set(temp);
                context.write(place, temperature);
            }
        }
    }

    public static class TemperatureReducer extends Reducer<Text, FloatWritable, Text, Text> {

        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;
            float minTemp = Float.MAX_VALUE;
            float maxTemp = Float.MIN_VALUE;

            // Calculate sum, count, min, and max temperature for each location
            for (FloatWritable val : values) {
                float temp = val.get();
                sum += temp;
                count++;
                minTemp = Math.min(minTemp, temp);
                maxTemp = Math.max(maxTemp, temp);
            }

            // Calculate average temperature
            float averageTemp = sum / count;

            // Emit location and stats
            context.write(key, new Text("Min: " + minTemp + ", Max: " + maxTemp + ", Average: " + averageTemp));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "temperature stats");
        job.setJarByClass(TemperatureStats.class);
        job.setMapperClass(TemperatureMapper.class);
        job.setReducerClass(TemperatureReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}