import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CitySalesMR {

    public static class CityMapper extends Mapper<Object, Text, Text, Text> {
        private Text city = new Text();
        private Text values = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.startsWith("order_id,")) return; 
            
            String[] fields = line.split(",", -1);
            if (fields.length != 12) return;

            try {
                String cty = fields[4].trim(); // Index 4 is City
                double quantity = Double.parseDouble(fields[8].trim());
                double revenue = Double.parseDouble(fields[10].trim());

                city.set(cty);
                values.set(quantity + "," + revenue);
                context.write(city, values);
            } catch (Exception e) {}
        }
    }

    public static class CityReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double totalQuantity = 0.0;
            double totalRevenue = 0.0;

            for (Text value : values) {
                String[] parts = value.toString().split(",");
                if (parts.length == 2) {
                    totalQuantity += Double.parseDouble(parts[0]);
                    totalRevenue += Double.parseDouble(parts[1]);
                }
            }
            result.set("Total Quantity = " + totalQuantity + ", Total Revenue = " + String.format("%.2f", totalRevenue));
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "City Sales Analysis");
        job.setJarByClass(CitySalesMR.class);
        job.setMapperClass(CityMapper.class);
        job.setReducerClass(CityReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}