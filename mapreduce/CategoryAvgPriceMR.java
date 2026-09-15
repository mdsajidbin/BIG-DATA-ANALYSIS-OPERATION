import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CategoryAvgPriceMR {

    // Mapper Class: Protiti row theke category ebong unit_price nibe
    public static class AvgMapper extends Mapper<Object, Text, Text, Text> {
        private Text category = new Text();
        private Text priceAndCount = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            // Header row skip korar jonno
            if (line.startsWith("order_id,")) return; 
            
            String[] fields = line.split(",", -1);
            if (fields.length != 12) return;

            try {
                String cat = fields[7].trim(); // Index 7 holo Category
                double price = Double.parseDouble(fields[9].trim()); // Index 9 holo unit_price

                category.set(cat);
                // Reducer-e price ebong count (1) eksathe pathano hocche
                priceAndCount.set(price + ",1");
                context.write(category, priceAndCount);
            } catch (Exception e) {}
        }
    }

    // Reducer Class: Sob price jog korbe ebong count diye vag kore Average ber korbe
    public static class AvgReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double totalPrice = 0.0;
            int totalCount = 0;

            for (Text value : values) {
                String[] parts = value.toString().split(",");
                if (parts.length == 2) {
                    totalPrice += Double.parseDouble(parts[0]);
                    totalCount += Integer.parseInt(parts[1]);
                }
            }
            
            // Average hishab kora hocche
            double avgPrice = totalPrice / totalCount;
            result.set("Average Unit Price = BDT " + String.format("%.2f", avgPrice));
            context.write(key, result);
        }
    }

    // Driver Class
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Category Average Price Analysis");
        job.setJarByClass(CategoryAvgPriceMR.class);
        job.setMapperClass(AvgMapper.class);
        job.setReducerClass(AvgReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}