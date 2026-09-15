import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EcommerceCategoryMR {

    // =========================
    // MAPPER
    // =========================
    public static class CategoryMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text category = new Text();
        private Text values = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();

            // Skip CSV header
            if (line.startsWith("order_id,")) {
                return;
            }

            String[] fields = line.split(",", -1);

            // CSV should contain 12 columns
            if (fields.length != 12) {
                return;
            }

            try {

                String cat = fields[7].trim();
                double quantity = Double.parseDouble(fields[8].trim());
                double revenue = Double.parseDouble(fields[10].trim());

                category.set(cat);

                // Store quantity and revenue
                values.set(quantity + "," + revenue);

                context.write(category, values);

            } catch (Exception e) {
                // Ignore invalid rows
            }
        }
    }


    // =========================
    // REDUCER
    // =========================
    public static class CategoryReducer
            extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double totalQuantity = 0.0;
            double totalRevenue = 0.0;

            for (Text value : values) {

                String[] parts = value.toString().split(",");

                if (parts.length == 2) {

                    totalQuantity += Double.parseDouble(parts[0]);
                    totalRevenue += Double.parseDouble(parts[1]);
                }
            }

            result.set(
                    "Total Quantity = " + totalQuantity +
                    ", Total Revenue = " +
                    String.format("%.2f", totalRevenue)
            );

            context.write(key, result);
        }
    }


    // =========================
    // DRIVER
    // =========================
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
                conf,
                "E-commerce Category Sales Analysis"
        );

        job.setJarByClass(EcommerceCategoryMR.class);

        job.setMapperClass(CategoryMapper.class);
        job.setReducerClass(CategoryReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(
                job,
                new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
                job,
                new Path(args[1])
        );

        System.exit(
                job.waitForCompletion(true) ? 0 : 1
        );
    }
}