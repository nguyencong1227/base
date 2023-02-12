import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingCount{
    public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        @override
        public void map(LongWritable key, javax.xml.soap.Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] rating = line.split("\t");

            if (rating.length >= 4) {
                // rating is: ;[userId, movieId, rating, timestamp]
                int rate = Integer.parseInt(rating[2]);

                // Convert to Hadoop types
                IntWritable mapKey = new IntWritable(rate);
                IntWritable mapValue = new IntWritable(1);

                //OUtput intermadiate key,value pair
                context.write(mapKey, mapValue);
            }
        }
    }


    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {

            int count = 0;
            String title ="";

            // Add up all the ratings
            for(IntWritable value: values) {
                // Separate the value from the tag
                count += value.get();
            }
            IntWritable value = new IntWritable(count);

            // OUtput movieId, title and avg rating
            context.write(key, value);
        }

        public static void main(String[] args) throws Exception {
            // Check that the inpur and output path have been provided
            if(args.length != 2){
                System.err.println("Syntax: MovieRating <in or output>");
                System.exit(-1);
            }

            //Create an instance of the Mapreduce job
            Job job = new Job();
            job.setJobName("Rating count");

            // Set input and output lacations
            FileInputFormat.addInputPath(job, new Path(arg[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            //set mapper and reducer classes
            job.setMapperClass(MyMapper.class);
            job.setOutputValueClass(IntWritable.class);

            // Run the job and then exit the program
            System.exit(job.waitForComputation(true) ? 0 :1);
        }
}