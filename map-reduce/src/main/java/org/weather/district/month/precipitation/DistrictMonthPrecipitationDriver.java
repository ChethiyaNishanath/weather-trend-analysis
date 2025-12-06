package org.weather.district.month.precipitation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DistrictMonthPrecipitationDriver {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "District-Month Aggregation");

        job.setJarByClass(DistrictMonthPrecipitationDriver.class);
        job.setMapperClass(DistrictMonthPrecipitationMapper.class);
        job.setReducerClass(DistrictMonthPrecipitationReducer.class);

        job.setOutputKeyClass(org.apache.hadoop.io.Text.class);
        job.setOutputValueClass(org.apache.hadoop.io.Text.class);

        job.addCacheFile(new Path("/user/test/locationData.csv").toUri());

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
