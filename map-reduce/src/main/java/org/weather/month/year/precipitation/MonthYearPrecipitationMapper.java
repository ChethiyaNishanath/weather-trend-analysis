package org.weather.month.year.precipitation;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MonthYearPrecipitationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        if (key.get() == 0 && value.toString().contains("location_id")) return;

        String[] f = value.toString().split(",");
        if (f.length < 14) return;

        String date = f[1];
        String precipitation = f[13];

        String[] dateParts = date.split("/");
        if (dateParts.length < 3) return;

        String month = dateParts[1];
        String year = dateParts[2];

        int intYear = Integer.parseInt(year);

        if ( intYear < 2014 || intYear > 2024) return;

        Text outKey = new Text(year + "-" + month);
        DoubleWritable outVal = new DoubleWritable(Double.parseDouble(precipitation));

        context.write(outKey, outVal);
    }
}
