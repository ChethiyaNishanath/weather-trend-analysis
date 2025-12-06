package org.weather.month.year.precipitation;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MonthYearPrecipitationReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        double sum = 0;
        for (DoubleWritable v : values) {
            sum += v.get();
        }

        context.write(key, new DoubleWritable(sum));
    }
}
