package org.weather.district.month.precipitation;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DistrictMonthPrecipitationReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double totalPrecipitation = 0;
        double tempSum = 0;
        int tempCount = 0;

        for (Text v : values) {
            String[] parts = v.toString().split(",");
            double precipitation = Double.parseDouble(parts[0]);
            double tempMean = Double.parseDouble(parts[1]);

            totalPrecipitation += precipitation;
            tempSum += tempMean;
            tempCount++;
        }

        double avgTemp = tempCount > 0 ? tempSum / tempCount : 0;

        context.write(
                key,
                new Text("precipitation_total=" + totalPrecipitation + ", mean_temp=" + avgTemp)
        );
    }
}
