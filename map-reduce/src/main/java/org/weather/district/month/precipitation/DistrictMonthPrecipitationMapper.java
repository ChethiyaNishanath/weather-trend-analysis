package org.weather.district.month.precipitation;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DistrictMonthPrecipitationMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        if (key.get() == 0 && value.toString().contains("location_id")) return;

        String[] fields = value.toString().split(",");
        if (fields.length < 14) return; // sanity check

        String locationId = fields[0].trim();

        try {
            int locIdInt = Integer.parseInt(locationId);
            if (locIdInt < 0 || locIdInt > 25) return;
        } catch (NumberFormatException e) {
            return;
        }

        String date = fields[1].trim();
        String tempMean = fields[5].trim();
        String precipitationHours = fields[13].trim();

        String[] dateParts = date.split("/");
        if (dateParts.length < 3) return;
        String month = dateParts[1];

        int year = Integer.parseInt(dateParts[2]);
        if (year < 2014 || year > 2024) return;

        Text outKey = new Text(locationId + "|" + month);
        Text outValue = new Text(precipitationHours + "," + tempMean);

        context.write(outKey, outValue);
    }
}