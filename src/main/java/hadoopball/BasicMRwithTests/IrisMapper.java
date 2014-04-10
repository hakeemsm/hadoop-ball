package hadoopball.BasicMRwithTests;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by hakeemsm on 3/31/14.
 */
public class IrisMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] values = line.split(",");
        context.write(new Text(ClassifyType(values)),new IntWritable(1));
    }

    private String ClassifyType(String[] values) {
        float sum = Float.parseFloat(values[0]) + Float.parseFloat(values[1]) + Float.parseFloat(values[2]);
        float testValue = sum/Float.parseFloat(values[3]);

        if (40.00 <= testValue && 60.00 >= testValue){
            return "setosa";
        }
        else if (61.00 <= testValue && 85.00 >= testValue){
            return "versicolor";
        }

        return "virginica";

    }
}
