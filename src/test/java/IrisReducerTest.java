import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.util.Arrays;

/**
 * Created by hakeemsm on 3/31/14.
 */
public class IrisReducerTest {
    @Test
    public void returnsNumberOfClassifiedValues(){
        new ReduceDriver<Text,IntWritable,Text,IntWritable>()
                .withReducer(new IrisReducer())
                .withInputKey(new Text("setosa"))
                .withInputValues(Arrays.asList(new IntWritable(1),new IntWritable(1)))
                .withOutput(new Text("setosa"),new IntWritable(2))
                .runTest();
    }
}
