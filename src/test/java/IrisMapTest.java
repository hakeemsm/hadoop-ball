import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

/**
 * Created by hakeemsm on 3/31/14.
 */
public class IrisMapTest {
    @Test
    public void outputsValidIrisClassValues(){
        Text value1 = new Text("4.6,3.2,1.4,0.2");
        Text value2 = new Text("5.3,3.7,1.5,0.2");

        new MapDriver<LongWritable,Text,Text,IntWritable>()
                .withMapper(new IrisMapper())
                .withInput(new LongWritable(1), new Text(value1))
                .withOutput(new Text("setosa"), new IntWritable(1))
                .runTest();
        new MapDriver<LongWritable,Text,Text,IntWritable>().
                withMapper(new IrisMapper())
                .withInput(new LongWritable(35), new Text(value2))
                .withOutput(new Text("setosa"), new IntWritable(1))
                .runTest();
    }
}
