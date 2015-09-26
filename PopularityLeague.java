import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class PopularityLeague extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO
        Job job = Job.getInstance(this.getConf(), "Popularity League");
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(LinkCountMap.class);
        job.setReducerClass(LinkCountReduce.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setJarByClass(PopularityLeague.class);
        return job.waitForCompletion(true) ? 0 : 1;       
    }

    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }

     public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        // TODO
     	List<String> leagueLinks;

     	@Override
        protected void setup(Context context) throws IOException,InterruptedException {

            Configuration conf = context.getConfiguration();
            String leaguePath = conf.get("league");            

            this.leagueLinks = Arrays.asList(readHDFSFile(leaguePath, conf).split("\n"));
            
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String entry = value.toString();
            String[] pair = entry.split(": ",2);
            String[] srcLinks = pair[1].split(" ");
            
            for (String str: srcLinks) {
                if(leagueLinks.contains(str)){
                	context.write(new IntWritable(Integer.parseInt(str)), new IntWritable(1));
                }
            }
        }
    }

    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        // TODO

        TreeMap<Integer,Integer> tm = new TreeMap<Integer,Integer>();
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value: values) {
                sum += value.get();
            }
            tm.put(new Integer(sum), new Integer(key.get()));
            
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
        	Set set = tm.entrySet();
        	Iterator it = set.iterator();
        	int i = 0;
        	while(it.hasNext()){
        		Map.Entry m = (Map.Entry)it.next();
        		context.write(new IntWritable(i++), new IntWritable((Integer)m.getValue()));
        	}
        }
    }

    // TODO
}