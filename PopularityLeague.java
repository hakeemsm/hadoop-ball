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

        TreeSet<Pair<Integer,Integer>> tm = new TreeSet<Pair<Integer,Integer>>();
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value: values) {
                sum += value.get();
            }
            tm.add(new Pair<Integer,Integer>(new Integer(sum), new Integer(key.get())));
            
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
        	/*Set set = tm.entrySet();
        	Iterator it = set.iterator();
        	int i = 0;
        	while(it.hasNext()){
        		Map.Entry m = (Map.Entry)it.next();
        		context.write(new IntWritable(i++), new IntWritable((Integer)m.getValue()));
        	}*/
        	int currVal = 0;
        	int i = 0;
        	int x = 0;
        	int prevVal = 0;
        	Integer[] counts = new Integer[tm.size()];

        	for (Pair<Integer,Integer> item: tm) {
        		counts[x++] = item.first;
        	}

        	for (Pair<Integer,Integer> item: tm) {
        		currVal = item.first;
        		if (i == 0 || (i > 0 && currVal != counts[i-1])) {
        			context.write(new IntWritable(item.second), new IntWritable(i++));	
        		}
        		else{
        			context.write(new IntWritable(item.second),new IntWritable(i));
        			i++;	
        		}
        		
        	}
        }
    }
}

    // TODO
    class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
