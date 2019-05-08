package MapReduceHomework;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class ScoreCalculateMapReduce {
	// Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> 代表了 IN 的 Key value 的类型 和 OUT 的 Key value 的类型
	// 因为进入的是文件, 所以设置 KEYIN 是文件 VALUEIN 则是文件内的 字符所以是 TEXT 输出的话 KEYOUT 是课程名称 所以是 TEXT， VALUEOUT则也是分数 即数字
	public static class ScoreMapper extends Mapper<Object, Text, Text, IntWritable>{
		// 课程名
		private Text className = new Text();
		// 对应分数
		private IntWritable score = new IntWritable();
		// 每行数据
		private Text line = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			line = value;
			String[] splitList = line.toString().split(",");
			className.set(splitList[0]);
			score.set(Integer.parseInt(splitList[2]));
//			System.out.println(className+"  "+score);
			context.write(className, score);
		}
	}
	
	// 分数计算 Reducer 类
	public static class CalculateReducer extends Reducer<Text, IntWritable, Text, Text>{
		private Text result = new Text();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			int num = 0;
			int max = 0;
			int min = 1000;
			for (IntWritable val : values){
				int v = val.get();
				sum += v;
				num += 1;
				max = Math.max(max, v);
				min = Math.min(min, v);
			}
			result.set(" max="+max+" min="+min+" avg="+sum/num);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception{
		// hadoop 的configuration
		Configuration configuration = new Configuration();
		// 创建 MapReduce Job
		Job job = Job.getInstance(configuration, "Score Calculate");
		// 设置jar包针对的Class
		job.setJarByClass(ScoreCalculateMapReduce.class);
		// 设置Mapper Class
		job.setMapperClass(ScoreMapper.class);
		// 设置Combiner Class
//		job.setCombinerClass(CalculateReducer.class);
		// 设置Reducer Class
		job.setReducerClass(CalculateReducer.class);
		// 设置全局的OutputKey Class 为 hadoop 的 Text类
		job.setOutputKeyClass(Text.class);
		// 设置全局的OutputValue Class 为 Text
		job.setOutputValueClass(Text.class);
		// 设置 Map 的OutputKey Class
		job.setMapOutputValueClass(IntWritable.class);
		// 添加输入路径和输出路径
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// 完成的时候 exit 代码是 0 否则是 1
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
