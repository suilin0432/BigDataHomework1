package MapReduceHomework;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class SameCountMapReduce {
	public static class Pair implements WritableComparable<Pair>{
		public Text className = new Text();
		public IntWritable score = new IntWritable();
		public Pair(){
			this.className = new Text();
			this.score = new IntWritable();
		}
		public Pair(Text className, IntWritable score){
			this.className = className;
			this.score = score;
		}
		public Text getClassName(){
			return this.className;
		}
		public IntWritable getScore(){
			return this.score;
		}
		public void setClassName(Text text){
			this.className = text;
		}
		public void setScore(IntWritable intWritable) {
			this.score = intWritable;
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			this.className.readFields(in);;
			this.score.readFields(in);;
		}
		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			this.className.write(out);;
			this.score.write(out);;
		}
		@Override
		public int compareTo(Pair o) {
			// TODO Auto-generated method stub
//			return this.hashCode() - o.hashCode();
			int a = this.className.compareTo(o.className);
			if (a == 0){
				return this.score.get() - o.score.get();
			}
			else{
				return a;
			}
		}
		public int hashCode(){
			return this.className.toString().hashCode() + this.score.get();
		}
		public String toString(){
			return this.className+"  "+this.score;
		}
		public boolean equals(Object obj){
			if(!(obj instanceof Pair)){
				return false;
			}else{
				Pair rPair = (Pair) obj;
				if (rPair.className == this.className && rPair.score == this.score){
					return true;
				}else{
					return false;
				}
			}
		}
	}
	// Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> 代表了 IN 的 Key value 的类型 和 OUT 的 Key value 的类型
	// 因为进入的是文件, 所以设置 KEYIN 是文件 VALUEIN 则是文件内的 字符所以是 TEXT 输出的话 KEYOUT 是课程名称 所以是 TEXT， VALUEOUT则也是分数 即数字
	public static class ScoreMapper extends Mapper<Object, Text, Pair, Text>{
		// 课程名
		private Text className = new Text();
		// 人名
		private Text studentName = new Text();
		// 对应分数
		private IntWritable score = new IntWritable();
		// 每行数据
		private Text line = new Text();
		// 输出键值对
		private Pair pair;
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			line = value;
			String[] splitList = line.toString().split(",");
			className.set(splitList[0]);
			studentName.set(splitList[1]);
			score.set(Integer.parseInt(splitList[2]));
			pair = new Pair(className, score);
//			System.out.println(pair+"  "+studentName);
			context.write(pair, studentName);
		}
	}
	// 分数计算 Reducer 类
	public static class CountReducer extends Reducer<Pair, Text, Text, Text>{
		private Text result = new Text();
//		private String className;
//		private int score;
		public void reduce(Pair key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
//			this.className = key.className;
//			this.score = key.score;
			int sum = 0;
			String string = "  "+sum+"  ";
			for (Text val : values){
				sum += 1;
				string = string + val + "  ";
			}
			result.set(string);
//			System.out.println("Reduce "+key.toString()+" "+result);
			context.write(new Text(key.toString()), result);
		}
	}
	
	public static void main(String[] args) throws Exception{
		// hadoop 的configuration
		Configuration configuration = new Configuration();
		System.out.println(0);
		// 创建 MapReduce Job
		Job job = Job.getInstance(configuration, "Same Count");
		// 设置jar包针对的Class
		job.setJarByClass(SameCountMapReduce.class);
		// 设置Mapper Class
		job.setMapperClass(ScoreMapper.class);
		// 设置Combiner Class
//		job.setCombinerClass(CalculateReducer.class);
		// 设置Reducer Class
		job.setReducerClass(CountReducer.class);
		// 设置全局的OutputKey Class 为 hadoop 的 Text类
		job.setOutputKeyClass(Text.class);
		// 设置全局的OutputValue Class 为 Text
		job.setOutputValueClass(Text.class);
		// 设置 Map 的OutputKey Class
		job.setMapOutputKeyClass(Pair.class);
		// 添加输入路径和输出路径
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// 完成的时候 exit 代码是 0 否则是 1
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

// 下面是不自定义类的时候使用的方法

//package MapReduceHomework;
//
//
//import java.io.IOException;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//public class SameCountMapReduce {
//	// Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> 代表了 IN 的 Key value 的类型 和 OUT 的 Key value 的类型
//	// 因为进入的是文件, 所以设置 KEYIN 是文件 VALUEIN 则是文件内的 字符所以是 TEXT 输出的话 KEYOUT 是课程名称 所以是 TEXT， VALUEOUT则也是分数 即数字
//	public static class ScoreMapper extends Mapper<Object, Text, Text, Text>{
//		// 课程名
//		private Text className = new Text();
//		// 人名
//		private Text studentName = new Text();
//		// 对应分数
//		private IntWritable score = new IntWritable();
//		// 每行数据
//		private Text line = new Text();
//		private Text result = new Text();
//		// 输出键值对		
//		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
//			System.out.println(8);
//			line = value;
//			System.out.println("###########"+line);
//			String[] splitList = line.toString().split(",");
//			className.set(splitList[0]);
//			studentName.set(splitList[1]);
//			score.set(Integer.parseInt(splitList[2]));
//			result.set(splitList[0]+"  "+splitList[2]);
//			System.out.println(result+"  "+studentName);
//			context.write(result, studentName);
//		}
//	}
//	// 分数计算 Reducer 类
//	public static class CountReducer extends Reducer<Text, Text, Text, Text>{
//		private Text result = new Text();
////		private String className;
////		private int score;
//		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
////			this.className = key.className;
////			this.score = key.score;
//			int sum = 0;
//			String string = "  "+sum+"  ";
//			for (Text val : values){
//				sum += 1;
//				string = string + val + ", ";
//			}
//			result.set(string);
//			System.out.println("Reduce "+key.toString()+" "+result);
//			context.write(new Text(key.toString()), result);
//		}
//	}
//	
//	public static void main(String[] args) throws Exception{
//		// hadoop 的configuration
//		Configuration configuration = new Configuration();
//		System.out.println(0);
//		// 创建 MapReduce Job
//		Job job = Job.getInstance(configuration, "Same Count");
//		// 设置jar包针对的Class
//		job.setJarByClass(SameCountMapReduce.class);
//		// 设置Mapper Class
//		job.setMapperClass(ScoreMapper.class);
//		// 设置Combiner Class
////		job.setCombinerClass(CalculateReducer.class);
//		// 设置Reducer Class
//		job.setReducerClass(CountReducer.class);
//		// 设置全局的OutputKey Class 为 hadoop 的 Text类
//		job.setOutputKeyClass(Text.class);
//		// 设置全局的OutputValue Class 为 Text
//		job.setOutputValueClass(Text.class);
//		// 设置 Map 的OutputKey Class
////		job.setMapOutputKeyClass(Object.class);
//		// 添加输入路径和输出路径
//		FileInputFormat.addInputPath(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
//		// 完成的时候 exit 代码是 0 否则是 1
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
//	}
//}
