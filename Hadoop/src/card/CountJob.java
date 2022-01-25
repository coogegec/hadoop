package card;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CountJob {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 2) { // 입출력 데이터 경로 확인
			System.err.println("Usage : CountJob <input> <output>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "CountJob"); // Job 이름 설정
		FileInputFormat.addInputPath(job, new Path(args[0])); // 입력 파일 경로 지정
		FileOutputFormat.setOutputPath(job, new Path(args[1])); // 출력 파일 경로 지정

		job.setJarByClass(CountJob.class); // jar 파일 내용 중 실행할 클래스 이름 지정
		job.setMapperClass(CountMapper.class); // mapper 클래스 지정
		job.setReducerClass(CountReducer.class); // reducer 클래스 지정

		job.setInputFormatClass(TextInputFormat.class); // 입력 자료형 지정
		job.setOutputFormatClass(TextOutputFormat.class); // 출력 자료형 지정

		job.setOutputKeyClass(Text.class); // key의 자료형 지정
		job.setOutputValueClass(IntWritable.class); // value의 자료형 지정
		job.waitForCompletion(true);
	}
}