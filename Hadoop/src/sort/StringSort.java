package sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class StringSort {

	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf, "StringSort");
		job.setJarByClass(StringSort.class);
		// mapper 지정(기본 mapper를 사용, 입력되는 레코드가 그대로 출력 레코드가 됨)
		job.setMapperClass(Mapper.class);
		// reducer 지정(기본 reducer를 사용)
		// 맵에서 출력되는 것이 그대로 리듀스의 출력이 됨, 리듀스단에서 내부적으로 sort가 이루어짐
		job.setReducerClass(Reducer.class);
		
		// 맵 출력과 리듀스 출력의 key, value 타입 설정
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// reduce의 수를 1로 설정
		// (모든 맵의 출력이 하나의 리듀스 태스크로 가게됨, 레코드 개수가 그대로이며 sort만 하게 됨)
		job.setNumReduceTasks(1);
		// 입력 파일의 형식
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		// 출력 파일의 형식
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		// 입력 파일
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// 출력 디렉토리 (사이즈를 줄이기 위해 시퀀스 포맷 사용, 60% 정도 사이즈 감소)
		SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
		// 블록 단위 압축 (레코드 단위로 압축)
		SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
		job.waitForCompletion(true);
	}
}