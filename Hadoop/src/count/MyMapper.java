package count;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	// 숫자 1을 직접 사용할 수 없음
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	// 라인번호, 텍스트 형식의 내용을 읽어서 단어, 카운트 형식으로 변환
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		StringTokenizer st = new StringTokenizer(value.toString());
		while (st.hasMoreTokens()) {
			word.set(st.nextToken());
			context.write(word, one);
		}
	}
}