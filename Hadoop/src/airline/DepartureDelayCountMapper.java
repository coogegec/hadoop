package airline;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DepartureDelayCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable outputValue = new IntWritable(1); // map 출력값
	private Text outputKey = new Text(); // map 출력키

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);

		outputKey.set(parser.getYear() + "," + parser.getMonth()); // 출력키 설정
		if (parser.getDepartureDelayTime() > 0) {
			context.write(outputKey, outputValue); // 출력 데이터 생성
		}
	}
}