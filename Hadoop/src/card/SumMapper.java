package card;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SumMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	private DoubleWritable outputValue = new DoubleWritable(0); // map 출력값
	private Text outputKey = new Text(); // map 출력키

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		double amount = 0;
		String type = "";
		String[] columns = value.toString().split(",");
		if (Long.parseLong(key.toString()) > 1) {
			amount = Double.parseDouble(columns[29]);
			type = columns[30];
		}
		outputKey.set(type); // 출력키 설정
		if (amount > 0) {
			outputValue = new DoubleWritable(amount);
			context.write(outputKey, outputValue);
		}
	}
}