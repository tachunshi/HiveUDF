package UDFHive.com.tend;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.apache.tools.ant.Evaluable;

@Description()
public class DateTransformUDF extends UDF{
	private final SimpleDateFormat inputformate = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",Locale.ENGLISH);
	private final SimpleDateFormat outputformate = new SimpleDateFormat("yyyyMMddHHmmss");
	public Text evaluate(Text input){
		Text output = new Text();
		if(null==input){
			return new Text();
		}
		String inputDate = input.toString().trim();
		if(null==inputDate){
			return new Text();
		}
		try {
			Date parseDate=inputformate.parse(inputDate);
			String outputDate = outputformate.format(parseDate);
			output.set(outputDate);
			
		} catch (Exception e) {
			e.printStackTrace();
			return output;
		}
		
		return output;
	}

	public static void main(String[] args) {
		System.out.println(new DateTransformUDF().evaluate(new Text("31/Aug/2015:00:04:37 +0800")));

	}

}
