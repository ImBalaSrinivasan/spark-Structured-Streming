package aggregatedcounter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import org.apache.spark.sql.api.java.UDF2;

public class CalculatevalueAggregated implements UDF2<String, String, String> {


	@Override
	public String call(String value, String formula) throws Exception {
		
		StringBuilder sb = new StringBuilder();
		for (String word : formula.replace("\"", "").split(" ")) {
			if (Pattern.matches("\\w+", word)) {
				Matcher m1 = Pattern.compile("(#\\s)(" + word + "\\^)(\\w+)(\\s#)").matcher(value);
				if(m1.find()) {
					sb.append(m1.group(3));
				}else {
					sb.append("0");
				}
			} else {
				sb.append(word);
			}
		}

		ScriptEngineManager mgr = new ScriptEngineManager();
		ScriptEngine engine = mgr.getEngineByName("JavaScript");
		String final_value = engine.eval(sb.toString()).toString();
		return final_value;

	}
}
