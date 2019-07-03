package others;

import java.util.regex.*;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;  

public class Sample {

	public static void main(String[] args) {
		String value = "# one^1 # two^2 # three^3 # four^4 #";
		String formula = "one + ( two * three ) + four";
		StringBuilder sb = new StringBuilder();  

		for(String word : formula.split(" ")){
			if(Pattern.matches("[a-zA-Z]+", word)) {
				Pattern p = Pattern.compile("(#\\s)("+ word	+"\\^)(\\w+)(\\s#)");
				Matcher m1 = p.matcher(value);
				while (m1.find()) { 
					sb.append(m1.group(3));
		        }
			}else {
				sb.append(word);
			}
            
		}
		
		System.out.println(sb.toString());
		ScriptEngineManager mgr = new ScriptEngineManager();
		ScriptEngine engine = mgr.getEngineByName("JavaScript");
		try {	
			System.out.println(engine.eval("1+(5*3)+4"));
		} catch (ScriptException e) {
			e.printStackTrace();
		}
		
	}

}
