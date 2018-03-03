import java.text.SimpleDateFormat;
import java.util.Date;

import com.hof.util.DateFields;

public class Test {
	
	  public static void main(String[] args) {
		   
		  Date parsedDate = new Date();
		  String d = "Thu, 16 Mar 2017 05:13:24 +0000";
		  SimpleDateFormat dateFormat = new SimpleDateFormat("E, dd MMM yyyy hh:mm:ss Z");
		  try {
			parsedDate = dateFormat.parse(d);
		} catch (java.text.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		  System.out.println(new SimpleDateFormat("MM-dd-yyyy").format(parsedDate));
		  System.out.println(new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss").format(parsedDate));
		  System.out.println(parsedDate);
		  
		  DateFields df = new DateFields(new java.sql.Timestamp(parsedDate.getTime()));
		  System.out.println(df.getTimestamp());
}
}
