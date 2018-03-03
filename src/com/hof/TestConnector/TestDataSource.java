package com.hof.TestConnector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Blob;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.HttpClientBuilder;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.hof.TestConnector.TestSourceMetaData;
import com.hof.mi.thirdparty.interfaces.AbstractDataSet;
import com.hof.mi.thirdparty.interfaces.AbstractDataSource;
import com.hof.mi.thirdparty.interfaces.ColumnMetaData;
import com.hof.mi.thirdparty.interfaces.DataType;
import com.hof.mi.thirdparty.interfaces.FilterData;
import com.hof.mi.thirdparty.interfaces.FilterMetaData;
import com.hof.mi.thirdparty.interfaces.ScheduleDefinition;
import com.hof.mi.thirdparty.interfaces.ThirdPartyException;
import com.hof.mi.thirdparty.interfaces.ScheduleDefinition.FrequencyTypeCode;
import com.hof.pool.JDBCMetaData;
import com.hof.util.DateFields;

public class TestDataSource extends AbstractDataSource {

	public BufferedReader rd=null;
	public StringBuffer result=new StringBuffer();
	public int listSizeMessages=0;
	public int listSizeCalls=0;
	public String value="";
	public JSONObject jsonMessages = null;
	public JSONArray arrMessages = null;
	
	public JSONObject jsonCalls = null;
	public JSONArray arrCalls = null;
	public String getDataSourceName() {
		
		return "Twilio";
		
	}
	
	public Date dateParser(String date) throws java.text.ParseException{
			SimpleDateFormat dateFormat = new SimpleDateFormat("E, dd MMM yyyy hh:mm:ss Z");
		    //SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss");
		    Date parsedDate = dateFormat.parse(date);
		    return parsedDate;
		    
		}
	
	public Timestamp dateFormatter(String date) throws java.text.ParseException{
		Date parsedDate= dateParser(date);
		Timestamp timestamp = new java.sql.Timestamp(parsedDate.getTime());
	    return timestamp;
	}
	
	/*public Timestamp dateFormatter(String date) throws java.text.ParseException{
		Calendar cal1 = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		SimpleDateFormat dateFormat = new SimpleDateFormat("E, dd MMM yyyy hh:mm:ss Z");
		SimpleDateFormat dateFormatEnd = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss");

	    Date parsedDate = null;
		try {
			parsedDate = dateFormat.parse(date);
		} catch (java.text.ParseException| NullPointerException e2 ) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
	    try {
			cal1.setTime( parsedDate);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	    String parseString=dateFormatEnd.format(parsedDate);
	    Date parseDate=null;
	    try {
	    	parseDate=dateFormatEnd.parse(parseString);
		} catch (java.text.ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	    cal1.setTime(parseDate);
	    Timestamp timestamp = new java.sql.Timestamp(cal1.getTimeInMillis());
	    return timestamp;
	    
	}*/
	
	public DateFields dateField(Timestamp timestamp){
		DateFields df1 = new DateFields(timestamp);
		return df1;
	}

	public HttpResponse respond() throws ClientProtocolException, IOException{
		String sid=(String)getAttribute("AUTHSID");
		String token=(String)getAttribute("AUTHTOKEN");
		String url="https://"+sid+":"+token+"@api.twilio.com/2010-04-01/Accounts/"+sid+"/Calls.json";
        HttpClient client = HttpClientBuilder.create().build();
		HttpRequest request = new HttpGet(url);
		HttpResponse response = client.execute((HttpUriRequest) request);
		return response;
		}
	
	public void View (String endpoint, String jsonarr, String BLOB){
		String twilio="[";
	    String nextPageUri="/2010-04-01/Accounts/"+(String)getAttribute("AUTHSID")+"/"+endpoint+".json?PageSize=50&Page=0&PageToken=null";
	  
	    HttpResponse response=null;
	
	    JSONParser parser= new JSONParser();
	   try{
	     saveBlob(BLOB, twilio.getBytes());
	   }catch(NullPointerException e){
		   e.printStackTrace();
	   }
	   
	   int q=0;
	    do
	    {
	    	StringBuffer result=new StringBuffer();
	  
	    	try {
	    		
				String sid=(String)getAttribute("AUTHSID");
				String token=(String)getAttribute("AUTHTOKEN");
				String nexturl="https://"+sid+":"+token+"@api.twilio.com"+nextPageUri;
				

				HttpClient client = HttpClientBuilder.create().build();
				HttpRequest request = new HttpGet(nexturl);
				response = client.execute((HttpUriRequest) request);
				
		    	
	    		} catch (IOException e) {
	    				// TODO Auto-generated catch block
	    			e.printStackTrace();
	    		}
	    	
	    	BufferedReader rd=null;
	    	try {
	    		rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
	    		} catch (IllegalStateException | IOException e1) {
	    			// TODO Auto-generated catch block
	    			e1.printStackTrace();
	    		}
	    	String line = "";
	    	try {
	    		while ((line = rd.readLine()) != null) 
	    		{
	    			result.append(line);
	    		}
	    		} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
	    		}
	    	
	
		 JSONObject json = null;
		 JSONArray arr = null;
		 int listSize = 0;
		 try {
		    	json = (JSONObject) parser.parse(result.toString());
		    	arr = (JSONArray) json.get(jsonarr);   	
		    	nextPageUri=(String) json.get("next_page_uri");
	            listSize=arr.size();    
			} catch (ParseException e) {
				e.printStackTrace();
			}
		 
		 if(loadBlob(BLOB)==null){
			 twilio="";
			 saveBlob(BLOB,twilio.getBytes());
		 }
		 
		 
		 twilio = new String(loadBlob(BLOB));
		 twilio = twilio+arr.toString();
		 twilio = twilio.replaceAll("]\\[", ",");
		 twilio = twilio.replaceAll("\\[\\[\\{", "\\[\\{");
		 saveBlob(BLOB,twilio.getBytes()); 
		 
	    }while(nextPageUri!=null);
		     }
		    
		
	
	public ScheduleDefinition getScheduleDefinition() { 
		/*In this function define the frequency with which Yellowfin should execute the autorun function*/
		return new ScheduleDefinition(FrequencyTypeCode.MINUTES, null, 15); 
	};
	
	
	public ArrayList<AbstractDataSet> getDataSets() {
		
		/*In this function define the list of datasets available for this connector.*/
		
		ArrayList<AbstractDataSet> p = new ArrayList<AbstractDataSet>();
		
		/*Each dataset is an AbstractDataSet.*/
		//p.add(Reports());
		
		
		/*Each dataset is an AbstractDataSet.*/
		p.add(callLog());
		p.add(messages());
		p.add(recordingLog());
		p.add(notifications());
		p.add(transcription());
		p.add(queue());
		p.add(applications());
		p.add(conferences());
		return p;
		
	}
	
	
	
////////////////////////////////////////CallsStart/////////////////////////////////////////////////////////////////////////////
	private AbstractDataSet callLog()
	{
		AbstractDataSet simpleDataSet = new AbstractDataSet() {
			
			public ArrayList<FilterMetaData> getFilters() {
				/*In this function define he list of available filters*/
				ArrayList<FilterMetaData> fm = new ArrayList<FilterMetaData>();
				return fm;
				
			}
			
			
			public String getDataSetName() {
				/*Here define the dataset name*/
				return "Call Log";
				
				
			}
			
			public ArrayList<ColumnMetaData> getColumns() {
				
				/*In this function define the list of columns available in the dataset*/
				ArrayList<ColumnMetaData> cm = new ArrayList<ColumnMetaData>();
				
				cm.add(new ColumnMetaData("AccountSID", DataType.TEXT));
				cm.add(new ColumnMetaData("CallSID", DataType.TEXT));
				cm.add(new ColumnMetaData("AnsweredBy", DataType.TEXT));
				cm.add(new ColumnMetaData("CallerName", DataType.TEXT));
				/*cm.add(new ColumnMetaData("StartTime", DataType.TIMESTAMP));
				cm.add(new ColumnMetaData("EndTime", DataType.TIMESTAMP));*/
				cm.add(new ColumnMetaData("Duration(Seconds)", DataType.NUMERIC));
				cm.add(new ColumnMetaData("To", DataType.TEXT));
				cm.add(new ColumnMetaData("From", DataType.TEXT));
				cm.add(new ColumnMetaData("Status", DataType.TEXT));
				cm.add(new ColumnMetaData("CallCost", DataType.NUMERIC));
				cm.add(new ColumnMetaData("CallCostUnit", DataType.TEXT));
				cm.add(new ColumnMetaData("Direction", DataType.TEXT));
				cm.add(new ColumnMetaData("Counter", DataType.NUMERIC));
				
				cm.add(new ColumnMetaData("Created On", DataType.DATE));
				cm.add(new ColumnMetaData("Created On Time Stamp",DataType.TIMESTAMP));
				cm.add(new ColumnMetaData("Created On Month Start Date",DataType.DATE));
				cm.add(new ColumnMetaData("Created On Year Start Date",DataType.DATE));
				cm.add(new ColumnMetaData("Created On Quarter Start Date",DataType.DATE));
				cm.add(new ColumnMetaData("Created On Week Start Date",DataType.DATE));
				cm.add(new ColumnMetaData("Created On Day of Week",DataType.INTEGER));
				cm.add(new ColumnMetaData("Created On Day of Week Name",DataType.TEXT));
				cm.add(new ColumnMetaData("Created On Hour",DataType.INTEGER));
				cm.add(new ColumnMetaData("Created On Month Number",DataType.INTEGER));
				cm.add(new ColumnMetaData("Created On Month Name",DataType.TEXT));
				cm.add(new ColumnMetaData("Created On Quarter",DataType.INTEGER));
				cm.add(new ColumnMetaData("Created On Year",DataType.INTEGER));
				cm.add(new ColumnMetaData("Created On Day of Month",DataType.INTEGER));		
				
				cm.add(new ColumnMetaData("Ended On", DataType.DATE));
				cm.add(new ColumnMetaData("Ended On Time Stamp",DataType.TIMESTAMP));
				cm.add(new ColumnMetaData("Ended On Month Start Date",DataType.DATE));
				cm.add(new ColumnMetaData("Ended On Year Start Date",DataType.DATE));
				cm.add(new ColumnMetaData("Ended On Quarter Start Date",DataType.DATE));
				cm.add(new ColumnMetaData("Ended On Week Start Date",DataType.DATE));
				cm.add(new ColumnMetaData("Ended On Day of Week",DataType.INTEGER));
				cm.add(new ColumnMetaData("Ended On Day of Week Name",DataType.TEXT));
				cm.add(new ColumnMetaData("Ended On Hour",DataType.INTEGER));
				cm.add(new ColumnMetaData("Ended On Month Number",DataType.INTEGER));
				cm.add(new ColumnMetaData("Ended On Month Name",DataType.TEXT));
				cm.add(new ColumnMetaData("Ended On Quarter",DataType.INTEGER));
				cm.add(new ColumnMetaData("Ended On Year",DataType.INTEGER));
				cm.add(new ColumnMetaData("Ended On Day of Month",DataType.INTEGER));
				
				return cm;
			}
			
				
			
			public Object[][] execute(List<ColumnMetaData> columns, List<FilterData> filters) {
				
				/*This is the main function that should return a dataset according to user's preferences
				 * The dataset is represented by an object matrix. 
				 * List<ColumnMetaData> columns contains the list of columns that the user has selected.
				 * List<FilterData> filters contains the filters and their values that the user has selected*/
				/*if (TestDataSource.this.loadBlob("LASTRUN") == null) {
                	throw new ThirdPartyException("The database is not yet populated! Please try in 10 minutes");
                }
				if (columns.size() == 0) {
                    return null;
                }*/
				
				Object[][]data = null;
				
				byte[] tcalls= loadBlob("TWILIO_CALLS");
				String strcalls = new String(tcalls);
				//System.out.println(strcalls);
				JSONParser parser = new JSONParser();
				JSONArray arrcalls;
				try {
				arrcalls = (JSONArray) parser.parse(strcalls); 
				data=new Object[arrcalls.size()][columns.size()];
				int i, j;
				JSONObject jsonCalls = null;
				for(i=0; i<arrcalls.size(); i++)
				{
					try {
						jsonCalls = (JSONObject) parser.parse(arrcalls.get(i).toString());
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
						

					    
					    Timestamp timestampStart = null;
						try {
							 timestampStart = dateFormatter((String) jsonCalls.get("start_time"));
						} catch (java.text.ParseException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
						    DateFields df1 = dateField(timestampStart);
						    
						    Timestamp timestampEnd = null;
							try {
								 timestampStart = dateFormatter((String) jsonCalls.get("end_time"));
							} catch (java.text.ParseException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}
							DateFields df2 = dateField(timestampStart);
						    
						
	
					for(j=0;j<columns.size(); j++)
					{
						if (columns.get(j).getColumnName().equals("CallSID"))
						{
							value=(String) jsonCalls.get("sid");
							data[i][j]=value;
						}
						else if (columns.get(j).getColumnName().equals("AccountSID"))
						{
							value=(String) jsonCalls.get("account_sid");
							data[i][j]=value;
						}
						
						else if (columns.get(j).getColumnName().equals("AnsweredBy"))
						{
							value=(String) jsonCalls.get("answered_by");
							try {
								if(value.equals(""))
									value="NULL";
							} catch (NullPointerException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							//value="NULL";
							data[i][j]=value;
						}
						
						else if (columns.get(j).getColumnName().equals("CallerName"))
						{
							value=(String) jsonCalls.get("caller_name");
							try {
								if(value.equals(null))
									value="";
								} catch (NullPointerException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							
							//value="N/A";
							data[i][j]=value;
						}
						/*else if (columns.get(j).getColumnName().equals("StartTime"))
						{
							value=(String) jsonCalls.get("start_time");
							
							try {
								data[i][j]=dateFormatter(value);
							} catch (java.text.ParseException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							System.out.println(data[i][j]);
						}
						else if (columns.get(j).getColumnName().equals("EndTime"))
						{
							value=(String) jsonCalls.get("end_time");
							try {
								data[i][j]=dateFormatter(value);
							} catch (java.text.ParseException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}*/
						else if (columns.get(j).getColumnName().equals("Duration(Seconds)"))
						{
							value=(String) jsonCalls.get("duration");
							int val=Integer.parseInt(value);
							data[i][j]=val;
						}
						else if (columns.get(j).getColumnName().equals("To"))
						{
							value=(String) jsonCalls.get("to");
							data[i][j]=value;
						}
						else if (columns.get(j).getColumnName().equals("From"))
						{
							value=(String) jsonCalls.get("from");
							data[i][j]=value;
						}
						else if (columns.get(j).getColumnName().equals("Status"))
						{
							value=(String) jsonCalls.get("status");
							data[i][j]=value;
						}
						else if (columns.get(j).getColumnName().equals("CallCost"))
						{
							try {
								value=(String) jsonCalls.get("price");
								
							} catch (NullPointerException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							if(value==null)
								value="0";
							double val=Double.parseDouble(value);
							data[i][j]=val;
						}
						else if (columns.get(j).getColumnName().equals("CallCostUnit"))
						{
							value=(String) jsonCalls.get("price_unit");
							data[i][j]=value;
						}
						
						else if (columns.get(j).getColumnName().equals("Direction"))
						{
							value=(String) jsonCalls.get("direction");
							data[i][j]=value;
						}
						
						else if (columns.get(j).getColumnName().equals("Created On"))
							data[i][j]=df1.getDate();
						else if(columns.get(j).getColumnName().equals("Created On Time Stamp"))
						    data[i][j]=df1.getTimestamp();					
						else if(columns.get(j).getColumnName().equals("Created On Month Start Date"))
						    data[i][j]=df1.getMonthStartDate();						
						else if(columns.get(j).getColumnName().equals("Created On Year Start Date"))
						    data[i][j]=df1.getYearStartDate();					
						else if(columns.get(j).getColumnName().equals("Created On Quarter Start Date"))
						    data[i][j]=df1.getQuarterStartDate();						
						else if(columns.get(j).getColumnName().equals("Created On Week Start Date"))
						    data[i][j]=df1.getWeekStartDate();						
						else if(columns.get(j).getColumnName().equals("Created On Day of Week"))
						    data[i][j]=df1.getWeekdayNumber();					
						else if(columns.get(j).getColumnName().equals("Created On Day of Week Name"))
						    data[i][j]=df1.getWeekdayName();						
						else if(columns.get(j).getColumnName().equals("Created On Hour"))
						    data[i][j]=df1.getHour();						
						else if(columns.get(j).getColumnName().equals("Created On Month Number"))
						    data[i][j]=df1.getMonthNumber();				
						else if(columns.get(j).getColumnName().equals("Created On Month Name"))
						    data[i][j]=df1.getMonthName();
						else if(columns.get(j).getColumnName().equals("Created On Quarter"))
						    data[i][j]=df1.getQuarter();
						else if(columns.get(j).getColumnName().equals("Created On Year"))
						    data[i][j]=df1.getYear();
						else if(columns.get(j).getColumnName().equals("Created On Day of Month"))
						    data[i][j]=df1.getDayOfMonth();
						else if (columns.get(j).getColumnName().equals("Ended On"))
							data[i][j]=df2.getDate();
						else if(columns.get(j).getColumnName().equals("Ended On Time Stamp"))
						    data[i][j]=df2.getTimestamp();
						else if(columns.get(j).getColumnName().equals("Ended On Month Start Date"))
						    data[i][j]=df2.getMonthStartDate();
						else if(columns.get(j).getColumnName().equals("Ended On Year Start Date"))
						    data[i][j]=df2.getYearStartDate();
						else if(columns.get(j).getColumnName().equals("Ended On Quarter Start Date"))
						    data[i][j]=df2.getQuarterStartDate();
						else if(columns.get(j).getColumnName().equals("Ended On Week Start Date"))
						    data[i][j]=df2.getWeekStartDate();
						else if(columns.get(j).getColumnName().equals("Ended On Day of Week"))
						    data[i][j]=df2.getWeekdayNumber();
						else if(columns.get(j).getColumnName().equals("Ended On Day of Week Name"))
						    data[i][j]=df2.getWeekdayName();
						else if(columns.get(j).getColumnName().equals("Ended On Hour"))
						    data[i][j]=df2.getHour();
						else if(columns.get(j).getColumnName().equals("Ended On Month Number"))
						    data[i][j]=df2.getMonthNumber();
						else if(columns.get(j).getColumnName().equals("Ended On Month Name"))
						    data[i][j]=df2.getMonthName();
						else if(columns.get(j).getColumnName().equals("Ended On Quarter"))
						    data[i][j]=df2.getQuarter();
						else if(columns.get(j).getColumnName().equals("Ended On Year"))
						    data[i][j]=df2.getYear();
						else if(columns.get(j).getColumnName().equals("Ended On Day of Month"))
						    data[i][j]=df2.getDayOfMonth();
						else if (columns.get(j).getColumnName().equals("Counter"))
							data[i][j] = 1;
					 }
					
				  }

				}catch (ParseException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				return data;
				
			}
			

			@Override
			public boolean getAllowsDuplicateColumns() {
				// TODO Auto-generated method stub
				return false;
			}


			@Override
			public boolean getAllowsAggregateColumns() {
				// TODO Auto-generated method stub
				return false;
			}

			
		};
		
		return simpleDataSet;
	}

     ////////////////////////////////////////CallsEnd/////////////////////////////////////////////////////////////////////////////

	
////////////////////////////////////////RecordingStart/////////////////////////////////////////////////////////////////////////////
private AbstractDataSet recordingLog()
{
AbstractDataSet simpleDataSet = new AbstractDataSet() {

public ArrayList<FilterMetaData> getFilters() {
/*In this function define he list of available filters*/
ArrayList<FilterMetaData> fm = new ArrayList<FilterMetaData>();
return fm;

}


public String getDataSetName() {
/*Here define the dataset name*/
return "Recording Log";


}

public ArrayList<ColumnMetaData> getColumns() {

/*In this function define the list of columns available in the dataset*/
ArrayList<ColumnMetaData> cm = new ArrayList<ColumnMetaData>();

cm.add(new ColumnMetaData("RecordSID", DataType.TEXT));
cm.add(new ColumnMetaData("AccountSID", DataType.TEXT));
cm.add(new ColumnMetaData("CallSID", DataType.TEXT));
/*cm.add(new ColumnMetaData("RecordDateCreated", DataType.TIMESTAMP));
cm.add(new ColumnMetaData("RecordDateUpdated", DataType.TIMESTAMP));*/
cm.add(new ColumnMetaData("RecordingDuration(Seconds)", DataType.NUMERIC));
cm.add(new ColumnMetaData("RecordingStatus", DataType.TEXT));
cm.add(new ColumnMetaData("RecordCost", DataType.NUMERIC));
cm.add(new ColumnMetaData("RecordCostUnit", DataType.TEXT));
cm.add(new ColumnMetaData("Source", DataType.TEXT));
cm.add(new ColumnMetaData("Channels", DataType.NUMERIC));
cm.add(new ColumnMetaData("Counter", DataType.NUMERIC));

cm.add(new ColumnMetaData("Created On", DataType.DATE));
cm.add(new ColumnMetaData("Created On Time Stamp",DataType.TIMESTAMP));
cm.add(new ColumnMetaData("Created On Month Start Date",DataType.DATE));
cm.add(new ColumnMetaData("Created On Year Start Date",DataType.DATE));
cm.add(new ColumnMetaData("Created On Quarter Start Date",DataType.DATE));
cm.add(new ColumnMetaData("Created On Week Start Date",DataType.DATE));
cm.add(new ColumnMetaData("Created On Day of Week",DataType.INTEGER));
cm.add(new ColumnMetaData("Created On Day of Week Name",DataType.TEXT));
cm.add(new ColumnMetaData("Created On Hour",DataType.INTEGER));
cm.add(new ColumnMetaData("Created On Month Number",DataType.INTEGER));
cm.add(new ColumnMetaData("Created On Month Name",DataType.TEXT));
cm.add(new ColumnMetaData("Created On Quarter",DataType.INTEGER));
cm.add(new ColumnMetaData("Created On Year",DataType.INTEGER));
cm.add(new ColumnMetaData("Created On Day of Month",DataType.INTEGER));		

cm.add(new ColumnMetaData("Updated On", DataType.DATE));
cm.add(new ColumnMetaData("Updated On Time Stamp",DataType.TIMESTAMP));
cm.add(new ColumnMetaData("Updated On Month Start Date",DataType.DATE));
cm.add(new ColumnMetaData("Updated On Year Start Date",DataType.DATE));
cm.add(new ColumnMetaData("Updated On Quarter Start Date",DataType.DATE));
cm.add(new ColumnMetaData("Updated On Week Start Date",DataType.DATE));
cm.add(new ColumnMetaData("Updated On Day of Week",DataType.INTEGER));
cm.add(new ColumnMetaData("Updated On Day of Week Name",DataType.TEXT));
cm.add(new ColumnMetaData("Updated On Hour",DataType.INTEGER));
cm.add(new ColumnMetaData("Updated On Month Number",DataType.INTEGER));
cm.add(new ColumnMetaData("Updated On Month Name",DataType.TEXT));
cm.add(new ColumnMetaData("Updated On Quarter",DataType.INTEGER));
cm.add(new ColumnMetaData("Updated On Year",DataType.INTEGER));
cm.add(new ColumnMetaData("Updated On Day of Month",DataType.INTEGER));


return cm;
}



public Object[][] execute(List<ColumnMetaData> columns, List<FilterData> filters) {

/*This is the main function that should return a dataset according to user's preferences
* The dataset is represented by an object matrix. 
* List<ColumnMetaData> columns contains the list of columns that the user has selected.
* List<FilterData> filters contains the filters and their values that the user has selected*/
/*if (TestDataSource.this.loadBlob("LASTRUN") == null) {
throw new ThirdPartyException("The database is not yet populated! Please try in 10 minutes");
}
if (columns.size() == 0) {
return null;
}*/

Object[][]data = null;

byte[] trecordings= loadBlob("TWILIO_RECORDINGS");
String strrecordings = new String(trecordings);
//System.out.println(strrecordings);
JSONParser parser = new JSONParser();
JSONArray arrRecordings;
try {
arrRecordings = (JSONArray) parser.parse(strrecordings); 
data=new Object[arrRecordings.size()][columns.size()];
int i, j;
JSONObject jsonRecordings = null;
for(i=0; i<arrRecordings.size(); i++)
{
try {
jsonRecordings = (JSONObject) parser.parse(arrRecordings.get(i).toString());
} catch (ParseException e) {
// TODO Auto-generated catch block
e.printStackTrace();
}


Timestamp timestampStart = null;
try {
	 timestampStart = dateFormatter((String) jsonRecordings.get("date_created"));
} catch (java.text.ParseException e1) {
	// TODO Auto-generated catch block
	e1.printStackTrace();
}
    DateFields df1 = dateField(timestampStart);
    
    Timestamp timestampEnd = null;
	try {
		 timestampStart = dateFormatter((String) jsonRecordings.get("date_updated"));
	} catch (java.text.ParseException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}
	DateFields df2 = dateField(timestampStart);



for(j=0;j<columns.size(); j++)
{
if (columns.get(j).getColumnName().equals("RecordSID"))
{
value=(String) jsonRecordings.get("sid");
data[i][j]=value;
}
else if (columns.get(j).getColumnName().equals("AccountSID"))
{
value=(String) jsonRecordings.get("account_sid");
data[i][j]=value;
}
else if (columns.get(j).getColumnName().equals("CallSID"))
{
value=(String) jsonRecordings.get("call_sid");
data[i][j]=value;
}

/*else if (columns.get(j).getColumnName().equals("RecordDateCreated"))
{
value=(String) jsonRecordings.get("date_created");

try {
data[i][j]=dateFormatter(value);
} catch (java.text.ParseException e) {
// TODO Auto-generated catch block
e.printStackTrace();
}
}
else if (columns.get(j).getColumnName().equals("RecordDateUpdated"))
{
value=(String) jsonRecordings.get("date_updated");
try {
data[i][j]=dateFormatter(value);
} catch (java.text.ParseException e) {
// TODO Auto-generated catch block
e.printStackTrace();
}
}*/
else if (columns.get(j).getColumnName().equals("RecordingDuration(Seconds)"))
{
value=(String) jsonRecordings.get("duration");
int val=Integer.parseInt(value);
data[i][j]=val;
}

else if (columns.get(j).getColumnName().equals("RecordingStatus"))
{
value=(String) jsonRecordings.get("status");
data[i][j]=value;
}
else if (columns.get(j).getColumnName().equals("RecordCost"))
{
try {
value=(String) jsonRecordings.get("price");

} catch (NullPointerException e) {
// TODO Auto-generated catch block
e.printStackTrace();
}
if(value==null)
value="0";
double val=Double.parseDouble(value);
data[i][j]=val;
}
else if (columns.get(j).getColumnName().equals("RecordCostUnit"))
{
value=(String) jsonRecordings.get("price_unit");
data[i][j]=value;
}

else if (columns.get(j).getColumnName().equals("Source"))
{
value=(String) jsonRecordings.get("source");
data[i][j]=value;
}
else if (columns.get(j).getColumnName().equals("Channels"))
{
//System.out.println(jsonRecordings.get("channels"));
long val=(long) jsonRecordings.get("channels");
data[i][j]=val;
}
else if (columns.get(j).getColumnName().equals("Created On"))
	data[i][j]=df1.getDate();
else if(columns.get(j).getColumnName().equals("Created On Time Stamp"))
    data[i][j]=df1.getTimestamp();					
else if(columns.get(j).getColumnName().equals("Created On Month Start Date"))
    data[i][j]=df1.getMonthStartDate();						
else if(columns.get(j).getColumnName().equals("Created On Year Start Date"))
    data[i][j]=df1.getYearStartDate();					
else if(columns.get(j).getColumnName().equals("Created On Quarter Start Date"))
    data[i][j]=df1.getQuarterStartDate();						
else if(columns.get(j).getColumnName().equals("Created On Week Start Date"))
    data[i][j]=df1.getWeekStartDate();						
else if(columns.get(j).getColumnName().equals("Created On Day of Week"))
    data[i][j]=df1.getWeekdayNumber();					
else if(columns.get(j).getColumnName().equals("Created On Day of Week Name"))
    data[i][j]=df1.getWeekdayName();						
else if(columns.get(j).getColumnName().equals("Created On Hour"))
    data[i][j]=df1.getHour();						
else if(columns.get(j).getColumnName().equals("Created On Month Number"))
    data[i][j]=df1.getMonthNumber();				
else if(columns.get(j).getColumnName().equals("Created On Month Name"))
    data[i][j]=df1.getMonthName();
else if(columns.get(j).getColumnName().equals("Created On Quarter"))
    data[i][j]=df1.getQuarter();
else if(columns.get(j).getColumnName().equals("Created On Year"))
    data[i][j]=df1.getYear();
else if(columns.get(j).getColumnName().equals("Created On Day of Month"))
    data[i][j]=df1.getDayOfMonth();
else if (columns.get(j).getColumnName().equals("Updated On"))
	data[i][j]=df2.getDate();
else if(columns.get(j).getColumnName().equals("Updated On Time Stamp"))
    data[i][j]=df2.getTimestamp();
else if(columns.get(j).getColumnName().equals("Updated On Month Start Date"))
    data[i][j]=df2.getMonthStartDate();
else if(columns.get(j).getColumnName().equals("Updated On Year Start Date"))
    data[i][j]=df2.getYearStartDate();
else if(columns.get(j).getColumnName().equals("Updated On Quarter Start Date"))
    data[i][j]=df2.getQuarterStartDate();
else if(columns.get(j).getColumnName().equals("Updated On Week Start Date"))
    data[i][j]=df2.getWeekStartDate();
else if(columns.get(j).getColumnName().equals("Updated On Day of Week"))
    data[i][j]=df2.getWeekdayNumber();
else if(columns.get(j).getColumnName().equals("Updated On Day of Week Name"))
    data[i][j]=df2.getWeekdayName();
else if(columns.get(j).getColumnName().equals("Updated On Hour"))
    data[i][j]=df2.getHour();
else if(columns.get(j).getColumnName().equals("Updated On Month Number"))
    data[i][j]=df2.getMonthNumber();
else if(columns.get(j).getColumnName().equals("Updated On Month Name"))
    data[i][j]=df2.getMonthName();
else if(columns.get(j).getColumnName().equals("Updated On Quarter"))
    data[i][j]=df2.getQuarter();
else if(columns.get(j).getColumnName().equals("Updated On Year"))
    data[i][j]=df2.getYear();
else if(columns.get(j).getColumnName().equals("Updated On Day of Month"))
    data[i][j]=df2.getDayOfMonth();
else if (columns.get(j).getColumnName().equals("Counter"))
	data[i][j] = 1;
}

}

}catch (ParseException e1) {
// TODO Auto-generated catch block
e1.printStackTrace();
}
return data;

}


@Override
public boolean getAllowsDuplicateColumns() {
// TODO Auto-generated method stub
return false;
}


@Override
public boolean getAllowsAggregateColumns() {
// TODO Auto-generated method stub
return false;
}


};

return simpleDataSet;
}

////////////////////////////////////////RecordingsEnd/////////////////////////////////////////////////////////////////////////////


/////////////////////////////////////////////////Notification Start//////////////////////////////////////////////////////////////////////////////////
	
private AbstractDataSet notifications()
{
	AbstractDataSet simpleDataSet = new AbstractDataSet() {
		
		public ArrayList<FilterMetaData> getFilters() {
			/*In this function define he list of available filters*/
			ArrayList<FilterMetaData> fm = new ArrayList<FilterMetaData>();
			return fm;
			
		}
		
		
		public String getDataSetName() {
			/*Here define the dataset name*/
			return "Notification Log";
			
			
		}
		
		public ArrayList<ColumnMetaData> getColumns() {
			
			/*In this function define the list of columns available in the dataset*/
			ArrayList<ColumnMetaData> cm = new ArrayList<ColumnMetaData>();
			
			cm.add(new ColumnMetaData("AccountSID", DataType.TEXT));
			cm.add(new ColumnMetaData("CallSID", DataType.TEXT));
			cm.add(new ColumnMetaData("NotificationSID", DataType.TEXT));
			cm.add(new ColumnMetaData("Log", DataType.TEXT));
			cm.add(new ColumnMetaData("ErrorCode", DataType.TEXT));
			cm.add(new ColumnMetaData("MessageText", DataType.TEXT));
			cm.add(new ColumnMetaData("MessageDate", DataType.TIMESTAMP));
			/*cm.add(new ColumnMetaData("DateCreated", DataType.TIMESTAMP));
			cm.add(new ColumnMetaData("DateUpdated", DataType.TIMESTAMP));*/
			cm.add(new ColumnMetaData("Counter", DataType.NUMERIC));

			cm.add(new ColumnMetaData("Created On", DataType.DATE));
			cm.add(new ColumnMetaData("Created On Time Stamp",DataType.TIMESTAMP));
			cm.add(new ColumnMetaData("Created On Month Start Date",DataType.DATE));
			cm.add(new ColumnMetaData("Created On Year Start Date",DataType.DATE));
			cm.add(new ColumnMetaData("Created On Quarter Start Date",DataType.DATE));
			cm.add(new ColumnMetaData("Created On Week Start Date",DataType.DATE));
			cm.add(new ColumnMetaData("Created On Day of Week",DataType.INTEGER));
			cm.add(new ColumnMetaData("Created On Day of Week Name",DataType.TEXT));
			cm.add(new ColumnMetaData("Created On Hour",DataType.INTEGER));
			cm.add(new ColumnMetaData("Created On Month Number",DataType.INTEGER));
			cm.add(new ColumnMetaData("Created On Month Name",DataType.TEXT));
			cm.add(new ColumnMetaData("Created On Quarter",DataType.INTEGER));
			cm.add(new ColumnMetaData("Created On Year",DataType.INTEGER));
			cm.add(new ColumnMetaData("Created On Day of Month",DataType.INTEGER));		

			cm.add(new ColumnMetaData("Updated On", DataType.DATE));
			cm.add(new ColumnMetaData("Updated On Time Stamp",DataType.TIMESTAMP));
			cm.add(new ColumnMetaData("Updated On Month Start Date",DataType.DATE));
			cm.add(new ColumnMetaData("Updated On Year Start Date",DataType.DATE));
			cm.add(new ColumnMetaData("Updated On Quarter Start Date",DataType.DATE));
			cm.add(new ColumnMetaData("Updated On Week Start Date",DataType.DATE));
			cm.add(new ColumnMetaData("Updated On Day of Week",DataType.INTEGER));
			cm.add(new ColumnMetaData("Updated On Day of Week Name",DataType.TEXT));
			cm.add(new ColumnMetaData("Updated On Hour",DataType.INTEGER));
			cm.add(new ColumnMetaData("Updated On Month Number",DataType.INTEGER));
			cm.add(new ColumnMetaData("Updated On Month Name",DataType.TEXT));
			cm.add(new ColumnMetaData("Updated On Quarter",DataType.INTEGER));
			cm.add(new ColumnMetaData("Updated On Year",DataType.INTEGER));
			cm.add(new ColumnMetaData("Updated On Day of Month",DataType.INTEGER));
			
			return cm;
		}
		
		
		
		public Object[][] execute(List<ColumnMetaData> columns, List<FilterData> filters) {
			
			/*This is the main function that should return a dataset according to user's preferences
			 * The dataset is represented by an object matrix. 
			 * List<ColumnMetaData> columns contains the list of columns that the user has selected.
			 * List<FilterData> filters contains the filters and their values that the user has selected*/
			/*if (TestDataSource.this.loadBlob("LASTRUN") == null) {
            	throw new ThirdPartyException("The database is not yet populated! Please try in 10 minutes");
            }
			if (columns.size() == 0) {
                return null;
            }*/
			
			Object[][]data = null;
			
			byte[] tnotification= loadBlob("TWILIO_NOTIFICATIONS");
			String strnotification = new String(tnotification);
			//System.out.println(strnotification);
			JSONParser parser = new JSONParser();
			JSONArray arrnotification;
			try {
			arrnotification = (JSONArray) parser.parse(strnotification); 
			data=new Object[arrnotification.size()][columns.size()];
			int i, j;
			JSONObject jsonNotification = null;
			for(i=0; i<arrnotification.size(); i++)
			{
				try {
					jsonNotification = (JSONObject) parser.parse(arrnotification.get(i).toString());
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				Timestamp timestampStart = null;
				try {
					 timestampStart = dateFormatter((String) jsonNotification.get("date_created"));
				} catch (java.text.ParseException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				    DateFields df1 = dateField(timestampStart);
				    
				    Timestamp timestampEnd = null;
					try {
						 timestampStart = dateFormatter((String) jsonNotification.get("date_updated"));
					} catch (java.text.ParseException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					DateFields df2 = dateField(timestampStart);
				
				for(j=0;j<columns.size(); j++)
				{
					if (columns.get(j).getColumnName().equals("CallSID"))
					{
						value=(String) jsonNotification.get("call_sid");
						data[i][j]=value;
					}
					else if (columns.get(j).getColumnName().equals("AccountSID"))
					{
						value=(String) jsonNotification.get("account_sid");
						data[i][j]=value;
					}
					
					else if (columns.get(j).getColumnName().equals("NotificationSID"))
					{
						value=(String) jsonNotification.get("sid");
						data[i][j]=value;
					}
					
					else if (columns.get(j).getColumnName().equals("Log"))
					{
						value=(String) jsonNotification.get("log");
						try {
							if(value.equals(null))
								value="";
							} catch (NullPointerException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
						//value="N/A";
						data[i][j]=value;
					}
					else if (columns.get(j).getColumnName().equals("DateCreated"))
					{
						value=(String) jsonNotification.get("date_created");
						
						try {
							data[i][j]=dateFormatter(value);
						} catch (java.text.ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					else if (columns.get(j).getColumnName().equals("DateUpdated"))
					{
						value=(String) jsonNotification.get("date_updated");
						try {
							data[i][j]=dateFormatter(value);
						} catch (java.text.ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					else if (columns.get(j).getColumnName().equals("MessageDate"))
					{
						value=(String) jsonNotification.get("message_date");
						try {
							data[i][j]=dateFormatter(value);
						} catch (java.text.ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					else if (columns.get(j).getColumnName().equals("ErrorCode"))
					{
						value=(String) jsonNotification.get("error_code");
						try {
							if(value.equals(null))
								value="";
							} catch (NullPointerException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						//int val=Integer.parseInt(value);
						data[i][j]=value;
					}
					else if (columns.get(j).getColumnName().equals("MessageText"))
					{
						value=(String) jsonNotification.get("message_text");
						data[i][j]=value;
					}
					else if (columns.get(j).getColumnName().equals("Created On"))
						data[i][j]=df1.getDate();
					else if(columns.get(j).getColumnName().equals("Created On Time Stamp"))
					    data[i][j]=df1.getTimestamp();					
					else if(columns.get(j).getColumnName().equals("Created On Month Start Date"))
					    data[i][j]=df1.getMonthStartDate();						
					else if(columns.get(j).getColumnName().equals("Created On Year Start Date"))
					    data[i][j]=df1.getYearStartDate();					
					else if(columns.get(j).getColumnName().equals("Created On Quarter Start Date"))
					    data[i][j]=df1.getQuarterStartDate();						
					else if(columns.get(j).getColumnName().equals("Created On Week Start Date"))
					    data[i][j]=df1.getWeekStartDate();						
					else if(columns.get(j).getColumnName().equals("Created On Day of Week"))
					    data[i][j]=df1.getWeekdayNumber();					
					else if(columns.get(j).getColumnName().equals("Created On Day of Week Name"))
					    data[i][j]=df1.getWeekdayName();						
					else if(columns.get(j).getColumnName().equals("Created On Hour"))
					    data[i][j]=df1.getHour();						
					else if(columns.get(j).getColumnName().equals("Created On Month Number"))
					    data[i][j]=df1.getMonthNumber();				
					else if(columns.get(j).getColumnName().equals("Created On Month Name"))
					    data[i][j]=df1.getMonthName();
					else if(columns.get(j).getColumnName().equals("Created On Quarter"))
					    data[i][j]=df1.getQuarter();
					else if(columns.get(j).getColumnName().equals("Created On Year"))
					    data[i][j]=df1.getYear();
					else if(columns.get(j).getColumnName().equals("Created On Day of Month"))
					    data[i][j]=df1.getDayOfMonth();
					else if (columns.get(j).getColumnName().equals("Updated On"))
						data[i][j]=df2.getDate();
					else if(columns.get(j).getColumnName().equals("Updated On Time Stamp"))
					    data[i][j]=df2.getTimestamp();
					else if(columns.get(j).getColumnName().equals("Updated On Month Start Date"))
					    data[i][j]=df2.getMonthStartDate();
					else if(columns.get(j).getColumnName().equals("Updated On Year Start Date"))
					    data[i][j]=df2.getYearStartDate();
					else if(columns.get(j).getColumnName().equals("Updated On Quarter Start Date"))
					    data[i][j]=df2.getQuarterStartDate();
					else if(columns.get(j).getColumnName().equals("Updated On Week Start Date"))
					    data[i][j]=df2.getWeekStartDate();
					else if(columns.get(j).getColumnName().equals("Updated On Day of Week"))
					    data[i][j]=df2.getWeekdayNumber();
					else if(columns.get(j).getColumnName().equals("Updated On Day of Week Name"))
					    data[i][j]=df2.getWeekdayName();
					else if(columns.get(j).getColumnName().equals("Updated On Hour"))
					    data[i][j]=df2.getHour();
					else if(columns.get(j).getColumnName().equals("Updated On Month Number"))
					    data[i][j]=df2.getMonthNumber();
					else if(columns.get(j).getColumnName().equals("Updated On Month Name"))
					    data[i][j]=df2.getMonthName();
					else if(columns.get(j).getColumnName().equals("Updated On Quarter"))
					    data[i][j]=df2.getQuarter();
					else if(columns.get(j).getColumnName().equals("Updated On Year"))
					    data[i][j]=df2.getYear();
					else if(columns.get(j).getColumnName().equals("Updated On Day of Month"))
					    data[i][j]=df2.getDayOfMonth();
					else if (columns.get(j).getColumnName().equals("Counter"))
						data[i][j] = 1;
				 }
				
			  }

			}catch (ParseException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			return data;
			
		}
		

		@Override
		public boolean getAllowsDuplicateColumns() {
			// TODO Auto-generated method stub
			return false;
		}


		@Override
		public boolean getAllowsAggregateColumns() {
			// TODO Auto-generated method stub
			return false;
		}

		
	};
	
	return simpleDataSet;
}

 ////////////////////////////////////////NotificationEnd/////////////////////////////////////////////////////////////////////////////

	
	
////////////////////////////////////////Transcription Start/////////////////////////////////////////////////////////////////////////////
private AbstractDataSet transcription()
{
AbstractDataSet simpleDataSet = new AbstractDataSet() {

public ArrayList<FilterMetaData> getFilters() {
/*In this function define he list of available filters*/
ArrayList<FilterMetaData> fm = new ArrayList<FilterMetaData>();
return fm;

}


public String getDataSetName() {
/*Here define the dataset name*/
return "Transcriptions";


}

public ArrayList<ColumnMetaData> getColumns() {

/*In this function define the list of columns available in the dataset*/
ArrayList<ColumnMetaData> cm = new ArrayList<ColumnMetaData>();

cm.add(new ColumnMetaData("TranscriptionSID", DataType.TEXT));
cm.add(new ColumnMetaData("RecordingSID", DataType.TEXT));
/*cm.add(new ColumnMetaData("DateCreated", DataType.TIMESTAMP));
cm.add(new ColumnMetaData("DateUpdated", DataType.TIMESTAMP));*/
cm.add(new ColumnMetaData("Duration(Seconds)", DataType.NUMERIC));
cm.add(new ColumnMetaData("Type", DataType.TEXT));
cm.add(new ColumnMetaData("TranscriptionText", DataType.TEXT));
cm.add(new ColumnMetaData("Status", DataType.TEXT));
cm.add(new ColumnMetaData("TranscriptionCost", DataType.NUMERIC));
cm.add(new ColumnMetaData("TranscriptionCostUnit", DataType.TEXT));
cm.add(new ColumnMetaData("AccountSID", DataType.TEXT));
cm.add(new ColumnMetaData("Counter", DataType.NUMERIC));

cm.add(new ColumnMetaData("Created On", DataType.DATE));
cm.add(new ColumnMetaData("Created On Time Stamp",DataType.TIMESTAMP));
cm.add(new ColumnMetaData("Created On Month Start Date",DataType.DATE));
cm.add(new ColumnMetaData("Created On Year Start Date",DataType.DATE));
cm.add(new ColumnMetaData("Created On Quarter Start Date",DataType.DATE));
cm.add(new ColumnMetaData("Created On Week Start Date",DataType.DATE));
cm.add(new ColumnMetaData("Created On Day of Week",DataType.INTEGER));
cm.add(new ColumnMetaData("Created On Day of Week Name",DataType.TEXT));
cm.add(new ColumnMetaData("Created On Hour",DataType.INTEGER));
cm.add(new ColumnMetaData("Created On Month Number",DataType.INTEGER));
cm.add(new ColumnMetaData("Created On Month Name",DataType.TEXT));
cm.add(new ColumnMetaData("Created On Quarter",DataType.INTEGER));
cm.add(new ColumnMetaData("Created On Year",DataType.INTEGER));
cm.add(new ColumnMetaData("Created On Day of Month",DataType.INTEGER));		

cm.add(new ColumnMetaData("Updated On", DataType.DATE));
cm.add(new ColumnMetaData("Updated On Time Stamp",DataType.TIMESTAMP));
cm.add(new ColumnMetaData("Updated On Month Start Date",DataType.DATE));
cm.add(new ColumnMetaData("Updated On Year Start Date",DataType.DATE));
cm.add(new ColumnMetaData("Updated On Quarter Start Date",DataType.DATE));
cm.add(new ColumnMetaData("Updated On Week Start Date",DataType.DATE));
cm.add(new ColumnMetaData("Updated On Day of Week",DataType.INTEGER));
cm.add(new ColumnMetaData("Updated On Day of Week Name",DataType.TEXT));
cm.add(new ColumnMetaData("Updated On Hour",DataType.INTEGER));
cm.add(new ColumnMetaData("Updated On Month Number",DataType.INTEGER));
cm.add(new ColumnMetaData("Updated On Month Name",DataType.TEXT));
cm.add(new ColumnMetaData("Updated On Quarter",DataType.INTEGER));
cm.add(new ColumnMetaData("Updated On Year",DataType.INTEGER));
cm.add(new ColumnMetaData("Updated On Day of Month",DataType.INTEGER));

return cm;
}



public Object[][] execute(List<ColumnMetaData> columns, List<FilterData> filters) {

/*This is the main function that should return a dataset according to user's preferences
* The dataset is represented by an object matrix. 
* List<ColumnMetaData> columns contains the list of columns that the user has selected.
* List<FilterData> filters contains the filters and their values that the user has selected*/
/*if (TestDataSource.this.loadBlob("LASTRUN") == null) {
throw new ThirdPartyException("The database is not yet populated! Please try in 10 minutes");
}
if (columns.size() == 0) {
return null;
}*/

Object[][]data = null;

byte[] ttranscriptions= loadBlob("TWILIO_TRANSCRIPTIONS");
String strtranscriptions = new String(ttranscriptions);
//System.out.println(strtranscriptions);
JSONParser parser = new JSONParser();
JSONArray arrtranscriptions;
try {
arrtranscriptions = (JSONArray) parser.parse(strtranscriptions); 
data=new Object[arrtranscriptions.size()][columns.size()];
int i, j;
JSONObject jsonTranscriptions = null;
for(i=0; i<arrtranscriptions.size(); i++)
{
try {
jsonTranscriptions = (JSONObject) parser.parse(arrtranscriptions.get(i).toString());
} catch (ParseException e) {
// TODO Auto-generated catch block
e.printStackTrace();
}

Timestamp timestampStart = null;
try {
	 timestampStart = dateFormatter((String) jsonTranscriptions.get("date_created"));
} catch (java.text.ParseException e1) {
	// TODO Auto-generated catch block
	e1.printStackTrace();
}
    DateFields df1 = dateField(timestampStart);
    
    Timestamp timestampEnd = null;
	try {
		 timestampStart = dateFormatter((String) jsonTranscriptions.get("date_updated"));
	} catch (java.text.ParseException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}
	DateFields df2 = dateField(timestampStart);



for(j=0;j<columns.size(); j++)
{
if (columns.get(j).getColumnName().equals("TranscriptionSID"))
{
value=(String) jsonTranscriptions.get("sid");
data[i][j]=value;
}
else if (columns.get(j).getColumnName().equals("AccountSID"))
{
value=(String) jsonTranscriptions.get("account_sid");
data[i][j]=value;
}

else if (columns.get(j).getColumnName().equals("Type"))
{
value=(String) jsonTranscriptions.get("type");
try {
if(value.equals(""))
value="NULL";
} catch (NullPointerException e) {
// TODO Auto-generated catch block
e.printStackTrace();
}
//value="NULL";
data[i][j]=value;
}

else if (columns.get(j).getColumnName().equals("TranscriptionText"))
{
value=(String) jsonTranscriptions.get("transcription_text");
try {
if(value.equals(null))
value="";
} catch (NullPointerException e) {
// TODO Auto-generated catch block
e.printStackTrace();
}

//value="N/A";
data[i][j]=value;
}
/*else if (columns.get(j).getColumnName().equals("DateCreated"))
{
value=(String) jsonTranscriptions.get("date_created");

try {
data[i][j]=dateFormatter(value);
} catch (java.text.ParseException e) {
// TODO Auto-generated catch block
e.printStackTrace();
}
}
else if (columns.get(j).getColumnName().equals("DateUpdated"))
{
value=(String) jsonTranscriptions.get("date_updated");
try {
data[i][j]=dateFormatter(value);
} catch (java.text.ParseException e) {
// TODO Auto-generated catch block
e.printStackTrace();
}
}*/
else if (columns.get(j).getColumnName().equals("Duration(Seconds)"))
{
value=(String) jsonTranscriptions.get("duration");
int val=Integer.parseInt(value);
data[i][j]=val;
}
else if (columns.get(j).getColumnName().equals("Status"))
{
value=(String) jsonTranscriptions.get("status");
data[i][j]=value;
}
else if (columns.get(j).getColumnName().equals("TranscriptionCost"))
{
try {
value=(String) jsonTranscriptions.get("price");

} catch (NullPointerException e) {
// TODO Auto-generated catch block
e.printStackTrace();
}
if(value==null)
value="0";
double val=Double.parseDouble(value);
data[i][j]=val;
}
else if (columns.get(j).getColumnName().equals("TranscriptionCostUnit"))
{
value=(String) jsonTranscriptions.get("price_unit");
data[i][j]=value;
}

else if (columns.get(j).getColumnName().equals("RecordingSID"))
{
value=(String) jsonTranscriptions.get("recording_sid");
data[i][j]=value;
}

else if (columns.get(j).getColumnName().equals("Created On"))
	data[i][j]=df1.getDate();
else if(columns.get(j).getColumnName().equals("Created On Time Stamp"))
    data[i][j]=df1.getTimestamp();					
else if(columns.get(j).getColumnName().equals("Created On Month Start Date"))
    data[i][j]=df1.getMonthStartDate();						
else if(columns.get(j).getColumnName().equals("Created On Year Start Date"))
    data[i][j]=df1.getYearStartDate();					
else if(columns.get(j).getColumnName().equals("Created On Quarter Start Date"))
    data[i][j]=df1.getQuarterStartDate();						
else if(columns.get(j).getColumnName().equals("Created On Week Start Date"))
    data[i][j]=df1.getWeekStartDate();						
else if(columns.get(j).getColumnName().equals("Created On Day of Week"))
    data[i][j]=df1.getWeekdayNumber();					
else if(columns.get(j).getColumnName().equals("Created On Day of Week Name"))
    data[i][j]=df1.getWeekdayName();						
else if(columns.get(j).getColumnName().equals("Created On Hour"))
    data[i][j]=df1.getHour();						
else if(columns.get(j).getColumnName().equals("Created On Month Number"))
    data[i][j]=df1.getMonthNumber();				
else if(columns.get(j).getColumnName().equals("Created On Month Name"))
    data[i][j]=df1.getMonthName();
else if(columns.get(j).getColumnName().equals("Created On Quarter"))
    data[i][j]=df1.getQuarter();
else if(columns.get(j).getColumnName().equals("Created On Year"))
    data[i][j]=df1.getYear();
else if(columns.get(j).getColumnName().equals("Created On Day of Month"))
    data[i][j]=df1.getDayOfMonth();
else if (columns.get(j).getColumnName().equals("Updated On"))
	data[i][j]=df2.getDate();
else if(columns.get(j).getColumnName().equals("Updated On Time Stamp"))
    data[i][j]=df2.getTimestamp();
else if(columns.get(j).getColumnName().equals("Updated On Month Start Date"))
    data[i][j]=df2.getMonthStartDate();
else if(columns.get(j).getColumnName().equals("Updated On Year Start Date"))
    data[i][j]=df2.getYearStartDate();
else if(columns.get(j).getColumnName().equals("Updated On Quarter Start Date"))
    data[i][j]=df2.getQuarterStartDate();
else if(columns.get(j).getColumnName().equals("Updated On Week Start Date"))
    data[i][j]=df2.getWeekStartDate();
else if(columns.get(j).getColumnName().equals("Updated On Day of Week"))
    data[i][j]=df2.getWeekdayNumber();
else if(columns.get(j).getColumnName().equals("Updated On Day of Week Name"))
    data[i][j]=df2.getWeekdayName();
else if(columns.get(j).getColumnName().equals("Updated On Hour"))
    data[i][j]=df2.getHour();
else if(columns.get(j).getColumnName().equals("Updated On Month Number"))
    data[i][j]=df2.getMonthNumber();
else if(columns.get(j).getColumnName().equals("Updated On Month Name"))
    data[i][j]=df2.getMonthName();
else if(columns.get(j).getColumnName().equals("Updated On Quarter"))
    data[i][j]=df2.getQuarter();
else if(columns.get(j).getColumnName().equals("Updated On Year"))
    data[i][j]=df2.getYear();
else if(columns.get(j).getColumnName().equals("Updated On Day of Month"))
    data[i][j]=df2.getDayOfMonth();
else if (columns.get(j).getColumnName().equals("Counter"))
	data[i][j] = 1;
}

}

}catch (ParseException e1) {
// TODO Auto-generated catch block
e1.printStackTrace();
}
return data;

}


@Override
public boolean getAllowsDuplicateColumns() {
// TODO Auto-generated method stub
return false;
}


@Override
public boolean getAllowsAggregateColumns() {
// TODO Auto-generated method stub
return false;
}


};

return simpleDataSet;
}

////////////////////////////////////////Transcriptions End/////////////////////////////////////////////////////////////////////////////


////////////////////////////////////////QueueStart/////////////////////////////////////////////////////////////////////////////
private AbstractDataSet queue()
{
AbstractDataSet simpleDataSet = new AbstractDataSet() {

public ArrayList<FilterMetaData> getFilters() {
/*In this function define he list of available filters*/
ArrayList<FilterMetaData> fm = new ArrayList<FilterMetaData>();
return fm;

}


public String getDataSetName() {
/*Here define the dataset name*/
return "Queues";


}

public ArrayList<ColumnMetaData> getColumns() {

/*In this function define the list of columns available in the dataset*/
ArrayList<ColumnMetaData> cm = new ArrayList<ColumnMetaData>();

cm.add(new ColumnMetaData("QueueSID", DataType.TEXT));
cm.add(new ColumnMetaData("AccountSID", DataType.TEXT));
/*cm.add(new ColumnMetaData("QueueDateCreated", DataType.TIMESTAMP));
cm.add(new ColumnMetaData("QueueDateUpdated", DataType.TIMESTAMP));*/
cm.add(new ColumnMetaData("AverageWaitTime(Seconds)", DataType.NUMERIC));
cm.add(new ColumnMetaData("CurrentSize", DataType.NUMERIC));
cm.add(new ColumnMetaData("MaxSize", DataType.NUMERIC));
cm.add(new ColumnMetaData("FriendlyName", DataType.TEXT));
cm.add(new ColumnMetaData("Counter", DataType.NUMERIC));

cm.add(new ColumnMetaData("Created On", DataType.DATE));
cm.add(new ColumnMetaData("Created On Time Stamp",DataType.TIMESTAMP));
cm.add(new ColumnMetaData("Created On Month Start Date",DataType.DATE));
cm.add(new ColumnMetaData("Created On Year Start Date",DataType.DATE));
cm.add(new ColumnMetaData("Created On Quarter Start Date",DataType.DATE));
cm.add(new ColumnMetaData("Created On Week Start Date",DataType.DATE));
cm.add(new ColumnMetaData("Created On Day of Week",DataType.INTEGER));
cm.add(new ColumnMetaData("Created On Day of Week Name",DataType.TEXT));
cm.add(new ColumnMetaData("Created On Hour",DataType.INTEGER));
cm.add(new ColumnMetaData("Created On Month Number",DataType.INTEGER));
cm.add(new ColumnMetaData("Created On Month Name",DataType.TEXT));
cm.add(new ColumnMetaData("Created On Quarter",DataType.INTEGER));
cm.add(new ColumnMetaData("Created On Year",DataType.INTEGER));
cm.add(new ColumnMetaData("Created On Day of Month",DataType.INTEGER));		

cm.add(new ColumnMetaData("Updated On", DataType.DATE));
cm.add(new ColumnMetaData("Updated On Time Stamp",DataType.TIMESTAMP));
cm.add(new ColumnMetaData("Updated On Month Start Date",DataType.DATE));
cm.add(new ColumnMetaData("Updated On Year Start Date",DataType.DATE));
cm.add(new ColumnMetaData("Updated On Quarter Start Date",DataType.DATE));
cm.add(new ColumnMetaData("Updated On Week Start Date",DataType.DATE));
cm.add(new ColumnMetaData("Updated On Day of Week",DataType.INTEGER));
cm.add(new ColumnMetaData("Updated On Day of Week Name",DataType.TEXT));
cm.add(new ColumnMetaData("Updated On Hour",DataType.INTEGER));
cm.add(new ColumnMetaData("Updated On Month Number",DataType.INTEGER));
cm.add(new ColumnMetaData("Updated On Month Name",DataType.TEXT));
cm.add(new ColumnMetaData("Updated On Quarter",DataType.INTEGER));
cm.add(new ColumnMetaData("Updated On Year",DataType.INTEGER));
cm.add(new ColumnMetaData("Updated On Day of Month",DataType.INTEGER));


return cm;
}



public Object[][] execute(List<ColumnMetaData> columns, List<FilterData> filters) {

/*This is the main function that should return a dataset according to user's preferences
* The dataset is represented by an object matrix. 
* List<ColumnMetaData> columns contains the list of columns that the user has selected.
* List<FilterData> filters contains the filters and their values that the user has selected*/
/*if (TestDataSource.this.loadBlob("LASTRUN") == null) {
throw new ThirdPartyException("The database is not yet populated! Please try in 10 minutes");
}
if (columns.size() == 0) {
return null;
}*/

Object[][]data = null;

byte[] tqueues= loadBlob("TWILIO_QUEUES");
String strqueues = new String(tqueues);
//System.out.println(strqueues);
JSONParser parser = new JSONParser();
JSONArray arrqueues;
try {
arrqueues = (JSONArray) parser.parse(strqueues); 
data=new Object[arrqueues.size()][columns.size()];
int i, j;
JSONObject jsonQueues = null;
for(i=0; i<arrqueues.size(); i++)
{
try {
jsonQueues = (JSONObject) parser.parse(arrqueues.get(i).toString());
} catch (ParseException e) {
//TODO Auto-generated catch block
e.printStackTrace();
}

Timestamp timestampStart = null;
try {
	 timestampStart = dateFormatter((String) jsonQueues.get("date_created"));
} catch (java.text.ParseException e1) {
	// TODO Auto-generated catch block
	e1.printStackTrace();
}
    DateFields df1 = dateField(timestampStart);
    
    Timestamp timestampEnd = null;
	try {
		 timestampStart = dateFormatter((String) jsonQueues.get("date_updated"));
	} catch (java.text.ParseException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}
	DateFields df2 = dateField(timestampStart);



for(j=0;j<columns.size(); j++)
{
if (columns.get(j).getColumnName().equals("QueueSID"))
{
value=(String) jsonQueues.get("sid");
data[i][j]=value;
}
else if (columns.get(j).getColumnName().equals("AccountSID"))
{
value=(String) jsonQueues.get("account_sid");
data[i][j]=value;
}
else if (columns.get(j).getColumnName().equals("FriendlyName"))
{
value=(String) jsonQueues.get("friendly_name");
data[i][j]=value;
}

/*else if (columns.get(j).getColumnName().equals("QueueDateCreated"))
{
value=(String) jsonQueues.get("date_created");

try {
data[i][j]=dateFormatter(value);
} catch (java.text.ParseException e) {
//TODO Auto-generated catch block
e.printStackTrace();
}
}
else if (columns.get(j).getColumnName().equals("QueueDateUpdated"))
{
value=(String) jsonQueues.get("date_updated");
try {
data[i][j]=dateFormatter(value);
} catch (java.text.ParseException e) {
//TODO Auto-generated catch block
e.printStackTrace();
}
}*/
else if (columns.get(j).getColumnName().equals("AverageWaitTime(Seconds)"))
{

long val=(long) jsonQueues.get("average_wait_time");
data[i][j]=val;
}

else if (columns.get(j).getColumnName().equals("CurrentSize"))
{
long val=(long) jsonQueues.get("current_size");
data[i][j]=val;
}

else if (columns.get(j).getColumnName().equals("MaxSize"))
{

long val=(long) jsonQueues.get("max_size");
data[i][j]=val;
}

else if (columns.get(j).getColumnName().equals("Created On"))
	data[i][j]=df1.getDate();
else if(columns.get(j).getColumnName().equals("Created On Time Stamp"))
    data[i][j]=df1.getTimestamp();					
else if(columns.get(j).getColumnName().equals("Created On Month Start Date"))
    data[i][j]=df1.getMonthStartDate();						
else if(columns.get(j).getColumnName().equals("Created On Year Start Date"))
    data[i][j]=df1.getYearStartDate();					
else if(columns.get(j).getColumnName().equals("Created On Quarter Start Date"))
    data[i][j]=df1.getQuarterStartDate();						
else if(columns.get(j).getColumnName().equals("Created On Week Start Date"))
    data[i][j]=df1.getWeekStartDate();						
else if(columns.get(j).getColumnName().equals("Created On Day of Week"))
    data[i][j]=df1.getWeekdayNumber();					
else if(columns.get(j).getColumnName().equals("Created On Day of Week Name"))
    data[i][j]=df1.getWeekdayName();						
else if(columns.get(j).getColumnName().equals("Created On Hour"))
    data[i][j]=df1.getHour();						
else if(columns.get(j).getColumnName().equals("Created On Month Number"))
    data[i][j]=df1.getMonthNumber();				
else if(columns.get(j).getColumnName().equals("Created On Month Name"))
    data[i][j]=df1.getMonthName();
else if(columns.get(j).getColumnName().equals("Created On Quarter"))
    data[i][j]=df1.getQuarter();
else if(columns.get(j).getColumnName().equals("Created On Year"))
    data[i][j]=df1.getYear();
else if(columns.get(j).getColumnName().equals("Created On Day of Month"))
    data[i][j]=df1.getDayOfMonth();
else if (columns.get(j).getColumnName().equals("Updated On"))
	data[i][j]=df2.getDate();
else if(columns.get(j).getColumnName().equals("Updated On Time Stamp"))
    data[i][j]=df2.getTimestamp();
else if(columns.get(j).getColumnName().equals("Updated On Month Start Date"))
    data[i][j]=df2.getMonthStartDate();
else if(columns.get(j).getColumnName().equals("Updated On Year Start Date"))
    data[i][j]=df2.getYearStartDate();
else if(columns.get(j).getColumnName().equals("Updated On Quarter Start Date"))
    data[i][j]=df2.getQuarterStartDate();
else if(columns.get(j).getColumnName().equals("Updated On Week Start Date"))
    data[i][j]=df2.getWeekStartDate();
else if(columns.get(j).getColumnName().equals("Updated On Day of Week"))
    data[i][j]=df2.getWeekdayNumber();
else if(columns.get(j).getColumnName().equals("Updated On Day of Week Name"))
    data[i][j]=df2.getWeekdayName();
else if(columns.get(j).getColumnName().equals("Updated On Hour"))
    data[i][j]=df2.getHour();
else if(columns.get(j).getColumnName().equals("Updated On Month Number"))
    data[i][j]=df2.getMonthNumber();
else if(columns.get(j).getColumnName().equals("Updated On Month Name"))
    data[i][j]=df2.getMonthName();
else if(columns.get(j).getColumnName().equals("Updated On Quarter"))
    data[i][j]=df2.getQuarter();
else if(columns.get(j).getColumnName().equals("Updated On Year"))
    data[i][j]=df2.getYear();
else if(columns.get(j).getColumnName().equals("Updated On Day of Month"))
    data[i][j]=df2.getDayOfMonth();
else if (columns.get(j).getColumnName().equals("Counter"))
	data[i][j] = 1;


}

}

}catch (ParseException e1) {
//TODO Auto-generated catch block
e1.printStackTrace();
}
return data;

}


@Override
public boolean getAllowsDuplicateColumns() {
//TODO Auto-generated method stub
return false;
}


@Override
public boolean getAllowsAggregateColumns() {
//TODO Auto-generated method stub
return false;
}


};

return simpleDataSet;
}

////////////////////////////////////////QueueEnd/////////////////////////////////////////////////////////////////////////////


	
	////////////////////////////////////////MessagesStart/////////////////////////////////////////////////////////////////////////////
	
	private AbstractDataSet messages()
	{
		AbstractDataSet simpleDataSet = new AbstractDataSet() {
			
			public ArrayList<FilterMetaData> getFilters() {
				/*In this function define he list of available filters*/
				ArrayList<FilterMetaData> fm = new ArrayList<FilterMetaData>();
				return fm;
				
			}
			
			
			public String getDataSetName() {
				/*Here define the dataset name*/
				return "Messages";
				
				
			}
			
			public ArrayList<ColumnMetaData> getColumns() {
				
				/*In this function define the list of columns available in the dataset*/
				ArrayList<ColumnMetaData> cm = new ArrayList<ColumnMetaData>();
				
				cm.add(new ColumnMetaData("AccountSID", DataType.TEXT));
				cm.add(new ColumnMetaData("MessageSID", DataType.TEXT));
				/*cm.add(new ColumnMetaData("DateCreated", DataType.TIMESTAMP));
				cm.add(new ColumnMetaData("DateUpdated", DataType.TIMESTAMP));
				cm.add(new ColumnMetaData("DateSent", DataType.TIMESTAMP));*/
				cm.add(new ColumnMetaData("Body", DataType.TEXT));
				cm.add(new ColumnMetaData("To", DataType.TEXT));
				cm.add(new ColumnMetaData("From", DataType.TEXT));
				cm.add(new ColumnMetaData("Status", DataType.TEXT));
				cm.add(new ColumnMetaData("MessageCost", DataType.NUMERIC));
				cm.add(new ColumnMetaData("MessageCostUnit", DataType.TEXT));
				cm.add(new ColumnMetaData("Counter", DataType.NUMERIC));

				cm.add(new ColumnMetaData("Created On", DataType.DATE));
				cm.add(new ColumnMetaData("Created On Time Stamp",DataType.TIMESTAMP));
				cm.add(new ColumnMetaData("Created On Month Start Date",DataType.DATE));
				cm.add(new ColumnMetaData("Created On Year Start Date",DataType.DATE));
				cm.add(new ColumnMetaData("Created On Quarter Start Date",DataType.DATE));
				cm.add(new ColumnMetaData("Created On Week Start Date",DataType.DATE));
				cm.add(new ColumnMetaData("Created On Day of Week",DataType.INTEGER));
				cm.add(new ColumnMetaData("Created On Day of Week Name",DataType.TEXT));
				cm.add(new ColumnMetaData("Created On Hour",DataType.INTEGER));
				cm.add(new ColumnMetaData("Created On Month Number",DataType.INTEGER));
				cm.add(new ColumnMetaData("Created On Month Name",DataType.TEXT));
				cm.add(new ColumnMetaData("Created On Quarter",DataType.INTEGER));
				cm.add(new ColumnMetaData("Created On Year",DataType.INTEGER));
				cm.add(new ColumnMetaData("Created On Day of Month",DataType.INTEGER));		

				cm.add(new ColumnMetaData("Updated On", DataType.DATE));
				cm.add(new ColumnMetaData("Updated On Time Stamp",DataType.TIMESTAMP));
				cm.add(new ColumnMetaData("Updated On Month Start Date",DataType.DATE));
				cm.add(new ColumnMetaData("Updated On Year Start Date",DataType.DATE));
				cm.add(new ColumnMetaData("Updated On Quarter Start Date",DataType.DATE));
				cm.add(new ColumnMetaData("Updated On Week Start Date",DataType.DATE));
				cm.add(new ColumnMetaData("Updated On Day of Week",DataType.INTEGER));
				cm.add(new ColumnMetaData("Updated On Day of Week Name",DataType.TEXT));
				cm.add(new ColumnMetaData("Updated On Hour",DataType.INTEGER));
				cm.add(new ColumnMetaData("Updated On Month Number",DataType.INTEGER));
				cm.add(new ColumnMetaData("Updated On Month Name",DataType.TEXT));
				cm.add(new ColumnMetaData("Updated On Quarter",DataType.INTEGER));
				cm.add(new ColumnMetaData("Updated On Year",DataType.INTEGER));
				cm.add(new ColumnMetaData("Updated On Day of Month",DataType.INTEGER));
				
				return cm;
			}
			
			
			public Object[][] execute(List<ColumnMetaData> columns, List<FilterData> filters) {
				
				/*This is the main function that should return a dataset according to user's preferences
				 * The dataset is represented by an object matrix. 
				 * List<ColumnMetaData> columns contains the list of columns that the user has selected.
				 * List<FilterData> filters contains the filters and their values that the user has selected*/
				
				
				Object[][]data = null;
				
				byte[] tmessage= loadBlob("TWILIO_MESSAGES");
				String strmessage = new String(tmessage);
				JSONParser parser = new JSONParser();
				JSONArray arr;
				try {
				arr = (JSONArray) parser.parse(strmessage); 
				data=new Object[arr.size()][columns.size()];
				int i, j;
				JSONObject jsonMessages = null;
				for(i=0; i<arr.size(); i++)
				{
					try {
						jsonMessages = (JSONObject) parser.parse(arr.get(i).toString());
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					
					Timestamp timestampStart = null;
					try {
						 timestampStart = dateFormatter((String) jsonMessages.get("date_created"));
					} catch (java.text.ParseException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					    DateFields df1 = dateField(timestampStart);
					    
					    Timestamp timestampEnd = null;
						try {
							 timestampStart = dateFormatter((String) jsonMessages.get("date_updated"));
						} catch (java.text.ParseException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
						DateFields df2 = dateField(timestampStart);
					
					
					
					for(j=0;j<columns.size(); j++)
					{
						if (columns.get(j).getColumnName().equals("MessageSID"))
						{
							value=(String) jsonMessages.get("sid");
							data[i][j]=value;
						}
						else if (columns.get(j).getColumnName().equals("AccountSID"))
						{
							value=(String) jsonMessages.get("account_sid");
							data[i][j]=value;
						}
						
						/*else if (columns.get(j).getColumnName().equals("DateCreated"))
						{
							value=(String) jsonMessages.get("date_created");
							//Timestamp timestamp=null;
							try {
								data[i][j]= dateFormatter(value);
							} catch (Exception e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							//value="NULL";
							//data[i][j]= timestamp;
						}
						
						else if (columns.get(j).getColumnName().equals("DateUpdated"))
						{
							value=(String) jsonMessages.get("date_updated");
							try {
								data[i][j]= dateFormatter(value);
							} catch (java.text.ParseException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							//value="N/A";
							//data[i][j]=value;
						}
						else if (columns.get(j).getColumnName().equals("DateSent"))
						{
							
							value=(String) jsonMessages.get("date_sent");
							try {
								data[i][j]= dateFormatter(value);
							} catch (java.text.ParseException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}*/
						else if (columns.get(j).getColumnName().equals("Body"))
						{
							value=(String) jsonMessages.get("body");
							data[i][j]=value;
						}
						else if (columns.get(j).getColumnName().equals("To"))
						{
							value=(String) jsonMessages.get("to");
							data[i][j]=value;
						}
						else if (columns.get(j).getColumnName().equals("From"))
						{
							value=(String) jsonMessages.get("from");
							data[i][j]=value;
						}
						else if (columns.get(j).getColumnName().equals("Status"))
						{
							value=(String) jsonMessages.get("status");
							data[i][j]=value;
						}
						else if (columns.get(j).getColumnName().equals("MessageCost"))
						{
							
							value=(String) jsonMessages.get("price");
							double val=Double.parseDouble(value);
							data[i][j]=val;
						}
						else if (columns.get(j).getColumnName().equals("MessageCostUnit"))
						{
							value=(String) jsonMessages.get("price_unit");
							data[i][j]=value;
						}
						else if (columns.get(j).getColumnName().equals("Created On"))
							data[i][j]=df1.getDate();
						else if(columns.get(j).getColumnName().equals("Created On Time Stamp"))
						    data[i][j]=df1.getTimestamp();					
						else if(columns.get(j).getColumnName().equals("Created On Month Start Date"))
						    data[i][j]=df1.getMonthStartDate();						
						else if(columns.get(j).getColumnName().equals("Created On Year Start Date"))
						    data[i][j]=df1.getYearStartDate();					
						else if(columns.get(j).getColumnName().equals("Created On Quarter Start Date"))
						    data[i][j]=df1.getQuarterStartDate();						
						else if(columns.get(j).getColumnName().equals("Created On Week Start Date"))
						    data[i][j]=df1.getWeekStartDate();						
						else if(columns.get(j).getColumnName().equals("Created On Day of Week"))
						    data[i][j]=df1.getWeekdayNumber();					
						else if(columns.get(j).getColumnName().equals("Created On Day of Week Name"))
						    data[i][j]=df1.getWeekdayName();						
						else if(columns.get(j).getColumnName().equals("Created On Hour"))
						    data[i][j]=df1.getHour();						
						else if(columns.get(j).getColumnName().equals("Created On Month Number"))
						    data[i][j]=df1.getMonthNumber();				
						else if(columns.get(j).getColumnName().equals("Created On Month Name"))
						    data[i][j]=df1.getMonthName();
						else if(columns.get(j).getColumnName().equals("Created On Quarter"))
						    data[i][j]=df1.getQuarter();
						else if(columns.get(j).getColumnName().equals("Created On Year"))
						    data[i][j]=df1.getYear();
						else if(columns.get(j).getColumnName().equals("Created On Day of Month"))
						    data[i][j]=df1.getDayOfMonth();
						else if (columns.get(j).getColumnName().equals("Updated On"))
							data[i][j]=df2.getDate();
						else if(columns.get(j).getColumnName().equals("Updated On Time Stamp"))
						    data[i][j]=df2.getTimestamp();
						else if(columns.get(j).getColumnName().equals("Updated On Month Start Date"))
						    data[i][j]=df2.getMonthStartDate();
						else if(columns.get(j).getColumnName().equals("Updated On Year Start Date"))
						    data[i][j]=df2.getYearStartDate();
						else if(columns.get(j).getColumnName().equals("Updated On Quarter Start Date"))
						    data[i][j]=df2.getQuarterStartDate();
						else if(columns.get(j).getColumnName().equals("Updated On Week Start Date"))
						    data[i][j]=df2.getWeekStartDate();
						else if(columns.get(j).getColumnName().equals("Updated On Day of Week"))
						    data[i][j]=df2.getWeekdayNumber();
						else if(columns.get(j).getColumnName().equals("Updated On Day of Week Name"))
						    data[i][j]=df2.getWeekdayName();
						else if(columns.get(j).getColumnName().equals("Updated On Hour"))
						    data[i][j]=df2.getHour();
						else if(columns.get(j).getColumnName().equals("Updated On Month Number"))
						    data[i][j]=df2.getMonthNumber();
						else if(columns.get(j).getColumnName().equals("Updated On Month Name"))
						    data[i][j]=df2.getMonthName();
						else if(columns.get(j).getColumnName().equals("Updated On Quarter"))
						    data[i][j]=df2.getQuarter();
						else if(columns.get(j).getColumnName().equals("Updated On Year"))
						    data[i][j]=df2.getYear();
						else if(columns.get(j).getColumnName().equals("Updated On Day of Month"))
						    data[i][j]=df2.getDayOfMonth();
						else if (columns.get(j).getColumnName().equals("Counter"))
							data[i][j] = 1;
					}
				}
				
				} catch (ParseException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
				return data;
				
			}
			
		
			

			@Override
			public boolean getAllowsDuplicateColumns() {
				// TODO Auto-generated method stub
				return false;
			}


			@Override
			public boolean getAllowsAggregateColumns() {
				// TODO Auto-generated method stub
				return false;
			}

			
		};
		
		return simpleDataSet;
	}
	
	/////////////////////////////////////////MessagesEnd//////////////////////////////////////////////////////////////

	////////////////////////////////////////////////Conferences Start///////////////////////////////////////////////////////////////////////////////////////
	
	private AbstractDataSet conferences()
	{
		AbstractDataSet simpleDataSet = new AbstractDataSet() {
			
			public ArrayList<FilterMetaData> getFilters() {
				/*In this function define he list of available filters*/
				ArrayList<FilterMetaData> fm = new ArrayList<FilterMetaData>();
				return fm;
				
			}
			
			
			public String getDataSetName() {
				/*Here define the dataset name*/
				return "Conferences Log";
				
				
			}
			
			public ArrayList<ColumnMetaData> getColumns() {
				
				/*In this function define the list of columns available in the dataset*/
				ArrayList<ColumnMetaData> cm = new ArrayList<ColumnMetaData>();
				
				cm.add(new ColumnMetaData("AccountSID", DataType.TEXT));
				cm.add(new ColumnMetaData("ConferenceSID", DataType.TEXT));
				/*cm.add(new ColumnMetaData("DateCreated", DataType.TIMESTAMP));
				cm.add(new ColumnMetaData("DateUpdated", DataType.TIMESTAMP));*/
				cm.add(new ColumnMetaData("FriendlyName", DataType.TEXT));
				cm.add(new ColumnMetaData("Region", DataType.TEXT));
				cm.add(new ColumnMetaData("Status", DataType.TEXT));
				cm.add(new ColumnMetaData("Counter", DataType.NUMERIC));

				cm.add(new ColumnMetaData("Created On", DataType.DATE));
				cm.add(new ColumnMetaData("Created On Time Stamp",DataType.TIMESTAMP));
				cm.add(new ColumnMetaData("Created On Month Start Date",DataType.DATE));
				cm.add(new ColumnMetaData("Created On Year Start Date",DataType.DATE));
				cm.add(new ColumnMetaData("Created On Quarter Start Date",DataType.DATE));
				cm.add(new ColumnMetaData("Created On Week Start Date",DataType.DATE));
				cm.add(new ColumnMetaData("Created On Day of Week",DataType.INTEGER));
				cm.add(new ColumnMetaData("Created On Day of Week Name",DataType.TEXT));
				cm.add(new ColumnMetaData("Created On Hour",DataType.INTEGER));
				cm.add(new ColumnMetaData("Created On Month Number",DataType.INTEGER));
				cm.add(new ColumnMetaData("Created On Month Name",DataType.TEXT));
				cm.add(new ColumnMetaData("Created On Quarter",DataType.INTEGER));
				cm.add(new ColumnMetaData("Created On Year",DataType.INTEGER));
				cm.add(new ColumnMetaData("Created On Day of Month",DataType.INTEGER));		

				cm.add(new ColumnMetaData("Updated On", DataType.DATE));
				cm.add(new ColumnMetaData("Updated On Time Stamp",DataType.TIMESTAMP));
				cm.add(new ColumnMetaData("Updated On Month Start Date",DataType.DATE));
				cm.add(new ColumnMetaData("Updated On Year Start Date",DataType.DATE));
				cm.add(new ColumnMetaData("Updated On Quarter Start Date",DataType.DATE));
				cm.add(new ColumnMetaData("Updated On Week Start Date",DataType.DATE));
				cm.add(new ColumnMetaData("Updated On Day of Week",DataType.INTEGER));
				cm.add(new ColumnMetaData("Updated On Day of Week Name",DataType.TEXT));
				cm.add(new ColumnMetaData("Updated On Hour",DataType.INTEGER));
				cm.add(new ColumnMetaData("Updated On Month Number",DataType.INTEGER));
				cm.add(new ColumnMetaData("Updated On Month Name",DataType.TEXT));
				cm.add(new ColumnMetaData("Updated On Quarter",DataType.INTEGER));
				cm.add(new ColumnMetaData("Updated On Year",DataType.INTEGER));
				cm.add(new ColumnMetaData("Updated On Day of Month",DataType.INTEGER));
				
				return cm;
			}
			
			
			public Object[][] execute(List<ColumnMetaData> columns, List<FilterData> filters) {
				
				/*This is the main function that should return a dataset according to user's preferences
				 * The dataset is represented by an object matrix. 
				 * List<ColumnMetaData> columns contains the list of columns that the user has selected.
				 * List<FilterData> filters contains the filters and their values that the user has selected*/
				
				
				Object[][]data = null;
				
				byte[] tconference= loadBlob("TWILIO_CONFERENCES");
				String strconference = new String(tconference);
				JSONParser parser = new JSONParser();
				JSONArray arr;
				try {
				arr = (JSONArray) parser.parse(strconference); 
				data=new Object[arr.size()][columns.size()];
				int i, j;
				JSONObject jsonConferences = null;
				for(i=0; i<arr.size(); i++)
				{
					try {
						jsonConferences = (JSONObject) parser.parse(arr.get(i).toString());
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					Timestamp timestampStart = null;
					try {
						 timestampStart = dateFormatter((String) jsonConferences.get("date_created"));
					} catch (java.text.ParseException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					    DateFields df1 = dateField(timestampStart);
					    
					    Timestamp timestampEnd = null;
						try {
							 timestampStart = dateFormatter((String) jsonConferences.get("date_updated"));
						} catch (java.text.ParseException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
						DateFields df2 = dateField(timestampStart);
					
					for(j=0;j<columns.size(); j++)
					{
						if (columns.get(j).getColumnName().equals("ConferenceSID"))
						{
							value=(String) jsonConferences.get("sid");
							data[i][j]=value;
						}
						else if (columns.get(j).getColumnName().equals("AccountSID"))
						{
							value=(String) jsonConferences.get("account_sid");
							data[i][j]=value;
						}
						
						/*else if (columns.get(j).getColumnName().equals("DateCreated"))
						{
							value=(String) jsonConferences.get("date_created");
							//Timestamp timestamp=null;
							try {
								data[i][j]= dateFormatter(value);
							} catch (Exception e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							//value="NULL";
							//data[i][j]= timestamp;
						}
						
						else if (columns.get(j).getColumnName().equals("DateUpdated"))
						{
							value=(String) jsonConferences.get("date_updated");
							try {
								data[i][j]= dateFormatter(value);
							} catch (java.text.ParseException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							//value="N/A";
							//data[i][j]=value;
						}*/
						else if (columns.get(j).getColumnName().equals("FriendlyName"))
						{
							try{
							value=(String) jsonConferences.get("friendly_name");
							}catch(NullPointerException e){
								e.printStackTrace();
							}
							data[i][j]=value;
						}
						else if (columns.get(j).getColumnName().equals("Region"))
						{
							value=(String) jsonConferences.get("region");
							data[i][j]=value;
						}
						else if (columns.get(j).getColumnName().equals("Status"))
						{
							value=(String) jsonConferences.get("status");
							data[i][j]=value;
						}
						else if (columns.get(j).getColumnName().equals("Created On"))
							data[i][j]=df1.getDate();
						else if(columns.get(j).getColumnName().equals("Created On Time Stamp"))
						    data[i][j]=df1.getTimestamp();					
						else if(columns.get(j).getColumnName().equals("Created On Month Start Date"))
						    data[i][j]=df1.getMonthStartDate();						
						else if(columns.get(j).getColumnName().equals("Created On Year Start Date"))
						    data[i][j]=df1.getYearStartDate();					
						else if(columns.get(j).getColumnName().equals("Created On Quarter Start Date"))
						    data[i][j]=df1.getQuarterStartDate();						
						else if(columns.get(j).getColumnName().equals("Created On Week Start Date"))
						    data[i][j]=df1.getWeekStartDate();						
						else if(columns.get(j).getColumnName().equals("Created On Day of Week"))
						    data[i][j]=df1.getWeekdayNumber();					
						else if(columns.get(j).getColumnName().equals("Created On Day of Week Name"))
						    data[i][j]=df1.getWeekdayName();						
						else if(columns.get(j).getColumnName().equals("Created On Hour"))
						    data[i][j]=df1.getHour();						
						else if(columns.get(j).getColumnName().equals("Created On Month Number"))
						    data[i][j]=df1.getMonthNumber();				
						else if(columns.get(j).getColumnName().equals("Created On Month Name"))
						    data[i][j]=df1.getMonthName();
						else if(columns.get(j).getColumnName().equals("Created On Quarter"))
						    data[i][j]=df1.getQuarter();
						else if(columns.get(j).getColumnName().equals("Created On Year"))
						    data[i][j]=df1.getYear();
						else if(columns.get(j).getColumnName().equals("Created On Day of Month"))
						    data[i][j]=df1.getDayOfMonth();
						else if (columns.get(j).getColumnName().equals("Updated On"))
							data[i][j]=df2.getDate();
						else if(columns.get(j).getColumnName().equals("Updated On Time Stamp"))
						    data[i][j]=df2.getTimestamp();
						else if(columns.get(j).getColumnName().equals("Updated On Month Start Date"))
						    data[i][j]=df2.getMonthStartDate();
						else if(columns.get(j).getColumnName().equals("Updated On Year Start Date"))
						    data[i][j]=df2.getYearStartDate();
						else if(columns.get(j).getColumnName().equals("Updated On Quarter Start Date"))
						    data[i][j]=df2.getQuarterStartDate();
						else if(columns.get(j).getColumnName().equals("Updated On Week Start Date"))
						    data[i][j]=df2.getWeekStartDate();
						else if(columns.get(j).getColumnName().equals("Updated On Day of Week"))
						    data[i][j]=df2.getWeekdayNumber();
						else if(columns.get(j).getColumnName().equals("Updated On Day of Week Name"))
						    data[i][j]=df2.getWeekdayName();
						else if(columns.get(j).getColumnName().equals("Updated On Hour"))
						    data[i][j]=df2.getHour();
						else if(columns.get(j).getColumnName().equals("Updated On Month Number"))
						    data[i][j]=df2.getMonthNumber();
						else if(columns.get(j).getColumnName().equals("Updated On Month Name"))
						    data[i][j]=df2.getMonthName();
						else if(columns.get(j).getColumnName().equals("Updated On Quarter"))
						    data[i][j]=df2.getQuarter();
						else if(columns.get(j).getColumnName().equals("Updated On Year"))
						    data[i][j]=df2.getYear();
						else if(columns.get(j).getColumnName().equals("Updated On Day of Month"))
						    data[i][j]=df2.getDayOfMonth();
						else if (columns.get(j).getColumnName().equals("Counter"))
							data[i][j] = 1;
						
					}
				}
				
				} catch (ParseException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
				return data;
				
			}
			
		
			

			@Override
			public boolean getAllowsDuplicateColumns() {
				// TODO Auto-generated method stub
				return false;
			}


			@Override
			public boolean getAllowsAggregateColumns() {
				// TODO Auto-generated method stub
				return false;
			}

			
		};
		
		return simpleDataSet;
	}
	
	/////////////////////////////////////////Conferences End//////////////////////////////////////////////////////////////
	
	
	////////////////////////////////////////Applications Start/////////////////////////////////////////////////////////////////////////////
	
	private AbstractDataSet applications()
	{
		AbstractDataSet simpleDataSet = new AbstractDataSet() {
			
			public ArrayList<FilterMetaData> getFilters() {
				/*In this function define he list of available filters*/
				ArrayList<FilterMetaData> fm = new ArrayList<FilterMetaData>();
				return fm;
				
			}
			
			
			public String getDataSetName() {
				/*Here define the dataset name*/
				return "Applications";
				
				
			}
			
			public ArrayList<ColumnMetaData> getColumns() {
				
				/*In this function define the list of columns available in the dataset*/
				ArrayList<ColumnMetaData> cm = new ArrayList<ColumnMetaData>();
				
				cm.add(new ColumnMetaData("AccountSID", DataType.TEXT));
				cm.add(new ColumnMetaData("ApplicationSID", DataType.TEXT));
				/*cm.add(new ColumnMetaData("DateCreated", DataType.TIMESTAMP));
				cm.add(new ColumnMetaData("DateUpdated", DataType.TIMESTAMP));*/
				cm.add(new ColumnMetaData("FriendlyName", DataType.TEXT));
				cm.add(new ColumnMetaData("VoiceURL", DataType.TEXT));
				cm.add(new ColumnMetaData("VoiceMethod", DataType.TEXT));
				cm.add(new ColumnMetaData("VoiceFallbackURL", DataType.TEXT));
				cm.add(new ColumnMetaData("VoiceFallbackMethod", DataType.TEXT));
				cm.add(new ColumnMetaData("StatusCallback", DataType.TEXT));
				cm.add(new ColumnMetaData("StatusCallbackMethod", DataType.TEXT));
				cm.add(new ColumnMetaData("VoiceCallerIDLookup", DataType.BOOLEAN));
				cm.add(new ColumnMetaData("SMS_URL", DataType.TEXT));
				cm.add(new ColumnMetaData("SMS_StatusCallback", DataType.TEXT));
				cm.add(new ColumnMetaData("SMS_Method", DataType.TEXT));
				cm.add(new ColumnMetaData("SMS_FallbackURL", DataType.TEXT));
				cm.add(new ColumnMetaData("SMS_FallbackMethod", DataType.TEXT));
				cm.add(new ColumnMetaData("MessageStatusCallback", DataType.TEXT));
				cm.add(new ColumnMetaData("Counter", DataType.NUMERIC));

				cm.add(new ColumnMetaData("Created On", DataType.DATE));
				cm.add(new ColumnMetaData("Created On Time Stamp",DataType.TIMESTAMP));
				cm.add(new ColumnMetaData("Created On Month Start Date",DataType.DATE));
				cm.add(new ColumnMetaData("Created On Year Start Date",DataType.DATE));
				cm.add(new ColumnMetaData("Created On Quarter Start Date",DataType.DATE));
				cm.add(new ColumnMetaData("Created On Week Start Date",DataType.DATE));
				cm.add(new ColumnMetaData("Created On Day of Week",DataType.INTEGER));
				cm.add(new ColumnMetaData("Created On Day of Week Name",DataType.TEXT));
				cm.add(new ColumnMetaData("Created On Hour",DataType.INTEGER));
				cm.add(new ColumnMetaData("Created On Month Number",DataType.INTEGER));
				cm.add(new ColumnMetaData("Created On Month Name",DataType.TEXT));
				cm.add(new ColumnMetaData("Created On Quarter",DataType.INTEGER));
				cm.add(new ColumnMetaData("Created On Year",DataType.INTEGER));
				cm.add(new ColumnMetaData("Created On Day of Month",DataType.INTEGER));		

				cm.add(new ColumnMetaData("Updated On", DataType.DATE));
				cm.add(new ColumnMetaData("Updated On Time Stamp",DataType.TIMESTAMP));
				cm.add(new ColumnMetaData("Updated On Month Start Date",DataType.DATE));
				cm.add(new ColumnMetaData("Updated On Year Start Date",DataType.DATE));
				cm.add(new ColumnMetaData("Updated On Quarter Start Date",DataType.DATE));
				cm.add(new ColumnMetaData("Updated On Week Start Date",DataType.DATE));
				cm.add(new ColumnMetaData("Updated On Day of Week",DataType.INTEGER));
				cm.add(new ColumnMetaData("Updated On Day of Week Name",DataType.TEXT));
				cm.add(new ColumnMetaData("Updated On Hour",DataType.INTEGER));
				cm.add(new ColumnMetaData("Updated On Month Number",DataType.INTEGER));
				cm.add(new ColumnMetaData("Updated On Month Name",DataType.TEXT));
				cm.add(new ColumnMetaData("Updated On Quarter",DataType.INTEGER));
				cm.add(new ColumnMetaData("Updated On Year",DataType.INTEGER));
				cm.add(new ColumnMetaData("Updated On Day of Month",DataType.INTEGER));
				
				return cm;
			}
			
			
			public Object[][] execute(List<ColumnMetaData> columns, List<FilterData> filters) {
				
				/*This is the main function that should return a dataset according to user's preferences
				 * The dataset is represented by an object matrix. 
				 * List<ColumnMetaData> columns contains the list of columns that the user has selected.
				 * List<FilterData> filters contains the filters and their values that the user has selected*/
				
				
				Object[][]data = null;
				
				byte[] tapplication= loadBlob("TWILIO_APPLICATIONS");
				String strapplication = new String(tapplication);
				JSONParser parser = new JSONParser();
				JSONArray arrApplication;
				try {
				arrApplication = (JSONArray) parser.parse(strapplication); 
				data=new Object[arrApplication.size()][columns.size()];
				int i, j;
				JSONObject jsonApplications = null;
				for(i=0; i<arrApplication.size(); i++)
				{
					try {
						jsonApplications = (JSONObject) parser.parse(arrApplication.get(i).toString());
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					
					Timestamp timestampStart = null;
					try {
						 timestampStart = dateFormatter((String) jsonApplications.get("date_created"));
					} catch (java.text.ParseException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					    DateFields df1 = dateField(timestampStart);
					    
					    Timestamp timestampEnd = null;
						try {
							 timestampStart = dateFormatter((String) jsonApplications.get("date_updated"));
						} catch (java.text.ParseException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
						DateFields df2 = dateField(timestampStart);
					
					
					
					for(j=0;j<columns.size(); j++)
					{
						if (columns.get(j).getColumnName().equals("ApplicationSID"))
						{
							value=(String) jsonApplications.get("sid");
							data[i][j]=value;
						}
						else if (columns.get(j).getColumnName().equals("AccountSID"))
						{
							value=(String) jsonApplications.get("account_sid");
							data[i][j]=value;
						}
						
						else if (columns.get(j).getColumnName().equals("DateCreated"))
						{
							value=(String) jsonApplications.get("date_created");
							//Timestamp timestamp=null;
							try {
								data[i][j]= dateFormatter(value);
							} catch (Exception e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							//value="NULL";
							//data[i][j]= timestamp;
						}
						
						else if (columns.get(j).getColumnName().equals("DateUpdated"))
						{
							value=(String) jsonApplications.get("date_updated");
							try {
								data[i][j]= dateFormatter(value);
							} catch (java.text.ParseException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							//value="N/A";
							//data[i][j]=value;
						}
						else if (columns.get(j).getColumnName().equals("FriendlyName"))
						{
							
							value=(String) jsonApplications.get("friendly_name");
							data[i][j]= value;
							
						}
						else if (columns.get(j).getColumnName().equals("VoiceURL"))
						{
							value=(String) jsonApplications.get("voice_url");
							data[i][j]=value;
						}
						else if (columns.get(j).getColumnName().equals("VoiceMethod"))
						{
							value=(String) jsonApplications.get("voice_method");
							data[i][j]=value;
						}
						else if (columns.get(j).getColumnName().equals("VoiceFallbackURL"))
						{
							try{
							value=(String) jsonApplications.get("voice_fallback_url");
							}catch(NullPointerException e){
								e.printStackTrace();
							}
							data[i][j]=value;
						}
						else if (columns.get(j).getColumnName().equals("VoiceFallbackMethod"))
						{
							value=(String) jsonApplications.get("voice_fallback_method");
							data[i][j]=value;
						}
						else if (columns.get(j).getColumnName().equals("StatusCallback"))
						{
							try{
								value=(String) jsonApplications.get("status_callback");
								}catch(NullPointerException e){
									e.printStackTrace();
								}
							data[i][j]=value;
						}
						else if (columns.get(j).getColumnName().equals("StatusCallbackMethod"))
						{
							value=(String) jsonApplications.get("status_callback_method");
							data[i][j]=value;
						}
						else if (columns.get(j).getColumnName().equals("VoiceCallerIDLookup"))
						{
							Boolean val=(Boolean) jsonApplications.get("voice_caller_id_lookup");
							data[i][j]=val;
						}
						else if (columns.get(j).getColumnName().equals("SMS_URL"))
						{
							try{
								value=(String) jsonApplications.get("sms_url");
								}catch(NullPointerException e){
									e.printStackTrace();
								}
							data[i][j]=value;
						}
						else if (columns.get(j).getColumnName().equals("SMS_Method"))
						{
							value=(String) jsonApplications.get("sms_method");
							data[i][j]=value;
						}
						else if (columns.get(j).getColumnName().equals("SMS_FallbackURL"))
						{
							try{
								value=(String) jsonApplications.get("sms_fallback_url");
								}catch(NullPointerException e){
									e.printStackTrace();
								}
							data[i][j]=value;
						}
						else if (columns.get(j).getColumnName().equals("SMS_StatusCallback"))
						{
							try{
								value=(String) jsonApplications.get("sms_status_callback");
								}catch(NullPointerException e){
									e.printStackTrace();
								}
							data[i][j]=value;
						}
						else if (columns.get(j).getColumnName().equals("SMS_FallbackMethod"))
						{
							value=(String) jsonApplications.get("sms_fallback_method");
							data[i][j]=value;
						}
						else if (columns.get(j).getColumnName().equals("MessageStatusCallback"))
						{
							try{
								value=(String) jsonApplications.get("message_status_callback");
								}catch(NullPointerException e){
									e.printStackTrace();
								}
							data[i][j]=value;
						}
						else if (columns.get(j).getColumnName().equals("Created On"))
							data[i][j]=df1.getDate();
						else if(columns.get(j).getColumnName().equals("Created On Time Stamp"))
						    data[i][j]=df1.getTimestamp();					
						else if(columns.get(j).getColumnName().equals("Created On Month Start Date"))
						    data[i][j]=df1.getMonthStartDate();						
						else if(columns.get(j).getColumnName().equals("Created On Year Start Date"))
						    data[i][j]=df1.getYearStartDate();					
						else if(columns.get(j).getColumnName().equals("Created On Quarter Start Date"))
						    data[i][j]=df1.getQuarterStartDate();						
						else if(columns.get(j).getColumnName().equals("Created On Week Start Date"))
						    data[i][j]=df1.getWeekStartDate();						
						else if(columns.get(j).getColumnName().equals("Created On Day of Week"))
						    data[i][j]=df1.getWeekdayNumber();					
						else if(columns.get(j).getColumnName().equals("Created On Day of Week Name"))
						    data[i][j]=df1.getWeekdayName();						
						else if(columns.get(j).getColumnName().equals("Created On Hour"))
						    data[i][j]=df1.getHour();						
						else if(columns.get(j).getColumnName().equals("Created On Month Number"))
						    data[i][j]=df1.getMonthNumber();				
						else if(columns.get(j).getColumnName().equals("Created On Month Name"))
						    data[i][j]=df1.getMonthName();
						else if(columns.get(j).getColumnName().equals("Created On Quarter"))
						    data[i][j]=df1.getQuarter();
						else if(columns.get(j).getColumnName().equals("Created On Year"))
						    data[i][j]=df1.getYear();
						else if(columns.get(j).getColumnName().equals("Created On Day of Month"))
						    data[i][j]=df1.getDayOfMonth();
						else if (columns.get(j).getColumnName().equals("Updated On"))
							data[i][j]=df2.getDate();
						else if(columns.get(j).getColumnName().equals("Updated On Time Stamp"))
						    data[i][j]=df2.getTimestamp();
						else if(columns.get(j).getColumnName().equals("Updated On Month Start Date"))
						    data[i][j]=df2.getMonthStartDate();
						else if(columns.get(j).getColumnName().equals("Updated On Year Start Date"))
						    data[i][j]=df2.getYearStartDate();
						else if(columns.get(j).getColumnName().equals("Updated On Quarter Start Date"))
						    data[i][j]=df2.getQuarterStartDate();
						else if(columns.get(j).getColumnName().equals("Updated On Week Start Date"))
						    data[i][j]=df2.getWeekStartDate();
						else if(columns.get(j).getColumnName().equals("Updated On Day of Week"))
						    data[i][j]=df2.getWeekdayNumber();
						else if(columns.get(j).getColumnName().equals("Updated On Day of Week Name"))
						    data[i][j]=df2.getWeekdayName();
						else if(columns.get(j).getColumnName().equals("Updated On Hour"))
						    data[i][j]=df2.getHour();
						else if(columns.get(j).getColumnName().equals("Updated On Month Number"))
						    data[i][j]=df2.getMonthNumber();
						else if(columns.get(j).getColumnName().equals("Updated On Month Name"))
						    data[i][j]=df2.getMonthName();
						else if(columns.get(j).getColumnName().equals("Updated On Quarter"))
						    data[i][j]=df2.getQuarter();
						else if(columns.get(j).getColumnName().equals("Updated On Year"))
						    data[i][j]=df2.getYear();
						else if(columns.get(j).getColumnName().equals("Updated On Day of Month"))
						    data[i][j]=df2.getDayOfMonth();
						else if (columns.get(j).getColumnName().equals("Counter"))
							data[i][j] = 1;

					}
				}
				
				} catch (ParseException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
				return data;
				
			}
			
		
			

			@Override
			public boolean getAllowsDuplicateColumns() {
				// TODO Auto-generated method stub
				return false;
			}


			@Override
			public boolean getAllowsAggregateColumns() {
				// TODO Auto-generated method stub
				return false;
			}

			
		};
		
		return simpleDataSet;
	}
	
	/////////////////////////////////////////ApplicationsEnd//////////////////////////////////////////////////////////////

	
	public JDBCMetaData getDataSourceMetaData() {
		return new TestSourceMetaData();
	}


	public boolean authenticate() 
	{
		return true;
	}
	
	public void disconnect(){
		
	}

	public Map<String,Object> testConnection() throws ClientProtocolException, IOException{
		
		/*In this function you should define the actions that the connector should perform
		 * if the user clicks the 'Test Connection' button. 
		 * If you want to tell Yellowfin that the connection was not successful then the 
		 * Map that you return should contain a value with key "ERROR"*/
		Map<String,Object> p = new HashMap<String, Object>();
		HttpResponse response=respond();
		int status=response.getStatusLine().getStatusCode();
		if(status==200)
		{
			
			p.put("Version", 1);
			p.put("connector", "test connector");
			
		}
		else
		{
			p.put("ERROR", "Plese insert valid credentials to pass validation");
		}
		
		return p;
	}	
	
	public boolean autoRun(){
		
		/*This function is being automatically called by 
		 * Yellowfin with a frequency defined in ScheduleDefinition() function.
		 * It can be helpful if you need to run a background job for the connector (for example update tokens)*/
		System.out.println("Auto running Test data source");
		
		View("Calls","calls","TWILIO_CALLS");
		View("Messages","messages","TWILIO_MESSAGES");
		View("Recordings","recordings","TWILIO_RECORDINGS");
		View("Notifications","notifications","TWILIO_NOTIFICATIONS");
		View("Transcriptions","transcriptions","TWILIO_TRANSCRIPTIONS");
		View("Queues","queues","TWILIO_QUEUES");
		View("Applications","applications","TWILIO_APPLICATIONS");
		View("Conferences","conferences","TWILIO_CONFERENCES");

		return true;
	}

}
