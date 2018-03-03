package com.hof.TestConnector;

import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.HttpClientBuilder;

import com.hof.pool.DBType;
import com.hof.pool.JDBCMetaData;
import com.hof.util.UtilString;

public class TestSourceMetaData extends JDBCMetaData {

	public TestSourceMetaData() {
		
		super();
		
		sourceName = "Twilio";
		sourceCode = "TWILIO";
		driverName = "com.hof.TestConnector.TestDataSource";
		sourceType = DBType.THIRD_PARTY;
	}
	
		
	
	 public  void initialiseParameters() 
	 {
	        super.initialiseParameters();
	        
	        /*In this function need to define the list of parameters that are required to be filled to establish a connection to the webservice.
	         */
	        addParameter(new Parameter("HELP", "Connection Details",  "Twilio", TYPE_NUMERIC, DISPLAY_STATIC_TEXT, null, true));
	        
	        /*Parameter p = new Parameter("URL", "1. Request Access PIN", "Connect to twitter to recieve a PIN for data access",TYPE_UNKNOWN, DISPLAY_URLBUTTON,  null, true);
	        p.addOption("BUTTONTEXT", "Request URL");
	        p.addOption("BUTTONURL", "http://google.com");
	        
	        addParameter(p);*/
	        Parameter p =new Parameter("AUTHSID", "2. Enter Client SID",  "Enter the SID recieved from Twilio", TYPE_TEXT, DISPLAY_TEXT_MED, null, true);
	        addParameter(p);
	        addParameter(new Parameter("AUTHTOKEN", "2. Enter Auth Token",  "Enter the Auth Token recieved from Twilio", TYPE_TEXT, DISPLAY_TEXT_MED, null, true));
	       // addParameter(new Parameter("PIN", "2. Enter PIN",  "Enter the PIN recieved from Twitter", TYPE_NUMERIC, DISPLAY_TEXT_MED, null, true));
	        /*p = new Parameter("POSTPIN", "3. Validate Authentication",  "Validate the authentication", TYPE_TEXT, DISPLAY_BUTTON, null, true);
	        p.addOption("BUTTONTEXT", "Validate Authentication");
	        addParameter(p);*/
	        /*
	        addParameter(new Parameter("ACCESSTOKEN", "Access Token",  "AccessToken that allows access to the Twitter API", TYPE_TEXT, DISPLAY_TEXT_LONG, null, true));
	        addParameter(new Parameter("ACCESSTOKENSECRET", "Access Token Secret",  "AccessToken Password that allows access to the Twitter API", TYPE_TEXT, DISPLAY_PASSWORD, null, true));
	       */
	 }	    
	   
	 public String buttonPressed(String buttonName) throws Exception 
	 {    
        /*In this function you should define the actions that should be performed in case if some button was pressed. 
         *String buttonName contains the ID of the button that was pressed */
		 if (buttonName.equals("POSTPIN"))
	     {
			/* setParameterValue("ACCESSTOKEN", "PIN: "+getParameterValue("PIN"));
		     setParameterValue("ACCESSTOKENSECRET", "PIN: " + getParameterValue("PIN"));*/
			 Map<String,Object> p = new HashMap<String, Object>();
			 String url="https://"+getParameterValue("AUTHSID")+":"+getParameterValue("AUTHTOKEN")+"@api.twilio.com/2010-04-01/Accounts/"+getParameterValue("AUTHSID")+"/Calls.json";
		        HttpClient client = HttpClientBuilder.create().build();
				HttpRequest request = new HttpGet(url);
				HttpResponse response= client.execute((HttpUriRequest) request);
				int status=response.getStatusLine().getStatusCode();
				if(status==200)
				{
					p.put("Success", "Good to go ..");
				}
				else
				{
					p.put("ERROR", "Plese insert valid credentials to pass this validation");
				}
			 
	     }
		 
		 return null;
	 }
	 
	 @Override
		public byte[] getDatasourceIcon() {
		 /*This function should return Base-64 encrypted version of icon file*/
		 String str = "iVBORw0KGgoAAAANSUhEUgAAAFAAAABQCAMAAAC5zwKfAAACN1BMVEUAAADjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHibjHib///+zJ00rAAAAu3RSTlMAAAECAwQFBgcICQoLDA4QERIUFRYYGRocHR4fISIjJCYnKCkqKywtLzAxMjM0NTc4Ojs9P0BCREVISUpMTU9QUVNUVVZXWFpcXmBhYmRnaW1vcXN1dnd4eXp7fH6AgYKDhIWGiIqMjpCRlZaXmJmanJ2en6Cho6SoqaqrrK2ur7O3uru8wMLDxMXHyMnKy8zNzs/Q0dLU1dbX2Nnb3N3e3+Dh4uXm5+nq6+zt7/Dx8vP09fb4+fr7/P3+DraYyQAABp9JREFUeAGslv1bFccVx/c7u4sXtFq9FamtQtUqvthaa2tbKi9oq+CjtmJiSDRqYogv+CKJGoOaaF6EoNEIxhjBiyiSGPAFvHd35vxz2dm9O7vzZBfM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";
		 return str.getBytes();
		}
	    
		
		@Override
		public String getDatasourceShortDescription(){
			return "Twilio Connector";
		}

		@Override
		public String getDatasourceLongDescription(){
			return "Twilio is a cloud communications Infrastructure as a Service (IaaS). It provides an API for making and receiving telephone calls, and sending and receiving text messages.r";
		}
}