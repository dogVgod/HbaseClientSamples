package com.fuck.exmaples.HbaseClientSample;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.HashMap;
import java.util.Map;
import  java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.UUID;

public class Util {
  public static void main(String[] args) {

	  System.out.println(Util.date2num("29/Oct/2014:18:25"));
	  String line = "(www_test_dataset www.test.com 10.75.12.129 29/Oct/2014:18:25 (23 ,570729))";
	  Map<String,String>  map = Util.parseLine2Map(line);
	  System.out.println(map.get("rowkey"));
	  System.out.println(map.get("hits"));
	  System.out.println(map.get("bytes"));
	  //System.out.println(Util.parseLine(line));
  }
  public static String parseLine(String line){
	 
	  Pattern pattern = Pattern.compile("\\(([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s\\((\\d+),(\\d+)\\)\\)");
	     String rowkey = "";
	   //String EXAMPLE_TEST = "(www_dpooltblogeluddtedj2_i i.feed.service.weibo.com 10.75.12.129 29/Oct/2014:18:25 (23,570729))";
	   Matcher matcher = pattern.matcher(line);
	   if(matcher.find()){
		      String cate = matcher.group(1);
		      String domain = matcher.group(2);
		      String sip = matcher.group(3);
		      String minflag = Util.date2num(matcher.group(4));
		      long hits = 0;
		      long bytes = 0;
		      try{
		         hits = Long.parseLong(matcher.group(5));
		      }catch (NumberFormatException nfe){
		        System.out.println("NumberFormatException: " + nfe.getMessage()); 
		      }
		      try{
		            bytes = Long.parseLong(matcher.group(6));
		      }catch (NumberFormatException nfe){
		            System.out.println("NumberFormatException: " + nfe.getMessage()); 
		      }
		        Map map = new HashMap();
		        map.put("cate", cate);
		        map.put("domain", domain);
		        map.put("sip", sip);
		        map.put("minflag", minflag);
		        map.put("hits", hits);
		        map.put("bytes", bytes);
		        rowkey = minflag +"|" +  domain + "|" +  sip +"|"+UUID.randomUUID() ;
	   }else{
		   rowkey = "";
	   }
	   return rowkey.toString();

	
  }
  
  public static Map<String,String> parseLine2Map(String line){
		 
	  Pattern pattern = Pattern.compile("\\(([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s\\((\\d+),(\\d+)\\)\\)");
	     String rowkey = "";
	     Map map = new HashMap();
	   //String EXAMPLE_TEST = "(www_dpooltblogeluddtedj2_i i.feed.service.weibo.com 10.75.12.129 29/Oct/2014:18:25 (23,570729))";
	   Matcher matcher = pattern.matcher(line);
	   if(matcher.find()){
		      String cate = matcher.group(1);
		      String domain = matcher.group(2);
		      String sip = matcher.group(3);
		      String minflag = Util.date2num(matcher.group(4));
		      String hits = matcher.group(5);
		      String bytes = matcher.group(6);
		      /*
		      try{
		         hits = Long.parseLong(matcher.group(5));
		      }catch (NumberFormatException nfe){
		        System.out.println("NumberFormatException: " + nfe.getMessage()); 
		      }
		      try{
		            bytes = Long.parseLong(matcher.group(6));
		      }catch (NumberFormatException nfe){
		            System.out.println("NumberFormatException: " + nfe.getMessage()); 
		      }
		      */
		        rowkey = minflag +"|" +  domain + "|" +  sip +"|"+UUID.randomUUID() ;
		       
		        map.put("rowkey", rowkey);
		        map.put("hits", hits);
		        map.put("bytes", bytes);
		        
	   }else{
		     //rowkey = "";
	   }
	   return map;

	
  }
  public static String date2num(String date){
	  HashMap<String,String> hm = new HashMap<String, String>();
	  hm.put("Jan", "01");
	  hm.put("Feb", "02");
	  hm.put("Mar", "03");
	  hm.put("Apr", "04");
	  hm.put("May", "05");
	  hm.put("Jun", "06");
	  hm.put("Jul", "07");
	  hm.put("Aug", "08");
	  hm.put("Sep", "09");
	  hm.put("Oct", "10");
	  hm.put("Nov", "11");
	  hm.put("Dec", "12");
      String[] tokens = date.split("/");
      String day = tokens[0];
      String month = hm.get(tokens[1]);
      String[] yhm = tokens[2].split(":");
      String year = yhm[0];
      String hour = yhm[1];
      String min = yhm[2];
      return year+month+day+hour+min;   
  } 
}
