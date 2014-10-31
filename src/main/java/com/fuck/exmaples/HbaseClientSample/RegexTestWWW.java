package com.fuck.exmaples.HbaseClientSample;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.HashMap;
import java.util.Map;
import  java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParseException;

public class RegexTestWWW {
  public static final String EXAMPLE_TEST = "(a b 10.75.12.129 29/Oct/2014:18:25 (23,570729))";
  public static void main(String[] args) {
    Pattern pattern = Pattern.compile("\\(([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s\\((\\d+),(\\d+)\\)\\)");
    Matcher matcher = pattern.matcher(EXAMPLE_TEST);
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
    if(matcher.find()){
      String cate = matcher.group(1);
      String domain = matcher.group(2);
      String sip = matcher.group(3);
      String minflag = matcher.group(4);
      String[] tokens = minflag.split("/");
      String day = tokens[0];
      String month = hm.get(tokens[1]);
      String[] yhm = tokens[2].split(":");
      String year = yhm[0];
      String hour = yhm[1];
      String min = yhm[2];
      System.out.println(year+month+day+hour+min);
      
      //29/Oct/2014:18:25
      /*
      try {
        final String OLD_FORMAT = "dd/MMM/yyyy:HH:mm";
        final String NEW_FORMAT = "yyyyMMddHHmm";
        SimpleDateFormat sdf = new SimpleDateFormat(OLD_FORMAT);
        Date date = sdf.parse(minflag);
        sdf.applyPattern(NEW_FORMAT);
        minflag = sdf.format(date);
        System.out.println(minflag); //Tue Aug 31 10:20:56 SGT 1982
        }
      catch(ParseException pe) {
            System.out.println("ERROR: could not parse date in string");
        }
        */
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
      System.out.println(cate+"\n"+bytes);
        Map map = new HashMap();
        map.put("cate", cate);
        map.put("domain", domain);
        map.put("sip", sip);
        map.put("minflag", minflag);
        map.put("hits", hits);
        map.put("bytes", bytes);
        for(Object key : map.keySet( )){
            System.out.println("key : " + key.toString() + " value : " +map.get(key));
        }
        String rowkey;
        rowkey = minflag +"|" +  domain + "|" +  sip ;
        System.out.println(rowkey);
    }
    /*
    while (matcher.find()) {
      System.out.print("Start index: " + matcher.start());
      System.out.print(" End index: " + matcher.end() + " ");
      System.out.println(matcher.group(0));
      System.out.println(matcher.group(1));
      System.out.println(matcher.group(2));
      System.out.println(matcher.group(3));
      System.out.println(matcher.group(4));
      System.out.println(matcher.group(5));
      System.out.println(matcher.group(6));
    }
    */
    /*
     if(EXAMPLE_TEST.matches("register=([^&]+)&ip=([^&]+)&pop=([^&]+)&service=([^&]+)&logtime=(.+?)$)")){
      System.out.println("match it");

     }else{

      System.out.println("error pattern");
    }
    */
  }
} 
