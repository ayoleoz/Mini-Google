package edu.upenn.cis.cis455;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;

import org.json.*;

public class Weather {
	
	public static InputStream in;
	public static BufferedReader br;
	public static JSONObject obj;
	public static String temp;
	public static String weather;
	public static String UV;
	public static String langitude;
	public static String longitude;
	public static String elevation;
	
	public static ArrayList<String> getWeather(Double lon, Double lat) {
		ArrayList<String> arrayList = new ArrayList<String>();
		String urlString = "https://api.openweathermap.org/data/2.5/weather?lat=" + lat + "&lon=" + lon + "&appid=6d055e39ee237af35ca066f35474e9df" + ".json";	
		HttpURLConnection conn = null;
		
		try{
			URL httpURL = new URL(urlString);
			
			conn = (HttpURLConnection)httpURL.openConnection();
			
			conn.setConnectTimeout(10000);
			conn.setReadTimeout(10000);
			conn.connect();
			
			in = conn.getInputStream();
			br = new BufferedReader(new InputStreamReader(in));	
			
			StringBuffer sb = new StringBuffer();
			String line = "";
			while((line = br.readLine()) != null){
				sb.append(line);
			}
			
			obj = new JSONObject(sb.toString());

			// TODO: need changes here
			temp = "" + obj.get("temp");
			weather = (String) obj.get("description");
			langitude = (String) obj.get("lat");
			longitude = (String) obj.get("lon");
			
			arrayList.add(temp);
            arrayList.add(weather);
            arrayList.add(langitude);
            arrayList.add(longitude);
		
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return arrayList;
	}
	
	public static void main(String[] args){
		ArrayList<String> arrayList = getWeather(39.952, -75.164);
		for (String string : arrayList){
			System.out.println(string);
		}
	}
}