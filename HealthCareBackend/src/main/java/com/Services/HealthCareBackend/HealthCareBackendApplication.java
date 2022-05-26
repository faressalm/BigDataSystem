package com.Services.HealthCareBackend;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;

@SpringBootApplication
public class HealthCareBackendApplication {

	public static void main(String[] args) {
		SpringApplication.run(HealthCareBackendApplication.class, args);
	}

}
//		SimpleDateFormat sp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//				long t = 1648245602 ;
//		Timestamp ts=new Timestamp((long)t*1000);
////		String date = sp.format( t * 1000);
////		System.out.println(date);
//		Date date=new Date(ts.getTime());
//		Calendar cal = Calendar.getInstance();
//		cal.setTime(date);                           // set cal to date
//		cal.set(Calendar.SECOND, 0);                 // set second in minute
//		cal.set(Calendar.MILLISECOND, 0);            // set millis in second
//		java.util.Date zeroedDate = cal.getTime();
//		System.out.println(ts);
//		ts =new Timestamp(zeroedDate.getTime());
//		System.out.println(ts);
//		System.out.println(ts.getTime());
//		Timestamp ts=new Timestamp((long)start);
////        Date date=new Date(ts.getTime());
//		Timestamp ts2=new Timestamp((long)end);
//		Date date2=new Date(ts2.getTime());
////        System.out.println(date);
//		System.out.println(date2);
//		Date date=new Date(ts.getTime());
//		Calendar cal = Calendar.getInstance();
//		cal.setTime(date);                           // set cal to date
//		cal.set(Calendar.SECOND, 0);                 // set second in minute
//		cal.set(Calendar.MILLISECOND, 0);            // set millis in second
//		java.util.Date zeroedDate = cal.getTime();
//		System.out.println(ts);
//		ts =new Timestamp(zeroedDate.getTime());
//		System.out.println(ts);