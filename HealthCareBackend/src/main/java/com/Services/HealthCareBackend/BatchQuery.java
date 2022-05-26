package com.Services.HealthCareBackend;

import java.io.File;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class BatchQuery {

    public List < HealthMessage > getHealthStatistics(long start, long end) {
        List < HealthMessage > messages = new ArrayList < > ();
        ArrayList < String > arrayList = getBatchPaths(start,end);
        arrayList.add("./../RealTime/*.parquet");
        try {
            Connection conn = DriverManager.getConnection("jdbc:duckdb:");
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(
                    "SELECT T1.id, MAX(T1.mean_cpu), MAX(T1.mean_ram), MAX(T1.mean_disk), MAX(T1.counter),MIN(T2.cpuPeakTimestamp)," +
                            "                    MIN(T3.ramPeakTimestamp), MIN(T4.diskPeakTimestamp)" +
                            "             FROM (" +
                            "               SELECT id, SUM(CPU)/SUM(counter) AS mean_cpu, SUM(RAM)/SUM(counter) AS mean_ram, SUM(Disk)/SUM(counter) AS mean_disk," +
                            "                      SUM(counter) AS counter, MAX(peakCPU) as peakCPU, MAX(peakRAM) as peakRAM, MAX(peakDisk) as peakDisk" +
                            " FROM  read_parquet" + appendDirectoriesNames(arrayList) +
                            " WHERE Timestamp >= " + start + "AND " + "Timestamp <= " + end +
                            "               GROUP BY id" +
                            "             ) AS T1" +
                            "             JOIN (" +
                            "              SELECT id, peakCPU,Timestamp as cpuPeakTimestamp" +
                            " FROM  read_parquet" + appendDirectoriesNames(arrayList) +
                            " WHERE Timestamp >= " + start + "AND " + "Timestamp <= " + end +
                            "             ) AS T2" +
                            "             ON T1.id = T2.id AND T1.peakCPU = T2.peakCPU" +
                            "             JOIN (" +
                            "              SELECT id, peakRAM, Timestamp as  ramPeakTimestamp" +
                            " FROM  read_parquet" + appendDirectoriesNames(arrayList) +
                            " WHERE Timestamp >= " + start + "AND " + "Timestamp <= " + end +
                            "             ) AS T3" +
                            "             ON T1.id = T3.id AND T1.peakRAM = T3.peakRAM" +
                            "             JOIN (" +
                            "              SELECT id, peakDisk,Timestamp as  diskPeakTimestamp" +
                            " FROM  read_parquet" + appendDirectoriesNames(arrayList) +
                            " WHERE Timestamp >= " + start + "AND " + "Timestamp <= " + end +
                            "             ) AS T4" +
                            "             ON T1.id = T4.id AND T1.peakDisk = T4.peakDisk" +
                            "             GROUP BY T1.id"
            );
            while (rs.next()) {
                messages.add(HealthMessage.mapToHealthMessage(rs));
            }
            rs.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return messages;
    }
    private String appendDirectoriesNames(ArrayList < String > list) {
        StringBuilder stringBuilder = new StringBuilder("([");
        stringBuilder.append("'");
        stringBuilder.append(list.get(0) + "'");
        for (int i = 1; i < list.size(); i++) {
            stringBuilder.append(",'");
            stringBuilder.append(list.get(i) + "'");
        }
        stringBuilder.append("])");
        return stringBuilder.toString();
    }
    private  ArrayList<String> getBatchPaths(long start , long end){
        ArrayList<String> paths =  new ArrayList<>() ;
        String root = "./../BatchViews/" ;
        Calendar calEnd = Calendar.getInstance() ;
        Calendar calStart = Calendar.getInstance() ;
        calStart.setTime( new Date(start) );
        Calendar calS = Calendar.getInstance() ;
        calEnd.setTime( new Date(end) );
        calS.setTime(new Date(start));
        long s = start ;
        while( compare(calS , calEnd) != -1 ){
            SimpleDateFormat sp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String date = sp.format(s);
            int y = Integer.parseInt(date.substring(0, 4));
            int m = Integer.parseInt(date.substring(5, 7));
            int d = Integer.parseInt(date.substring(8, 10));
            File f = new File(root  + y + "/" + m + "/" + d ) ;
            if ( !f.exists() || f.list().length == 0 ){

            }
            else if( compare(calS , calStart) == 0 || compare(calS , calEnd) == 0 ){
                paths.add(root  + y + "/" + m + "/" + d + "/parquet/*.parquet" ) ;
            }
            else {
                paths.add(root  + y + "/" + m + "/" + d + "/parquetDay/*.parquet" ) ;
            }
            s += 86400000 ;
            calS.setTime(new Date(s));
        }
        return paths ;
    }
    private    int  compare(Calendar cal1 , Calendar cal2 ){
        if (   cal1.get(Calendar.YEAR) < cal2.get(Calendar.YEAR) ||
                (  cal1.get(Calendar.YEAR) == cal2.get(Calendar.YEAR)  &&
                        cal1.get(Calendar.DAY_OF_YEAR) < cal2.get(Calendar.DAY_OF_YEAR)  ) ){ // first is smaller
            return 1 ;
        }
        else if ( cal1.get(Calendar.YEAR) == cal2.get(Calendar.YEAR)  &&
                cal1.get(Calendar.DAY_OF_YEAR) ==  cal2.get(Calendar.DAY_OF_YEAR)  ) { // equal
            return  0 ;
        }
        return -1 ; // first is bigger
    }
}