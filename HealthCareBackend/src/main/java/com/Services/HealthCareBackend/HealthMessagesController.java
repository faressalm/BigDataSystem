package com.Services.HealthCareBackend;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

@RestController
public class HealthMessagesController {
    BatchQuery batchQuery =new BatchQuery();
    @GetMapping("/getAllServices")
    public List<HealthMessage> getHealthMessages(@RequestParam double start,@RequestParam double end){
        System.out.println("request date");
        return batchQuery.getHealthStatistics((long) start,(long)end);
    }

    public void printDate(long time) {
        SimpleDateFormat sp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Timestamp ts = new Timestamp(time);
        String date = sp.format(time);
        System.out.println(date);
    }
}
