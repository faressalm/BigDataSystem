package com.tasks.Scheduler;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

@RestController
public class WelcomeController {
    @GetMapping("/welcome")
    public String welcomde(){
        return  "welcome";
    }
}
