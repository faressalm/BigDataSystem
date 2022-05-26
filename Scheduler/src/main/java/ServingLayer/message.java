package com.example.demo;

public class message {
    public  String serviceName ;
    public  long Timestamp ;
    public double CPU ;
    public RAM RAM ;
    public Disk Disk ;
    public  message(){
        Disk = new Disk();
        RAM = new RAM() ;
    }
}
