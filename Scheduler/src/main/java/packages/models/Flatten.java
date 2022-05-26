package packages.models;


import packages.models.message;

public class Flatten {
    public  int serviceName ;
    public  long Timestamp ;
    public double CPU ;
    public Double RAM_Total ;
    public Double RAM_Free ;
    public Double Disk_Total ;
    public Double Disk_Free ;
    public  Flatten(message ms){
        this.serviceName = Integer.parseInt(ms.serviceName.split("-")[1]);
        this.Timestamp = ms.Timestamp ;
        this.CPU = ms.CPU ;
        this.Disk_Total = ms.Disk.Total ;
        this.Disk_Free = ms.Disk.Free ;
        this.RAM_Total = ms.RAM.Total ;
        this.RAM_Free = ms.RAM.Free ;

    }
}
