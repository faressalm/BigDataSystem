package packages.models;

public class message {
    public  String serviceName ;
    public  long Timestamp ;
    public double CPU ;
    public packages.models.RAM RAM ;
    public packages.models.Disk Disk ;
    public  message(){
        Disk = new Disk();
        RAM = new RAM() ;
    }
}
