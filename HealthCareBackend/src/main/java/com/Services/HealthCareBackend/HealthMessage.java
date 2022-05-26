package com.Services.HealthCareBackend;

import java.sql.ResultSet;
import java.sql.SQLException;

public class HealthMessage {
    private final String serviceName ;
    private final double meanCPU;
    private final double meanDisk;
    private final double meanRAM;
    private final double peakTimeCPU;
    private final double peakTimeRAM;
    private final double peakTimeDisk;
    private final double countMessages;
    @Override
    public String toString() {
        return "HealthMessage{" +
                "serviceName='" + serviceName + '\'' +
                ", meanCPU=" + meanCPU +
                ", meanDisk=" + meanDisk +
                ", meanRAM=" + meanRAM +
                ", peakTimeCPU=" + peakTimeCPU +
                ", peakTimeRAM=" + peakTimeRAM +
                ", peakTimeDisk=" + peakTimeDisk +
                ", countMessages=" + countMessages +
                '}';
    }
    public HealthMessage(String serviceName,
                         double meanCPU, double meanDisk,
                         double meanRAM, double peakTimeCPU,
                         double peakTimeRAM, double peakTimeDisk, double countMessages) {
        this.serviceName = serviceName;
        this.meanCPU = meanCPU;
        this.meanDisk = meanDisk;
        this.meanRAM = meanRAM;
        this.peakTimeCPU = peakTimeCPU;
        this.peakTimeRAM = peakTimeRAM;
        this.peakTimeDisk = peakTimeDisk;
        this.countMessages = countMessages;
    }

    public String getServiceName() {
        return serviceName;
    }

    public double getMeanCPU() {
        return meanCPU;
    }

    public double getMeanDisk() {
        return meanDisk;
    }

    public double getMeanRAM() {
        return meanRAM;
    }

    public double getPeakTimeCPU() {
        return peakTimeCPU;
    }

    public double getPeakTimeRAM() {
        return peakTimeRAM;
    }

    public double getPeakTimeDisk() {
        return peakTimeDisk;
    }

    public double getCountMessages() {
        return countMessages;
    }

   static  public  HealthMessage mapToHealthMessage(ResultSet rs) throws SQLException {
        String serviceName = "sevice_" + rs.getString(1);
        double meanCPU = Double.parseDouble(rs.getString(2));
        double meanRAM = Double.parseDouble(rs.getString(3));
        double meanDisk = Double.parseDouble(rs.getString(4));
        int counts = Integer.parseInt(rs.getString(5));
        double peakTimeCPU = Double.parseDouble(rs.getString(6));
        double peakTimeRAM = Double.parseDouble(rs.getString(7));
        double peakTimeDisk= Double.parseDouble(rs.getString(8));
        HealthMessage healthMessage = new HealthMessage(serviceName, meanCPU, meanRAM,meanDisk,peakTimeCPU,peakTimeRAM,peakTimeDisk,counts);
        return healthMessage;
    }
}
