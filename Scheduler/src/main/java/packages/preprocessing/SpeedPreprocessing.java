package packages.preprocessing;

import packages.models.message;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import static packages.preprocessing.Parsing.readFileAsString;

public class SpeedPreprocessing {


    public    String convert(message ms ){
        String converted = "" ;
        converted = converted + ms.serviceName.split("-")[1] + ","+(ms.Timestamp * 1000)+ "," + ms.CPU +
                ","+ ms.RAM.Total+ "," + ms.RAM.Free + ","
        + ms.Disk.Total + ","+ ms.Disk.Free ;
        return converted ;
    }
    public  void start() {
        String file = "./../0.json" ;
        String json = null;
        try {
            json = readFileAsString(file);
            String[] obj = json.split("}}");
            for (int i = 0; i < obj.length; i++) {
                obj[i] += "}}";
            }
            for(int i = 0 ; i < 3 ; i++){
                ObjectMapper mp = new ObjectMapper();
                mp.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
                message ms = mp.readValue(obj[i], message.class);
                System.out.println(obj[i]);
                System.out.println(convert(ms));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
