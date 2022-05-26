package packages.preprocessing;

import packages.models.Flatten;
import packages.models.message;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;

import  static packages.preprocessing.Parsing.writeOutput;

import static packages.preprocessing.Parsing.readFileAsString;

public class Wrapper {
    public  static String convert(String path){

        String json = null;
        /**
        "./../../3.json"
        **/
        String[] prfx = path.split("/") ;
        int last = prfx.length - 1 ;
         String converted = "converted/" +   prfx[last - 3 ] +"_" + prfx[last - 2] + "_"
                 + prfx[last - 1] + "_" + prfx[last];
        try {
            json = readFileAsString(path);
            String[] obj = json.split("}}\n");
            File w = new File(converted) ;
            w.createNewFile() ;
            for (int i = 0; i < obj.length; i++) {
                obj[i] += "}}";
            }
            for(int i = 0 ; i < obj.length ; i++){
                ObjectMapper mp = new ObjectMapper();
                mp.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
                message ms = mp.readValue(obj[i], message.class);
                ms.Timestamp = ms.Timestamp * 1000 ;
                Flatten flat = new Flatten(ms) ;
                writeOutput(w, mp.writeValueAsString(flat)) ;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return converted;
    }

    public  void main(String[] args) {
        // Ex :
        convert("./../0.json") ;
    }
}
