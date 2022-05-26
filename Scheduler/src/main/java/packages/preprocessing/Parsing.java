package packages.preprocessing;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import packages.models.message;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.YearMonth;


public class Parsing {
     String root = "./../../" ;
    public   void create(int year){
           File f = new File(root + year ) ;

           if ( !f.exists()){
               f.mkdir() ;
                for(int i = 1 ; i <= 12 ; i++){
                    new File(root + year + "/" + i).mkdir() ;
                }
                for(int i = 1 ; i <= 12 ; i++  ){
                      for(int j = 1; j <= YearMonth.of(year , i).lengthOfMonth(); j++)
                            new File(root + year + "/" + i  + "/" + j ).mkdir() ;
                }
           }
    }
    public  void start() {
        String file = "./../../../";
        String json = null;
        String f ;
        for (int k = 0 ; k <= 141 ; k ++) {
            try {
                json = readFileAsString(file + k + ".json");
                String[] obj = json.split("}}");
                for (int i = 0; i < obj.length; i++) {
                    obj[i] += "}}";
                }
                for (int i = 0; i < obj.length; i++) {
                    ObjectMapper mp = new ObjectMapper();
                    mp.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
                    message ms = mp.readValue(obj[i], message.class);

                    SimpleDateFormat sp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    String date = sp.format(ms.Timestamp * 1000);
                    int year = Integer.parseInt(date.substring(0, 4));
                    int month = Integer.parseInt(date.substring(5, 7));
                    int day = Integer.parseInt(date.substring(8, 10));
                    int hours = Integer.parseInt(date.substring(11, 13));
                    create(year);
                    f = root + year + "/" + month + "/" + day + "/" + hours + ".json";
                    File w = new File(f);
                    w.createNewFile();
                    writeOutput(w, obj[i]);
                }

            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    public static void writeOutput(File path, String json) throws IOException {
        FileWriter writer = new FileWriter(path , true) ;
        writer.write(json);
        writer.write("\n"); ;
        writer.close();
    }
    public static String readFileAsString(String file)throws Exception
    {
        return new String(Files.readAllBytes(Paths.get(file)));
    }
}
