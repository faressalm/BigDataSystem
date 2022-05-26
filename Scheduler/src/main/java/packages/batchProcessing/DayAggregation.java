package packages.batchProcessing;

import java.io.File;
import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.YearMonth;
import java.util.Calendar;
import java.util.HashMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.example.data.Group;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import packages.models.message;


public class DayAggregation {
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

    public  class HealthMapper
            extends Mapper<Object, Text, Text, DoubleWritable>{

        private   DoubleWritable num = new DoubleWritable();
        private Text word = new Text();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String [] obj = value.toString().split("}}") ;
            for(int i = 0 ; i < obj.length;i++){
                obj[i] += "}}";
            }
            for(int i = 0 ; i < obj.length ; i ++ ){
                ObjectMapper mp = new ObjectMapper() ;
                mp.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
                message ms = mp.readValue(obj[i], message.class);
                ms.serviceName = ms.serviceName.replaceAll("-" , "_");
                //
                Timestamp ts = new Timestamp(ms.Timestamp * 1000) ;
                Date date=new Date(ts.getTime());
                Calendar cal = Calendar.getInstance();
                cal.setTime(date);                           // set cal to date
                cal.set(Calendar.SECOND, 0);                 // set second in minute
                cal.set(Calendar.MILLISECOND, 0);            // set millis in second
                java.util.Date zeroedDate = cal.getTime();
                ts =new Timestamp(zeroedDate.getTime());
                ms.Timestamp = ts.getTime();
                word.set(ms.serviceName);
                num.set(1.0);
                context.write(word, num);
                word.set(ms.serviceName + "_CPU");
                num.set(ms.CPU);
                context.write(word , num);
                word.set(ms.serviceName + "_Timestamp");
                num.set(ms.Timestamp);
                context.write(word , num);
                word.set(ms.serviceName + "_RAM");
                num.set( (ms.RAM.Total - ms.RAM.Free) / ms.RAM.Total);
                context.write(word , num);
                word.set(ms.serviceName + "_Disk");
                num.set( (ms.Disk.Total - ms.Disk.Free) / ms.Disk.Total);
                context.write(word , num);
                word.set(ms.serviceName + "_peakCPU");
                num.set(ms.CPU);
                context.write(word , num ) ;
                word.set(ms.serviceName + "_peakDisk");
                num.set( (ms.Disk.Total - ms.Disk.Free) / ms.Disk.Total );
                context.write(word , num ) ;
                word.set(ms.serviceName +"_peakRAM");
                num.set(  (ms.RAM.Total - ms.RAM.Free) / ms.RAM.Total );
                context.write(word , num ) ;
            }
        }
    }
    public   Double aggregate( Text key, Iterable<DoubleWritable> values){
        Double sum = 0.0;
        if( !key.toString().contains("Timestamp")  && !key.toString().contains("peak")  ){
            for (DoubleWritable val : values) {
                sum += val.get();
            }
        }
        else {
            for (DoubleWritable val : values) {
                sum = Math.max(  sum , val.get()  );
            }
        }
        return sum ;
    }
    public  class Combiner
            extends Reducer<Text,DoubleWritable,Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            result.set( aggregate(key , values ));
            context.write(key, result);
        }
    }

    public  class HealthReducer
            extends Reducer<Text,DoubleWritable,Void, GenericRecord> {
        //private DoubleWritable result = new DoubleWritable();
        private HashMap< Integer , GenericRecord > mp = new HashMap<>() ;
        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            Double sum = aggregate(key , values ) ;
            String[] prfx = String.valueOf(key).split("_");
            GenericRecord record ;
            if (!mp.containsKey( Integer.parseInt(prfx[1]) ) ) {
                record = new GenericData.Record(AVRO_SCHEMA);
                record.put("id", Integer.parseInt(prfx[1]) );
                mp.put( Integer.parseInt(prfx[1]) , record);
            } else {
                record = mp.get( Integer.parseInt(prfx[1])  );
            }
            if (prfx.length == 2) {
                record.put("counter", sum);
            } else {
                record.put(prfx[2], sum);
            }
            mp.put( Integer.parseInt(prfx[1]) , record ) ;
            if ( !record.toString().contains("null") ) {
                context.write(null, record);
                mp.remove(Integer.parseInt(prfx[1])  ) ;
            }
        }
    }
    /// Schema
    private	 final Schema AVRO_SCHEMA = new	Schema.Parser().parse(
            "{\n" +
                    "	\"type\":	\"record\",\n" +
                    "	\"name\":	\"testFile\",\n" +
                    "	\"doc\":	\"test records\",\n" +
                    "	\"fields\":\n" +
                    "	[\n" +
                    "			{\"name\": \"id\",	\"type\":	\"int\"},\n"+
                    "			{\"name\": \"counter\",	\"type\":	\"long\"},\n"+
                    "			{\"name\":	\"CPU\", \"type\":	\"double\"},\n"+
                    "			{\"name\":	\"Disk\", \"type\":	\"double\"},\n" +
                    "			{\"name\":	\"RAM\", \"type\":	\"double\"},\n" +
                    "			{\"name\":	\"Timestamp\", \"type\":	\"long\"},\n" +
                    "			{\"name\":	\"peakCPU\", \"type\":	\"double\"},\n" +
                    "			{\"name\":	\"peakDisk\", \"type\":	\"double\"},\n" +
                    "			{\"name\":	\"peakRAM\", \"type\":	\"double\"}\n" +
                    "	]\n"+
                    "}\n");
    public  void runTask(String inputPath) throws Exception {
        Configuration conf = new Configuration();
        /** Create Batch views **/
        Job job = Job.getInstance(conf, "Day Minutes");
        job.setJarByClass(DayAggregation.class);
        job.setMapperClass(HealthMapper.class);
        job.setCombinerClass(Combiner.class);
        job.setReducerClass(HealthReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(Group.class);
        job.setOutputFormatClass(AvroParquetOutputFormat.class);

        // setting schema to be used
        AvroParquetOutputFormat.setSchema(job, AVRO_SCHEMA);
        String output = root + "2025/1/1/parquetDay"  ;
        new File(output).delete() ;
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(output ));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}