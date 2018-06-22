import org.apache.zookeeper.common.IOUtils;
import java.io.*;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

//Currently not working class

public class FileWriteToHDFS {

    public static void main(String[] args) throws Exception {

        //String for today's date to be set as the output filename
        DateFormat dateFormat = new SimpleDateFormat("yyMMdd");
        Date date = new Date();
        String todayDate = dateFormat.format(date);
        String fileName = todayDate + ".jar";

        //Source file in the local file system
        String localSrc = "/Users/calprice/JavaCode/Kafka/" + fileName;
        //Destination file in HDFS
        String dst = "hdprd1-r02-edge-02:9057/tree/gvscsde/projects/api_integrations/kafka_data/" + fileName;
        String uri = "hdfs://hdprd1-r02-edge-02:9057/tree/gvscsde/projects/api_integrations/kafka_data";

/**
        //Other sample code from Stack Overflow
        Configuration configuration = new Configuration();
        FileSystem hdfs = FileSystem.get( new URI( "hdfs://hdprd1-r02-edge-02:9057" ), configuration );
        Path file = new Path("hdfs://" + dst);
        //if ( hdfs.exists( file )) { hdfs.delete( file, true ); }
        OutputStream os = hdfs.create(file);
        BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
        br.write("Hello World");
        br.close();
        hdfs.close(); */


        //Input stream for the file in local file system to be written to HDFS
        InputStream in = new BufferedInputStream(new FileInputStream(localSrc));

        //Get configuration of Hadoop system
        Configuration conf = new Configuration();
        System.out.println("Connecting to -- " + conf.get("fs.defaultFS"));

        //Destination file in HDFS
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        OutputStream out = fs.create(new Path("hdfs://" + dst));

        //Copy file from local to HDFS
        IOUtils.copyBytes(in, out, 4096, true);

        System.out.println(dst + " copied to HDFS");
    }
}