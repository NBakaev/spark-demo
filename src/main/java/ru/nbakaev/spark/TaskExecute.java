package ru.nbakaev.spark;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.apache.commons.io.IOUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Nikita on 25.08.2016.
 */
public class TaskExecute implements Serializable {

    public int process(JavaSparkContext sc, List<Integer> integers){
        JavaRDD<Integer> javaRDD = sc.parallelize(integers);
        Integer size = javaRDD.reduce((v1, v2) -> v1 + v2);
        return size;
    }

    public long process2(JavaSparkContext sc, List<Integer> integers){
        JavaRDD<Integer> javaRDD = sc.parallelize(integers);
        return javaRDD.filter(x -> x % 2 ==0).count();
    }

    public void process3(JavaSparkContext sc){
        String accessKey = "AKIAct4cXAMPLE";
        String secretKey = "wJalrXUtnFEvt54ghEXAMPLEKEY";
        String endpoint = "192.168.1.240:9000";
        final int WORDS_NUMBER = 100;

        sc.hadoopConfiguration().set("fs.s3a.access.key", accessKey);
        sc.hadoopConfiguration().set("fs.s3a.secret.key", secretKey);
        sc.hadoopConfiguration().set("fs.s3a.endpoint", endpoint);
        sc.hadoopConfiguration().set("fs.s3a.impl", "S3CompatibleClientForHadoop");
        sc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false");

//        sc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
//        JavaRDD<String> rdd = sc.textFile("s3a://spark/diabetes_scale.txt");
//        counts.persist(StorageLevel.MEMORY_AND_DISK());

        JavaRDD<String> rdd = sc.textFile("s3a://spark/book1.txt").union(sc.textFile("s3a://spark/book2.txt"));
        JavaRDD<String> words = rdd.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

        List<Tuple2<Integer, String>> top = words
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((a, b) -> a + b)
                .mapToPair(Tuple2::swap)
                .takeOrdered(WORDS_NUMBER, new WordCountComparator());

        AmazonS3Client s3Client = S3CompatibleClientForHadoop.getS3Client(accessKey, secretKey, endpoint);
        StringBuilder builder = new StringBuilder();

        for (Tuple2<Integer, String> wo : top) {
            builder.append(wo._1).append(" ").append(wo._2).append("\n");
        }

        s3Client.putObject("spark","book-result.txt", IOUtils.toInputStream(builder), new ObjectMetadata());
    }

}
