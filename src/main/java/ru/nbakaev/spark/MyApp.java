package ru.nbakaev.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

/**
 * Created by Nikita on 22.08.2016.
 */
public class MyApp implements Serializable {

    public static void main(String[] args) {
        String appName = "spark_java";
        String master = "spark://192.168.1.240:32644";

        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.addJar("F:\\test\\java-test\\target\\java-test-1.0-SNAPSHOT.jar");
        sc.addJar("F:\\test\\java-test\\target\\classes\\lib\\aws-java-sdk-s3-1.10.69.jar");
        sc.addJar("F:\\test\\java-test\\target\\classes\\lib\\hadoop-aws-2.7.3.jar");
        sc.addJar("F:\\test\\java-test\\target\\classes\\lib\\aws-java-sdk-core-1.10.69.jar");

        TaskExecute taskExecute = new TaskExecute();

//        List<Integer> integers = new ArrayList<Integer>();
//        Random random = new Random();
//        for (int i=0; i < 1_000; i++){
//            integers.add(random.nextInt());
//        }
//
//        int size = taskExecute.process(sc, integers);
//        System.out.println(size);
//
//        long size2 = taskExecute.process2(sc, integers);
//        System.out.println(size2);

        taskExecute.process3(sc);
    }

}
