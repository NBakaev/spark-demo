package ru.nbakaev.spark;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by Nikita on 8/31/2016.
 */
public class WordCountComparator implements Comparator<Tuple2<Integer, String>>, Serializable {

    @Override
    public int compare(Tuple2<Integer, String> o1, Tuple2<Integer, String> o2) {
        return o2._1()-o1._1();
    }

}