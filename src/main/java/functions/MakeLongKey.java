package functions;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by yat on 2015/12/7.
 */
public class MakeLongKey implements PairFunction<String, String, Long> {
    @Override
    public Tuple2<String, Long> call(String line) throws Exception {
        String[] splitKey = line.split("_");
        Integer n8chr = 26;
        Integer n32pos = Integer.parseInt(splitKey[1]);
        Integer n8alt = 4;
        if (splitKey[0].equals("0X")) {
            n8chr = 23;
        } else if (splitKey[0].equals("0Y")) {
            n8chr = 24;
        } else if (splitKey[0].equals("0M")) {
            n8chr = 25;
        } else {
            n8chr = Integer.parseInt(splitKey[0]);
        }

        if (splitKey[2].equals("A")) {
            n8alt = 0;
        } else if (splitKey[2].equals("T")) {
            n8alt = 1;
        } else if (splitKey[2].equals("C")) {
            n8alt = 2;
        } else if (splitKey[2].equals("G")) {
            n8alt = 3;
        } else {
            n8alt = 4;
        }
        long longID = ((((long) n32pos) << 12) | ((((long) n8chr) << 4) | (n8alt & 0xfL)));

        return new Tuple2<>(line,longID);
    }
}