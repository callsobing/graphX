package functions;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by yat on 2015/12/7.
 */
public class LongKeyToAnomeKey implements PairFunction<Tuple2<Long,Float>, String, Float> {
    @Override
    public Tuple2<String, Float> call(Tuple2<Long,Float> line) throws Exception {
        Long longKey = line._1();
        Integer n4alt = (int)(longKey & 0xfL);
        Integer n8chr = (int)((line._1() & 0xff0L) >> 4);
        Integer n32pos = (int)((line._1() & 0xffffffff000L) >> 12);
        String alt = "";
        String chr = "";
        String pos = String.format("%9s", n32pos.toString()).replace(' ', '0');

        if (n8chr.equals(23)) {
            chr = "0X";
        } else if (n8chr.equals(24)) {
            chr = "0Y";
        } else if (n8chr.equals(25)) {
            chr = "0M";
        } else {
            chr = String.format("%2s", n8chr.toString()).replace(' ', '0');
        }

        if (n4alt.equals(0)) {
            alt = "A";
        } else if (n4alt.equals(1)) {
            alt = "T";
        } else if (n4alt.equals(2)) {
            alt = "C";
        } else if (n4alt.equals(3)) {
            alt = "G";
        } else {
            alt = "NA";
        }

        String anomeKey = chr.concat("_").concat(pos).concat("_").concat(alt);
        return new Tuple2<>(anomeKey,line._2());
    }
}