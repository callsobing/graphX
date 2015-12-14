package functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yat on 2015/12/7.
 */
public class CreateClinvarContext implements PairFunction<String, Long, Float> {
    @Override
    public Tuple2<Long,Float> call(String line) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String[] field = line.split("\\t");
        Map<String, Object> clinvarMap = (Map<String, Object>) mapper.readValue(field[1], HashMap.class).get("clinvar");
        String keyid = clinvarMap.getOrDefault("key_id", "0X_0_T").toString();
        String clnsig = clinvarMap.getOrDefault("clnsig", "0").toString();
        Float patho = 0f;

        if (clnsig.equals("5")) {
            patho = 1f;
        } else if (clnsig.equals("2")) {
            patho = -1f;
        }

        Long longKey = new MakeLongKey().call(keyid)._2();
        return new Tuple2<>(longKey, patho);
    }
}
