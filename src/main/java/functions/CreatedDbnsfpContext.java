package functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yat on 2015/12/7.
 */
public class CreatedDbnsfpContext implements PairFunction<String, String, Long> {
    @Override
    public Tuple2<String, Long> call(String line) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String[] field = line.split("\\t");
        Map<String, Object> innerMap = (Map<String, Object>) mapper.readValue(field[1], HashMap.class).get("dbnsfp");
        String keyid = innerMap.getOrDefault("key_id", "0X_0_T").toString();
        String geneSym = innerMap.getOrDefault("gene_sym", "0").toString();
        Float patho = 0f;

        Long longKey = new MakeLongKey().call(keyid)._2();
        return new Tuple2<>(geneSym, longKey);
    }
}
