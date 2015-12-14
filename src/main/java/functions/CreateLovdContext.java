package functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yat on 2015/12/7.
 */
public class CreateLovdContext implements PairFunction<String, Long, String> {
    @Override
    public Tuple2<Long,String> call(String line) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String[] field = line.split("\\t");
        Map<String, Object> lovdMap = (Map<String, Object>) mapper.readValue(field[1], HashMap.class);
        String lovdName = lovdMap.keySet().iterator().next();
        Map<String, Object> lovdContextMap = (Map<String, Object>) lovdMap.get(lovdName);

        String keyid = lovdContextMap.getOrDefault("key_id", "0X_0_T").toString();
        String lovdpath = lovdContextMap.getOrDefault("path", "-/-").toString();
        String patho;

        switch (lovdpath) {
            case "+/+":
                patho = "1.0".concat("##").concat(lovdName);
                break;
            case "-/-":
                patho = "-1.0".concat("##").concat(lovdName);
                break;
            default:
                patho = "0.0".concat("##").concat(lovdName);
                break;
        }

        Long longKey = new MakeLongKey().call(keyid)._2();
        return new Tuple2<>(longKey, patho);
    }
}
