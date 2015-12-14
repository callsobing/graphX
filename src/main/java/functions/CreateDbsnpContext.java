package functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by yat on 2015/12/7.
 */
public class CreateDbsnpContext implements PairFunction<String, String, String> {
    @Override
    public Tuple2<String,String> call(String line) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String[] field = line.split("\\t");
        Map<String, Object> dbsnpMap =
                (Map<String, Object>) mapper.readValue(field[1], HashMap.class).get("dbsnp");
        String rs1 = dbsnpMap.getOrDefault("rs", "0").toString();

        return new Tuple2<>(rs1, field[0]);
    }
}
