package mapreduce;

import dto.DateAndScoreDto;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

/**
 * Custom partitioner that takes a record to be analysed for a particular date
 * and sends to a particular reducer. The number of reducers have also been set to 7, one reducer
 * per day.
 */
public class SaavnPartitioner extends Partitioner<Text, DateAndScoreDto> implements Configurable {
    private Configuration configuration;
    private Map<Integer, Integer> date = new HashMap<Integer, Integer>();

    public void setConf(Configuration configuration) {
        this.configuration = configuration;
        date.put(25, 0);
        date.put(26, 1);
        date.put(27, 2);
        date.put(28, 3);
        date.put(29, 4);
        date.put(30, 5);
        date.put(31, 6);
    }

    public Configuration getConf() {
        return configuration;
    }

    public int getPartition(Text songId, DateAndScoreDto dateAndScoreDto, int numReduceTasks) {
        return date.get(dateAndScoreDto.getDate().get());
    }
}
