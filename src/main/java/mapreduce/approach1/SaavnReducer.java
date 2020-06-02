package mapreduce.approach1;

import dto.DateAndScoreDto;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.stream.StreamSupport;

/**
 * Takes the scores for a particular song which is to be analysed for a particular date.
 * For every new element, the previous score for the element is multiplied by (1 - damping factor) and
 * then the weight of the new stream is added to this score. To filter out unpopular songs and
 * save some analysis time, we are only writing the song and its total score to output if its
 * total score is greater than unpopular filter value.
 */
public class SaavnReducer extends Reducer<Text, DateAndScoreDto, Text, DoubleWritable> {
    private static final double dampingFactor = 0.1;
    private static final double unpopularFilter = 9.99999999999999;

    public void reduce(Text songId, Iterable<DateAndScoreDto> dateAndScoreList, Context context) throws IOException, InterruptedException {
        double songScore = 0;
        long totalCount = StreamSupport.stream(dateAndScoreList.spliterator(), false).count();

        while (totalCount > 1) {
            songScore = songScore + Math.pow((1 - dampingFactor), totalCount);
            totalCount--;
        }
        songScore++;

        if (songScore > unpopularFilter) {
            context.write(new Text(songId), new DoubleWritable(songScore));
        }
    }
}
