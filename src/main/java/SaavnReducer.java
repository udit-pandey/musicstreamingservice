import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Takes the scores for a particular song which is to be analysed for a particular date.
 * For every new element, the previous score for the element is multiplied by (1 - damping factor) and
 * then the weight of the new stream is added to this score. To filter out unpopular songs and
 * save some analysis time, we are only writing the song and its total score to output if its
 * total score is greater than unpopular filter value.
 */
public class SaavnReducer extends Reducer<DateAndSongDto,DoubleWritable,Text,DoubleWritable> {
    private static final double dampingFactor = 0.1;
    private static final double unpopularFilter = 9.99999999999999;

    public void reduce(DateAndSongDto dateAndSongDto, Iterable<DoubleWritable> weights, Context context) throws IOException, InterruptedException {
        double songScore = 0;
        for (DoubleWritable weightOfSong : weights) {
            songScore = songScore * (1 - dampingFactor) + weightOfSong.get();
        }
        if (songScore > unpopularFilter) {
            context.write(new Text(dateAndSongDto.getSongId()), new DoubleWritable(songScore));
        }
    }
}
