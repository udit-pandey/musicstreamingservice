import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * This class carries out the mapper phase of the map reduce job.
 * Here, the junk records(rows having null values) and records that are beyond the sliding window
 * are filtered and not analysed further. Records apart from junk records are written as <<Date,Song>,1.0>
 * where 1.0 is the weight assigned to each song.
 */
public class SaavnMapper extends Mapper<LongWritable,Text,DateAndSongDto,DoubleWritable> {
    private int slidingWindow = 24; //in hours
    private DoubleWritable one = new DoubleWritable(1.0);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split(",");

        if (!HelperMethods.isJunkRecord(columns)) {
            if (HelperMethods.isRelevantRecentRecord(columns[3],columns[4],25)) {
                DateAndSongDto dateAndSongDto = new DateAndSongDto(25, columns[0]);
                context.write(dateAndSongDto, one);
            } else if (HelperMethods.isRelevantRecentRecord(columns[3],columns[4],26)) {
                DateAndSongDto dateAndSongDto = new DateAndSongDto(26, columns[0]);
                context.write(dateAndSongDto, one);
            } else if (HelperMethods.isRelevantRecentRecord(columns[3],columns[4],27)) {
                DateAndSongDto dateAndSongDto = new DateAndSongDto(27, columns[0]);
                context.write(dateAndSongDto, one);
            } else if (HelperMethods.isRelevantRecentRecord(columns[3],columns[4],28)) {
                DateAndSongDto dateAndSongDto = new DateAndSongDto(28, columns[0]);
                context.write(dateAndSongDto, one);
            } else if (HelperMethods.isRelevantRecentRecord(columns[3],columns[4],29)) {
                DateAndSongDto dateAndSongDto = new DateAndSongDto(29, columns[0]);
                context.write(dateAndSongDto, one);
            } else if (HelperMethods.isRelevantRecentRecord(columns[3],columns[4],30)) {
                DateAndSongDto dateAndSongDto = new DateAndSongDto(30, columns[0]);
                context.write(dateAndSongDto, one);
            } else if (HelperMethods.isRelevantRecentRecord(columns[3],columns[4],31)) {
                DateAndSongDto dateAndSongDto = new DateAndSongDto(31, columns[0]);
                context.write(dateAndSongDto, one);
            }
        }
    }
}
