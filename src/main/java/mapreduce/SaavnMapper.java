package mapreduce;

import dto.DateAndScoreDto;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * This class carries out the mapper phase of the map reduce job.
 * Here, the junk records(rows having null values) and records that are beyond the sliding window
 * are filtered and not analysed further. Records apart from junk records are written as <<Date,Song>,1.0>
 * where 1.0 is the weight assigned to each song.
 */
public class SaavnMapper extends Mapper<LongWritable, Text, Text, DateAndScoreDto> {
    private double score = 1.0;

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split(",");

        if (!HelperMethods.isJunkRecord(columns)) {
            DateAndScoreDto dateAndScoreDto = null;

            if (HelperMethods.isRelevantRecentRecord(columns[3], columns[4], 25)) {
                dateAndScoreDto = new DateAndScoreDto(25, score);
            } else if (HelperMethods.isRelevantRecentRecord(columns[3], columns[4], 26)) {
                dateAndScoreDto = new DateAndScoreDto(26, score);
            } else if (HelperMethods.isRelevantRecentRecord(columns[3], columns[4], 27)) {
                dateAndScoreDto = new DateAndScoreDto(27, score);
            } else if (HelperMethods.isRelevantRecentRecord(columns[3], columns[4], 28)) {
                dateAndScoreDto = new DateAndScoreDto(28, score);
            } else if (HelperMethods.isRelevantRecentRecord(columns[3], columns[4], 29)) {
                dateAndScoreDto = new DateAndScoreDto(29, score);
            } else if (HelperMethods.isRelevantRecentRecord(columns[3], columns[4], 30)) {
                dateAndScoreDto = new DateAndScoreDto(30, score);
            } else if (HelperMethods.isRelevantRecentRecord(columns[3], columns[4], 31)) {
                dateAndScoreDto = new DateAndScoreDto(31, score);
            }
            context.write(new Text(columns[1]), dateAndScoreDto);
        }
    }
}
