package dto;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A custom class to store date and score for transferring the record
 * between mapper, combiner, partitioner and reducer.
 */
public class DateAndScoreDto implements WritableComparable<DateAndScoreDto> {
    private IntWritable date;
    private DoubleWritable score;

    public DateAndScoreDto() {
        date = new IntWritable();
        score = new DoubleWritable();
    }

    public DateAndScoreDto(int date, double score) {
        this.score = new DoubleWritable(score);
        this.date = new IntWritable(date);
    }

    public IntWritable getDate() {
        return date;
    }

    public void setDate(IntWritable date) {
        this.date = date;
    }

    public DoubleWritable getScore() {
        return score;
    }

    public void setScore(DoubleWritable score) {
        this.score = score;
    }

    public int compareTo(DateAndScoreDto dateAndScoreDto) {
        int cmp = date.compareTo(dateAndScoreDto.getDate());

        if (cmp != 0) {
            return cmp;
        }

        return score.compareTo(dateAndScoreDto.getScore());
    }

    public void write(DataOutput dataOutput) throws IOException {
        date.write(dataOutput);
        score.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        date.readFields(dataInput);
        score.readFields(dataInput);
    }
}
