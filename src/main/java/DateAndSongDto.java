import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A custom class to store date and songId for transferring the record
 * between mapper, combiner, partitioner and reducer.
 */
public class DateAndSongDto implements WritableComparable<DateAndSongDto> {
    private IntWritable date;
    private Text songId;

    public DateAndSongDto(){
        date = new IntWritable();
        songId = new Text();
    }

    public DateAndSongDto(int date, String songId) {
        this.songId = new Text(songId);
        this.date = new IntWritable(date);
    }

    public IntWritable getDate() {
        return date;
    }

    public void setDate(IntWritable date) {
        this.date = date;
    }

    public Text getSongId() {
        return songId;
    }

    public void setSongId(Text songId) {
        this.songId = songId;
    }

    public int compareTo(DateAndSongDto dateAndSongDto) {
        int cmp = date.compareTo(dateAndSongDto.getDate());

        if (cmp != 0){
            return cmp;
        }

        return songId.compareTo(dateAndSongDto.getSongId());
    }

    public void write(DataOutput dataOutput) throws IOException {
        date.write(dataOutput);
        songId.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        date.readFields(dataInput);
        songId.readFields(dataInput);
    }
}
