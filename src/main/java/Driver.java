import dto.DateAndScoreDto;
import exception.SaavnAnalyticsException;
import exception.SaavnAnalyticsJobConfigException;
import mapreduce.SaavnMapper;
import mapreduce.SaavnPartitioner;
import mapreduce.SaavnReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Driver class that sets all the job configuration and runs the map reduce job.
 * After the job execution is over, the output is sorted by their score in descending order
 * and saved to a file named after the date for which the trending songs are analysed.
 */
public class Driver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int returnStatus = ToolRunner.run(new Configuration(), new Driver(), args);
        System.exit(returnStatus);
    }

    public int run(String[] args) throws SaavnAnalyticsException, SaavnAnalyticsJobConfigException {
        Job job;
        int jobStatus;

        try {
            job = Job.getInstance();
            job.setJobName("Saavn Trending Songs");
            job.setJarByClass(Driver.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(DateAndScoreDto.class);
            job.setMapperClass(SaavnMapper.class);
            job.setReducerClass(SaavnReducer.class);
            job.setPartitionerClass(SaavnPartitioner.class);
            job.setNumReduceTasks(7);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
        } catch (Exception e) {
            throw new SaavnAnalyticsJobConfigException(e.getMessage());
        }

        try {
            jobStatus = job.waitForCompletion(true) ? 0 : 1;
        } catch (Exception e) {
            throw new SaavnAnalyticsException(e.getMessage());
        }
        return jobStatus;
    }
}
