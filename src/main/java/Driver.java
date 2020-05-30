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

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Driver class that sets all the job configuration and runs the map reduce job.
 * After the job execution is over, the output is sorted by their score in descending order
 * and saved to a file named after the date for which the trending songs are analysed.
 */
public class Driver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int returnStatus = ToolRunner.run(new Configuration(),new Driver(),args);
        System.exit(returnStatus);
    }

    public int run(String[] args) throws IOException, URISyntaxException {
        Job job = Job.getInstance();
        job.setJobName("Saavn Trending Songs");
        job.setJarByClass(Driver.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapOutputKeyClass(DateAndSongDto.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setMapperClass(SaavnMapper.class);
        job.setReducerClass(SaavnReducer.class);
        job.setPartitionerClass(SaavnPartitioner.class);
        job.setReducerClass(SaavnReducer.class);
        job.setNumReduceTasks(7);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        try {
            int returnStatus = job.waitForCompletion(true) ? 0 : 1;
//            if (returnStatus == 1){
//              TODO:Requires refactoring : For each reduce output- get sorted list and write to a file
//
//                for (int i=0 ; i<=6; i++){
//                    List<SongAndScoreDto> songAndScoreList = HelperMethods.getSortedSongsListByScore(job.getConfiguration(),args[1] + "part-r-0000" + i);
//                    HelperMethods.writeToFile(songAndScoreList,args[1] + Integer.toString(25+i));
//                }
//            }
            return returnStatus;
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return 0;
    }
}
