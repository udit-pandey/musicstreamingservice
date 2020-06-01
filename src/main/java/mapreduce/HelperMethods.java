package mapreduce;

import dto.SongAndScoreDto;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Helper class that contains methods to filter out junk records, recent relevant record,
 * sort songs by their score and method to write file to local file system.
 */
public class HelperMethods {
    private static final int slidingWindow = 4;

    public static boolean isJunkRecord(String[] columns){
        boolean isJunkRecord = false;
        if (columns.length >= 5){
            isJunkRecord = isNullOrEmpty(columns[0]) || isNullOrEmpty(columns[1]) || isNullOrEmpty(columns[2])
                    || isNullOrEmpty(columns[3]) || isNullOrEmpty(columns[4]) || columns[4].split("-").length != 3;
        }else{
            isJunkRecord = true;
        }
        return isJunkRecord;
    }

    public static boolean isNullOrEmpty(String str){
        return str == null || str.length() == 0;
    }

    public static boolean isRelevantRecentRecord(String hourOfStreaming, String dateOfStreaming, int dateOfAnalysis){
        int date = Integer.parseInt(dateOfStreaming.split("-")[2]);
        return (dateOfAnalysis - 1) == date;
    }

    public static List<SongAndScoreDto> getSortedSongsListByScore(Configuration conf, String pathOnHdfs) throws IOException {
        Path path=new Path("hdfs://" + pathOnHdfs);
        FileSystem fs = FileSystem.get(conf);
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
        List<SongAndScoreDto> songAndScoreList = new ArrayList<SongAndScoreDto>();
        try {
            String line;
            line=br.readLine();
            while (line != null){
                String songId = line.split("\t")[0];
                double score = Double.parseDouble(line.split("\t")[1]);
                songAndScoreList.add(new SongAndScoreDto(songId,score));
                line = br.readLine();
            }
        } finally {
            br.close();
        }
        Collections.sort(songAndScoreList);
        return songAndScoreList;
    }

    public static void writeToFile(List<SongAndScoreDto> songsList, String date) throws IOException, URISyntaxException {
        Configuration configuration = new Configuration();
        FileSystem hdfs = FileSystem.get( new URI( "hdfs://localhost:8020" ), configuration );
        Path file = new Path("hdfs://localhost:8020/user/ec2-user/"+date+".txt");
        if (hdfs.exists(file)) {
            hdfs.delete(file,true);
        }
        OutputStream os = hdfs.create(file);
        BufferedWriter bw = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
        int cnt = 0;
        for (SongAndScoreDto songAndScore : songsList) {
            bw.write(songAndScore.getSongId() + "\t" + songAndScore.getScore());
            bw.newLine();
            cnt++;
            if(cnt>100){
                break;
            }
        }
        bw.close();
        hdfs.close();
    }

}
