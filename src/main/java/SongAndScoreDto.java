/**
 * A custom class to store songId and total score for song. This custom class implements
 * comparable interface and thus makes it easier to sort a list songs based on their score.
 */
public class SongAndScoreDto implements Comparable{
    private String songId;
    private double score;

    public SongAndScoreDto(String songId, double score) {
        this.songId = songId;
        this.score = score;
    }

    public String getSongId() {
        return songId;
    }

    public void setSongId(String songId) {
        this.songId = songId;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public int compareTo(Object o) {
        return Double.compare(((SongAndScoreDto) o).getScore(),this.getScore());
    }
}
