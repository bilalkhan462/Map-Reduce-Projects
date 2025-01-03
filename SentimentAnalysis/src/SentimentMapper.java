import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SentimentMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable positive = new IntWritable(1);
    private final static IntWritable negative = new IntWritable(-1);
    private final static IntWritable neutral = new IntWritable(0);
    private Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tweetData = line.split(",", 3);

        if (tweetData.length < 3) {
            // Handle incomplete or malformed input data
            return;
        }

        String tweet = tweetData[1];
        String sentiment = tweetData[2].trim();

        IntWritable sentimentValue;
        if (sentiment.equalsIgnoreCase("Positive")) {
            sentimentValue = positive;
        } else if (sentiment.equalsIgnoreCase("Negative")) {
            sentimentValue = negative;
        } else {
            sentimentValue = neutral;
        }

        String[] words = tweet.split(" ");
        for (String str : words) {
            word.set(str.toLowerCase());  // Convert words to lowercase to avoid case-sensitive duplicates
            context.write(word, sentimentValue);
        }
    }
}
