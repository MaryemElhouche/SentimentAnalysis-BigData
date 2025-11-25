import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SentimentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text sentiment = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // Ignorer l'en-tête
        if (line.startsWith("target")) {
            return;
        }

        String[] cols = line.split(",");

        if(cols.length > 0) {
            String target = cols[0].trim(); // déjà "negatif" ou "positif"

            if(target.equals("negatif") || target.equals("positif")) {
                sentiment.set(target);
                context.write(sentiment, one);
            }
        }
    }
}

