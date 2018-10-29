import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Dixit Patel
 */

public class TransactionMapper extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Pattern itemPattern = Pattern.compile("^(?:[^,]*,){2}([^,]*)");
    private static Log LOG = LogFactory.getLog(Mapper.class);

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        Matcher itemMatch = itemPattern.matcher(line);
        if(itemMatch.find()){
            word.set(itemMatch.group(1));
            context.write(word, one);
            }
        }


    }
