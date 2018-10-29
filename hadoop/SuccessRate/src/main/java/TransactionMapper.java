import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

/**
 * @author Dixit Patel
 */
public class TransactionMapper extends Mapper<Object, Text, Text, Text> {

    private Text outkey = new Text();
    private Text outvalue = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        StringTokenizer elements = new StringTokenizer(value.toString());
        ArrayList<String> line = new ArrayList<String>();
        while(elements.hasMoreElements()){
            line.add(elements.nextToken());
        }

        String itemId = line.get(0);
        outkey.set(itemId);
        outvalue.set("B," + Long.parseLong(line.get(1)));
        context.write(outkey, outvalue);
    }
}
