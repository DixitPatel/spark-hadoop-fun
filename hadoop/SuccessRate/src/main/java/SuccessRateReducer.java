import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * @author Dixit Patel
 */
public class SuccessRateReducer extends Reducer<Text, Text, Text, DoubleWritable> {

    private Text tmp = new Text();
    private Map<String, Double> successMap = new TreeMap<String, Double>();


    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        long clicks =0;
        long buys = 0;
        Iterator val = values.iterator();
        while(val.hasNext()){
            tmp = (Text)val.next();
            if(tmp.charAt(0) == 'A'){
                clicks = (Long.parseLong(tmp.toString().split(",")[1]));
            }
            else if(tmp.charAt(0) == 'B'){
                buys = Long.parseLong(tmp.toString().split(",")[1]);

            }
        }

        if(clicks!=0){
            double success = (buys/(clicks*1.0));
            successMap.put(key.toString(),(success));
            //context.write(key,new DoubleWritable(success));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        Map<String, Double> sortedMap = SuccessRateReducer.sortByValues(successMap);

        int counter = 0;
        for (String key: sortedMap.keySet()) {
            if (counter ++ == 10) {
                break;
            }
            context.write(new Text(key), new DoubleWritable(sortedMap.get(key)));
        }
    }

    public static <K extends Comparable,V extends Comparable> Map<K,V> sortByValues(Map<K,V> map){

        List<Map.Entry<K,V>> entries = new LinkedList<Map.Entry<K,V>>(map.entrySet());

        //Collections.sort(entries,Collections.reverseOrder());
        Collections.sort(entries, new Comparator<Map.Entry<K,V>>() {

            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                //desc sort
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        Map<K,V> sortedMap = new LinkedHashMap<K,V>();

        for(Map.Entry<K,V> entry: entries){
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }


}
