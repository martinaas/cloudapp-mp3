import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * a bolt that finds the top n words.
 */
public class TopNFinderBolt extends BaseBasicBolt {
    private HashMap<String, Integer> currentTopWords = new HashMap<String, Integer>();
    private int N;

    private long intervalToReport = 20;
    private long lastReportTime = System.currentTimeMillis();

    public TopNFinderBolt(int N) {
        this.N = N;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String word = tuple.getString(0);
        Integer count = tuple.getInteger(1);
        if (word != null && !word.trim().equals(""))
            currentTopWords.put(word, count);

        if (currentTopWords.size() > N) {

            String keyWithLowestCount = null;
            int min = 0;
            for (Map.Entry entry : currentTopWords.entrySet()) {
                Integer countValue = (Integer) entry.getValue();
                if (min > countValue) {
                    min = countValue;
                    keyWithLowestCount = (String) entry.getKey();
                }
            }

            if (count > min && keyWithLowestCount != null) {
                currentTopWords.remove(keyWithLowestCount);
            } else
                currentTopWords.remove(word);
        }
        //reports the top N words periodically
        if (currentTopWords.size() > 0) {
            if (System.currentTimeMillis() - lastReportTime >= intervalToReport) {
                collector.emit(new Values(printMap()));
                lastReportTime = System.currentTimeMillis();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("top-N"));

    }

    public String printMap() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("top-words = [ ");
        for (String word : currentTopWords.keySet()) {
            stringBuilder.append("(" + word + " , " + currentTopWords.get(word) + ") , ");
        }
        int lastCommaIndex = stringBuilder.lastIndexOf(",");
        stringBuilder.deleteCharAt(lastCommaIndex + 1);
        stringBuilder.deleteCharAt(lastCommaIndex);
        stringBuilder.append("]");
        return stringBuilder.toString();

    }
}
