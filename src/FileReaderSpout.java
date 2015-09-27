
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class FileReaderSpout implements IRichSpout {
    private SpoutOutputCollector _collector;
    private TopologyContext context;
    private FileReader fileReader;
    private BufferedReader bufferedReader;


    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {

        String fileName = (String) conf.get("fileName");
        if (null != fileName) {

            try {
                fileReader = new FileReader(fileName);
                bufferedReader = new BufferedReader(fileReader);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }

        this.context = context;
        this._collector = collector;
    }

    @Override
    public void nextTuple() {

        Utils.sleep(100);
        try {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                _collector.emit(new Values(line));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        Utils.sleep(100);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("word"));

    }

    @Override
    public void close() {
        try {
            if (fileReader != null)
                fileReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object msgId) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
