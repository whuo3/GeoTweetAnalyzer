
package geoTweet.bolts;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import geoTweet.tools.NthLastModifiedTimeTracker;
import geoTweet.tools.GeoSlidingWindow;
import geoTweet.tools.TupleHelpers;
import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import backtype.storm.utils.Time;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;

import geoTweet.tools.Geo;

public class GeoRollingBolt extends BaseRichBolt {

    private Set<HostAndPort>  jedisClusterNodes;
    private JedisCluster      jc;

    private static int geoRollingID = 0;

    private static final Logger LOG = Logger.getLogger(GeoRollingBolt.class);
    private static final int NUM_WINDOW_CHUNKS = 5;
    private static final int DEFAULT_SLIDING_WINDOW_IN_SECONDS = NUM_WINDOW_CHUNKS * 60;
    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = DEFAULT_SLIDING_WINDOW_IN_SECONDS / NUM_WINDOW_CHUNKS;
    private static final String WINDOW_LENGTH_WARNING_TEMPLATE = "Actual window length is %d seconds when it should be %d seconds"
        + " (you can safely ignore this warning during the startup phase)";

    private final GeoSlidingWindow<Object> counter;
    private final int windowLengthInSeconds;
    private final int emitFrequencyInSeconds;
    private OutputCollector collector;
    private NthLastModifiedTimeTracker lastModifiedTracker;
    private int boltID;

    public GeoRollingBolt() {
        this(DEFAULT_SLIDING_WINDOW_IN_SECONDS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public GeoRollingBolt(int windowLengthInSeconds, int emitFrequencyInSeconds) {
        this.windowLengthInSeconds = windowLengthInSeconds;
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;
        counter = new GeoSlidingWindow<Object>(deriveNumWindowChunksFrom(this.windowLengthInSeconds,
            this.emitFrequencyInSeconds));
    }

    private int deriveNumWindowChunksFrom(int windowLengthInSeconds, int windowUpdateFrequencyInSeconds) {
        return windowLengthInSeconds / windowUpdateFrequencyInSeconds;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        lastModifiedTracker = new NthLastModifiedTimeTracker(deriveNumWindowChunksFrom(this.windowLengthInSeconds,
            this.emitFrequencyInSeconds));

        jedisClusterNodes = new HashSet<HostAndPort>();
        jedisClusterNodes.add(new HostAndPort("172.22.152.37", 7000));
        jedisClusterNodes.add(new HostAndPort("172.22.152.37", 7001));
        jedisClusterNodes.add(new HostAndPort("172.22.152.38", 7000));
        jedisClusterNodes.add(new HostAndPort("172.22.152.38", 7001));
        jedisClusterNodes.add(new HostAndPort("172.22.152.39", 7000));
        jedisClusterNodes.add(new HostAndPort("172.22.152.39", 7001));
        jc = new JedisCluster(jedisClusterNodes);

        boltID = geoRollingID++;
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleHelpers.isTickTuple(tuple)) {
            LOG.info("Received tick tuple, triggering emit of current window counts");
            emitCurrentWindowCounts();
            collector.ack(tuple);
        }
        else {
            countObjAndAck(tuple);
        }
    }

    private void emitCurrentWindowCounts() {
        Map<Object, List<Geo>> geoSnapShot = counter.getCountsThenAdvanceWindow();
        int actualWindowLengthInSeconds = lastModifiedTracker.secondsSinceOldestModification();
        lastModifiedTracker.markAsModified();
        if (actualWindowLengthInSeconds != windowLengthInSeconds) {
            LOG.warn(String.format(WINDOW_LENGTH_WARNING_TEMPLATE, actualWindowLengthInSeconds, windowLengthInSeconds));
        }
        JSONObject jsonObj = new JSONObject();
        for(Object obj : geoSnapShot.keySet()) {
          String hashTag = (String)obj;
          JSONArray arr = new JSONArray();
          for(Geo geo : geoSnapShot.get(obj)) {
            JSONArray geoObj = new JSONArray();
            geoObj.add(geo.longitude);
            geoObj.add(geo.latitude);
            arr.add(geoObj);
          }
          jsonObj.put(hashTag, arr);
        }
        jc.set("GeoList" + boltID, jsonObj.toString());
        System.out.println("saved to jedis " + boltID + " " + jsonObj.toString());
    }

    private void countObjAndAck(Tuple tuple) {
        Object obj = tuple.getValue(0);
        String longitude = (String)(tuple.getValue(1));
        String latitude = (String)(tuple.getValue(2));
        counter.incrementCount(obj, longitude, latitude);
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //declarer.declare(new Fields("obj", "count", "actualWindowLengthInSeconds"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }
}
