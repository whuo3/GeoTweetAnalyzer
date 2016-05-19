package drpc.bolts;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import backtype.storm.topology.BasicOutputCollector;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

import java.util.Comparator;
import java.util.Map;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Collections;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;

public class UpdateBolt extends BaseRichBolt
{
  private static class Node {
    String  hashTag;
    List<double[]> geoList;
    public Node(String hashTag) {
      this.hashTag = hashTag;
      this.geoList = new ArrayList<>();
    }
    public void addGeo(String longitude, String latitude) {
      double[] cur = new double[2];
      cur[0] = Double.parseDouble(longitude);
      cur[1] = Double.parseDouble(latitude);
      geoList.add(cur);
    }
    public void addGeo(double[] longAndLat) {
      double[] cur = new double[2];
      cur[0] = longAndLat[0];
      cur[1] = longAndLat[1];
      geoList.add(cur);
    }
  }
  private static final int  UpdateTimeInterval = 3600;
  private Set<HostAndPort>  jedisClusterNodes;
  private JedisCluster      jc;
  private JedisPool[]       pools;
  private Jedis             jedis;
  private Random            rand;
  private OutputCollector   collector;
  private boolean           isGeo;
  private Map<String,JedisPool> clusterNodes;
  private String            curRank;
  private List<Node>        snapShot;

  private long lastUpdateTimeStamp;

  private JSONParser jsonParser;

  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {
    collector = outputCollector;

    jedisClusterNodes = new HashSet<HostAndPort>();
    jedisClusterNodes.add(new HostAndPort("172.22.152.37", 7000));
    jedisClusterNodes.add(new HostAndPort("172.22.152.37", 7001));
    jedisClusterNodes.add(new HostAndPort("172.22.152.38", 7000));
    jedisClusterNodes.add(new HostAndPort("172.22.152.38", 7001));
    jedisClusterNodes.add(new HostAndPort("172.22.152.39", 7000));
    jedisClusterNodes.add(new HostAndPort("172.22.152.39", 7001));
    jc = new JedisCluster(jedisClusterNodes);

    lastUpdateTimeStamp = System.currentTimeMillis()/1000;

    jsonParser = new JSONParser();

    snapShot = new ArrayList<>();

    getUpdate();
  }
  private void getUpdate() {
    int i = 0;
    snapShot.clear();
    String distributedResult = jc.get("GeoList" + i);
    while(distributedResult != null) {
      snapShot.addAll(parseJSON(distributedResult));
      distributedResult = jc.get("GeoList" + (++i));
    }
  }
  private List<Node> parseJSON(String input) {
    List<Node> result = new ArrayList<>();
    try{
      JSONObject obj = (JSONObject)(jsonParser.parse(input));
      for(Object key : obj.keySet()) {
        String hashTag = (String)key;
        JSONArray jsonArr = (JSONArray)(obj.get(key));
        Node cur = new Node(hashTag);
        for(int i = 0; i < jsonArr.size(); i++) {
          JSONArray latAndLong = (JSONArray)(jsonArr.get(i));
          String longitude = (String)(latAndLong.get(0));
          String latitude = (String)(latAndLong.get(1));
          cur.addGeo(longitude, latitude);
        }
        result.add(cur);
      }
    } catch(Exception exc){
      System.out.println("Exception in parseJSON");
    }
    return result;
  }
  private boolean inRange(double[] longAndLat, double long_low, double long_up, double lat_low, double lat_up) {
    double longitude = longAndLat[0];
    double latitude = longAndLat[1];
    return (latitude >= lat_low) && (latitude <= lat_up) && (longitude >= long_low) && (longitude <= long_up);
  }
  private List<Node> filter(String[] range) {
    double long_low = Double.parseDouble(range[0]);
    double long_up = Double.parseDouble(range[1]);
    double lat_low = Double.parseDouble(range[2]);
    double lat_up = Double.parseDouble(range[3]);
    List<Node> filterResults = new ArrayList<>();
    for(Node nd : snapShot) {
      String hashTag = nd.hashTag;
      boolean atLeastOne = false;
      Node newNd = new Node(hashTag);
      for(double[] longAndLat : nd.geoList) {
        if(inRange(longAndLat, long_low, long_up, lat_low, lat_up)) {
          atLeastOne = true;
          newNd.addGeo(longAndLat);
        }
      }
      if(atLeastOne) {
        filterResults.add(newNd);
      }
    }
    return filterResults;
  }
  private String rank(List<Node> filterResults) {
    Collections.sort(filterResults, new Comparator<Node>(){
      @Override
      public int compare(Node one, Node two) {
        int oneSize = one.geoList.size();
        int twoSize = two.geoList.size();
        if(oneSize == twoSize) {
          return 0;
        }
        return oneSize > twoSize? -1 : 1;
      }
    });
    StringBuilder sb = new StringBuilder();
    for(int i = 0; i < 10 && i < filterResults.size(); i++) {
      sb.append(filterResults.get(i).hashTag + "&&");
    }
    if(sb.length() > 0) {
      sb.delete(sb.length() - 2, sb.length());
    }
    return sb.toString();
  }
  @Override
  public void execute(Tuple tuple) {
      long curTimeStamp = System.currentTimeMillis()/1000;
      if(curTimeStamp - lastUpdateTimeStamp > UpdateTimeInterval) {
        getUpdate();
        lastUpdateTimeStamp = curTimeStamp;
      }
      List<Node> filterResults = filter( ((String)(tuple.getValue(1))).split("&&") );
      String rankResult = rank(filterResults);
      System.out.println("Result: " + rankResult);
      collector.emit(new Values(tuple.getValue(0), rankResult));
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("id", "result"));
  }
}
