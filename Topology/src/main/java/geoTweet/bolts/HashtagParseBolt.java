package geoTweet.bolts;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import backtype.storm.topology.OutputFieldsDeclarer;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

import geoTweet.tools.Rankings;
import geoTweet.tools.Rankable;

import org.json.simple.JSONObject;

import twitter4j.Status;
import twitter4j.HashtagEntity;

/*
* This class will parse the geoLocation and save the geoLocation based key-value pair to Redis
* input: tweet status
* process: store latitude and longitude as key and tweet text as value to Redis
* output: HashTag which is from geotagged tweets
*/

public class HashtagParseBolt extends BaseRichBolt
{
  private Set<HostAndPort>  jedisClusterNodes;
  private JedisCluster      jc;
  private JedisPool[]       pools;
  private Jedis             jedis;
  private Random            rand;
  private JSONObject        tweetObj;
  private OutputCollector   collector;

  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {
    collector = outputCollector;
    tweetObj = new JSONObject();
    jedisClusterNodes = new HashSet<HostAndPort>();
    jedisClusterNodes.add(new HostAndPort("172.22.152.37", 7000));
    jedisClusterNodes.add(new HostAndPort("172.22.152.37", 7001));
    jedisClusterNodes.add(new HostAndPort("172.22.152.38", 7000));
    jedisClusterNodes.add(new HostAndPort("172.22.152.38", 7001));
    jedisClusterNodes.add(new HostAndPort("172.22.152.39", 7000));
    jedisClusterNodes.add(new HostAndPort("172.22.152.39", 7001));
    jc = new JedisCluster(jedisClusterNodes);
    Map<String,JedisPool> clusterNodes =jc.getClusterNodes();
    pools = new JedisPool[clusterNodes.size()];
    int i = 0;
    for(String key : clusterNodes.keySet()) {
      pools[i++] = clusterNodes.get(key);
    }
    jedis = pools[0].getResource();
  }

  @Override
  public void execute(Tuple tuple)
  {
    Status curStatus = (Status)(tuple.getValue(0));

    // if no tweet is available, wait for 50 ms and return
    if (curStatus == null)
    {
      Utils.sleep(50);
      return;
    }

    HashtagEntity[] arrayHashTags = curStatus.getHashtagEntities();
    StringBuilder serializedHashTags = new StringBuilder();

    for(HashtagEntity ht : arrayHashTags) {
      collector.emit(new Values(ht.getText()));
    }
  }

  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
  {
    outputFieldsDeclarer.declare(new Fields("hashTags"));
  }
}
