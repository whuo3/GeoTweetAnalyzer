package geoTweet;

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

import geoTweet.bolts.ReportBolt;
import geoTweet.bolts.RollingCountBolt;
import geoTweet.bolts.GeoRollingBolt;
import geoTweet.bolts.IntermediateRankingsBolt;
import geoTweet.bolts.TotalRankingsBolt;
import geoTweet.bolts.HashtagParseBolt;
import geoTweet.bolts.GeoParsedBolt;

class TweetTopology
{
  private static final int TOP_N = 10;
  private static final int WINDOW_LENGTH_SECONDS = 86400;
  private static final int RESULT_EMIT_FREQUENCY_SECONDS = 5;
  public static void main(String[] args) throws Exception
  {
    // create the topology
    TopologyBuilder builder = new TopologyBuilder();

    // now create the tweet spout with the credentials
    TweetSpout tweetSpout = new TweetSpout(
        "YfEgyAiZ9TBbBUNuQw1rMhXuJ",
        "yegookmFUL0eKZrQHvJxmqG84Xg3TuxUkaWe4rDwcbKOuEUeUH",
        "708122590410182657-OxT9ughYyQ9x87zh0p1GnrMZIa58bxb",
        "5PYbMfvGvWIIdakdiuvU0YV3GkXq5vbbOjZC0rY8Szywv"
    );

    String spoutId = "wordGenerator";
    String hashTagParserId = "hashTagParser";
    String counterId = "counter";
    String intermediateRankerId = "intermediateRanker";
    String totalRankerId = "finalRanker";
    String reportId = "ReportRedis";

    String geoTaggedParser = "geoTaggedParser";
    String geoCounterId = "geoCounter";
    String geoIntermediateRankerId = "geoIntermediateRanker";
    String geoTotalRankerId = "geoFinalRanker";
    String geoReportId = "geoReportRedis";

    builder.setSpout(spoutId, tweetSpout, 1);

    //Global hashTags rank
    builder.setBolt(hashTagParserId, new HashtagParseBolt(), 3).shuffleGrouping(spoutId);
    builder.setBolt(counterId, new RollingCountBolt(WINDOW_LENGTH_SECONDS, RESULT_EMIT_FREQUENCY_SECONDS), 6).fieldsGrouping(hashTagParserId, new Fields("hashTags"));
    builder.setBolt(intermediateRankerId, new IntermediateRankingsBolt(TOP_N), 3).fieldsGrouping(counterId, new Fields("obj"));
    builder.setBolt(totalRankerId, new TotalRankingsBolt(TOP_N, RESULT_EMIT_FREQUENCY_SECONDS)).globalGrouping(intermediateRankerId);
    builder.setBolt(reportId, new ReportBolt(false)).globalGrouping(totalRankerId);


    builder.setBolt(geoTaggedParser, new GeoParsedBolt(), 3).shuffleGrouping(spoutId);
    builder.setBolt(geoCounterId, new GeoRollingBolt(WINDOW_LENGTH_SECONDS, RESULT_EMIT_FREQUENCY_SECONDS), 3).fieldsGrouping(geoTaggedParser, new Fields("hashTags"));
    // builder.setBolt(geoIntermediateRankerId, new IntermediateRankingsBolt(TOP_N), 4).fieldsGrouping(geoCounterId, new Fields("obj"));
    // builder.setBolt(geoTotalRankerId, new TotalRankingsBolt(TOP_N, RESULT_EMIT_FREQUENCY_SECONDS)).globalGrouping(geoIntermediateRankerId);
    // builder.setBolt(geoReportId, new ReportBolt(true)).globalGrouping(geoTotalRankerId);

    // create the default config object
    Config conf = new Config();

    conf.setDebug(true);

    if (args != null && args.length > 0) {

      // run it in a live cluster

      // set the number of workers for running all spout and bolt tasks
      conf.setNumWorkers(3);

      // create the topology and submit with config
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

    } else {

      // run it in a simulated local cluster

      // set the number of threads to run - similar to setting number of workers in live cluster
      conf.setMaxTaskParallelism(2);

      // create the local cluster instance
      LocalCluster cluster = new LocalCluster();

      // submit the topology to the local cluster
      cluster.submitTopology("tweet-word-count", conf, builder.createTopology());

      // let the topology run for 30 seconds. note topologies never terminate!
      //Utils.sleep(30000);

      // now kill the topology
      //cluster.killTopology("tweet-word-count");

      // we are done, so shutdown the local cluster
      //cluster.shutdown();
    }
  }
}
