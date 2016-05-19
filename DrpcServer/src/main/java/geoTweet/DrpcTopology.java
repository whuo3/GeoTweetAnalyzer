package drpc;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.LocalDRPC;
import backtype.storm.LocalCluster;

import drpc.bolts.UpdateBolt;

import java.util.Map;


public class DrpcTopology {

  public static void main(String[] args) throws Exception
  {
    // create the topology
    LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("DrpcServer");
    builder.addBolt(new UpdateBolt(), 1);

    LocalDRPC drpc = new LocalDRPC();
    LocalCluster cluster = new LocalCluster();
    Config conf = new Config();
    cluster.submitTopology("DrpcServer", conf, builder.createLocalTopology(drpc));
    System.out.println("Results for 'hello':" + drpc.execute("DrpcServer", "-180&&180&&-85&&85"));

    cluster.shutdown();
    drpc.shutdown();

    //
    // Config conf = new Config();
    //
    // conf.setNumWorkers(3);
    //
    // StormSubmitter.submitTopologyWithProgressBar("DrpcServer", conf, builder.createRemoteTopology());
  }
}
