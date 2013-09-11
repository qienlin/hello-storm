/**
 * 
 */
package com.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

import com.storm.bolts.WordSplitBolt;
import com.storm.spouts.LineReaderSpout;

/**
 * @author Daniel
 * 
 */
public class HelloStorm {

	private static final String FILE = "D:/Workspaces/research/hello_storm/src/main/resources/file.txt";

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("lineReader", new LineReaderSpout());
		builder.setBolt("wordSplitBolt", new WordSplitBolt()).shuffleGrouping(
				"lineReader");

		Config conf = new Config();
		conf.put("file", FILE);
		conf.setDebug(true);
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("topology", conf, builder.createTopology());
	}
}
