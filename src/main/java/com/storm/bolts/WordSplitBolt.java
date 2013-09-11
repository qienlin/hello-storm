/**
 * 
 */
package com.storm.bolts;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * @author Daniel
 * 
 */
public class WordSplitBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 2176543546817613935L;

	private Integer taskId;

	private String componentId;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.taskId = context.getThisTaskId();
		this.componentId = context.getThisComponentId();
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		String sentence = input.getString(0);
		System.err.println(sentence);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	@Override
	public void cleanup() {
		System.err.println("====WordSplitBolt [" + componentId + "-" + taskId
				+ "]====");
	}
}
