/**
 * 
 */
package com.storm.spouts;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * @author Daniel
 * 
 */
public class LineReaderSpout extends BaseRichSpout {

	private static final long serialVersionUID = -5463846776315846006L;

	private SpoutOutputCollector collector;

	private FileReader fileReader;

	private boolean completed;

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		try {
			this.fileReader = new FileReader(conf.get("file").toString());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		this.collector = collector;
	}

	public void nextTuple() {
		System.err.println("nextTuple");
		if (this.completed) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		String line;
		BufferedReader reader = new BufferedReader(this.fileReader);
		try {
			while ((line = reader.readLine()) != null) {
				if (!line.isEmpty()) {
					this.collector.emit(new Values(line), line);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			this.completed = true;
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}
}
