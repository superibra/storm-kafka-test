package barac.storm;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class NumberSpout extends BaseRichSpout{
	 /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private SpoutOutputCollector collector;
	    
	    private static int currentNumber = 1;
	public void nextTuple() {
		  collector.emit( new Values( new Integer( currentNumber++ ) ) );
		
	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		 this.collector = collector;
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		 declarer.declare( new Fields( "number" ) );
		
	}
	
	   @Override
	    public void ack(Object id) 
	    {
	    }

	    @Override
	    public void fail(Object id) 
	    {
	    }    

}
