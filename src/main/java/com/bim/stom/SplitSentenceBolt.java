package com.bim.stom;

/*
public class SplitSentenceBolt extends BaseBasicBolt {
  
    @Override  
    public void declareOutputFields(OutputFieldsDeclarer declarer) {  
        //定义了传到下一个bolt的字段描述  
        declarer.declare(new Fields("word"));  
    }  
  
    @Override  
    public void execute(Tuple input, BasicOutputCollector collector) {  
        String sentence = input.getStringByField("sentence");  
        String[] words = sentence.split(" ");  
        for (String word : words) {  
            //发送单词  
            collector.emit(new Values(word));  
        }  
    }  
}  */
