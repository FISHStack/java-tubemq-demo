package com.example.lan;

import org.apache.tubemq.client.config.TubeClientConfig;
import org.apache.tubemq.client.factory.MessageSessionFactory;
import org.apache.tubemq.client.factory.TubeSingleSessionFactory;
import org.apache.tubemq.client.producer.MessageProducer;
import org.apache.tubemq.client.producer.MessageSentResult;
import org.apache.tubemq.corebase.Message;

/**
 * @author yunhorn lyp
 * @date 2020/6/26下午10:53
 */
public class Producer {

  public static void main(String[] args) throws Exception {
    String localHost = "192.168.1.6";
    String masterHostAndPort = "192.168.1.4:8000";
    TubeClientConfig clientConfig = new TubeClientConfig(localHost, masterHostAndPort);
    MessageProducer messageProducer;
    MessageSessionFactory messageSessionFactory;
    messageSessionFactory = new TubeSingleSessionFactory(clientConfig);
    messageProducer = messageSessionFactory.createProducer();

    messageProducer.publish("test");
    for (int i = 0; i < 10; i++) {
      MessageSentResult result =  messageProducer.sendMessage(new Message("test", "hello world!".getBytes()));
      if(result.isSuccess()){
        System.out.println("send message success!");
      }
    }

    System.exit(-1);

  }

}
