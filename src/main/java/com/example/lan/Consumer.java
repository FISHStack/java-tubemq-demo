package com.example.lan;

import java.util.List;
import java.util.concurrent.Executor;
import org.apache.tubemq.client.config.ConsumerConfig;
import org.apache.tubemq.client.config.TubeClientConfig;
import org.apache.tubemq.client.consumer.ConsumerResult;
import org.apache.tubemq.client.consumer.MessageListener;
import org.apache.tubemq.client.consumer.PullMessageConsumer;
import org.apache.tubemq.client.consumer.PushMessageConsumer;
import org.apache.tubemq.client.factory.MessageSessionFactory;
import org.apache.tubemq.client.factory.TubeSingleSessionFactory;
import org.apache.tubemq.client.producer.MessageProducer;
import org.apache.tubemq.corebase.Message;

/**
 * @author yunhorn lyp
 * @date 2020/6/27上午1:00
 */
public class Consumer {

  public static void main(String[] args) throws Exception {
    String localHost = "192.168.1.6";
    String masterHostAndPort = "192.168.1.4:8000";
    ConsumerConfig consumerConfig = new ConsumerConfig(localHost, masterHostAndPort,"group");
    consumerConfig.setConsumeModel(-1);
    MessageSessionFactory messageSessionFactory;
    messageSessionFactory = new TubeSingleSessionFactory(consumerConfig);
    PushMessageConsumer pushMessageConsumer = messageSessionFactory.createPushConsumer(consumerConfig);
    pushMessageConsumer.subscribe("test", null, new MessageListener() {

      public void receiveMessages(List<Message> list) throws InterruptedException {
        for (Message message : list) {
          System.out.println(new String(message.getData()));
        }
      }

      public Executor getExecutor() {
        return null;
      }

      public void stop() {

      }
    });
    pushMessageConsumer.completeSubscribe();

  }

}
