package com.cham.message;

import com.cham.message.processor.TweetProcessor;
import com.cham.message.publisher.FakeTweetKafkaConsumer;
import com.cham.message.subscriber.TweetSubscriber;

public class ReactiveStreamsMain {

    public static void main(String args[]) throws InterruptedException {
        FakeTweetKafkaConsumer publisher = new FakeTweetKafkaConsumer();
        TweetSubscriber subscriber = new TweetSubscriber();
        subscriber.setNumberOfTweets(100);
        TweetProcessor processor = new TweetProcessor();
        processor.setNumberOfTweets(100);
        publisher.subscribe(processor);
        processor.subscribe(subscriber);
        publisher.waitUntilTerminated();
    }
}
