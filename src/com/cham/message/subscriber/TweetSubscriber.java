package com.cham.message.subscriber;

import com.cham.message.dto.Tweet;

import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;

import static java.lang.Thread.currentThread;

public class TweetSubscriber implements Subscriber<Tweet> {

    private static final String LOG_MESSAGE_FORMAT = "Subscriber >> [%s] %s%n";

    private long numberOfTweets = 0;

    private Subscription subscription;

    private long tweetCount;

    public void setNumberOfTweets(long n) {
        this.numberOfTweets = n;
        tweetCount = numberOfTweets;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        log("Subscribed..");
        this.subscription = subscription;
        requestItems(numberOfTweets);
    }

    private void requestItems(long n) {
        log("Requesting %d new tweets...", n);
        subscription.request(n);
    }

    @Override
    public void onNext(Tweet item) {
        if (item != null) {
            logTweet(item);
            synchronized (this) {
                tweetCount--;
                if (tweetCount == 0) {
                    log("Cancelling subscription...");
                    subscription.cancel();
                }
            }
        } else {
            log("Null tweet!");
        }
    }

    @Override
    public void onComplete() {
        log("onComplete(): There is no remaining tweets in Processor.");
    }

    @Override
    public void onError(Throwable t) {
        log("Error >> %s", t);
    }

    private void log(String message, Object... args) {
        String fullMessage = String.format(LOG_MESSAGE_FORMAT, currentThread().getName(), message);
        System.out.printf(fullMessage, args);
    }

    private void logTweet(Tweet tweetMessage){
        System.out.println("Tweet received is " + tweetMessage.toString());
    }

}
