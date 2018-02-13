package com.cham.message.processor;

import com.cham.message.dto.Tweet;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Thread.currentThread;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class TweetProcessor implements Flow.Processor<Tweet, Tweet> {

    private static final String LOG_MESSAGE_FORMAT = "TweetProcessor >> [%s] %s%n";

    private Flow.Subscription publisherSubscription;

    final ExecutorService executorService = Executors.newFixedThreadPool(8);

    private QueueSubscription subscription;
    private long numberOfTweets;
    private ConcurrentLinkedQueue<Tweet> tweets;

    private final CompletableFuture<Void> future = new CompletableFuture<>();

    // constructor
    public TweetProcessor() {
        numberOfTweets = 0;
        tweets = new ConcurrentLinkedQueue<>();
    }

    public void setNumberOfTweets(long n) {
        this.numberOfTweets = n;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super Tweet> subscriber) {
        subscription = new QueueSubscription(subscriber, executorService);
        subscriber.onSubscribe(subscription);
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        log("Subscribed...");
        publisherSubscription = subscription;
        requestItems();
    }

    private void requestItems() {
        log("Requesting %d new tweets...", numberOfTweets);
        publisherSubscription.request(numberOfTweets);
    }

    @Override
    public void onNext(Tweet tweet) {
        if (null == tweet)
            throw new NullPointerException();
         tweets.add(tweet);
        log("processing tweet: [" + tweet + "] ");

    }

    @Override
    public void onComplete() {
        log("Complete!");
    }

    @Override
    public void onError(Throwable t) {
        log("Error >> %s", t);
    }

    private class QueueSubscription implements Flow.Subscription {

        private final ExecutorService executor;

        private Flow.Subscriber<? super Tweet> subscriber;

        private AtomicBoolean isCanceled;

        public QueueSubscription(Flow.Subscriber<? super Tweet> subscriber, ExecutorService executor) {
            this.executor = executor;
            this.subscriber = subscriber;
            isCanceled = new AtomicBoolean(false);
        }

        @Override
        public void request(long n) {
            if (isCanceled.get())
                return;
            if (n < 0)
                executor.execute(() -> subscriber.onError(new IllegalArgumentException()));
            else if (tweets.size() > 0)
                publishItems(n);
            else if (tweets.size() == 0) {
                subscriber.onComplete();
            }
        }

        private void publishItems(long n) {

            int remainItems = tweets.size();

            if ((remainItems == n) || (remainItems > n)) {
                for (int i = 0; i < n; i++) {
                    executor.execute(() -> {
                        subscriber.onNext(tweets.poll());
                    });
                }
                log("Remaining " + (tweets.size() - n) + " tweets to be published to Subscriber!");
            } else if ((remainItems > 0) && (remainItems < n)) {
                for (int i = 0; i < remainItems; i++) {
                    executor.execute(() -> {
                        subscriber.onNext(tweets.poll());
                    });
                }
                subscriber.onComplete();
            } else {
                log("Processor contains no tweets!");
            }

        }

        @Override
        public void cancel() {
            isCanceled.set(true);
            shutdown();
            publisherSubscription.cancel();
        }

        private void shutdown() {
            log("Shut down executorService...");
            executor.shutdown();
            newSingleThreadExecutor().submit(() -> {
                log("Shutdown complete.");
                future.complete(null);
            });
        }
    }

    private void log(String message, Object... args) {
        String fullMessage = String.format(LOG_MESSAGE_FORMAT, currentThread().getName(), message);
        System.out.printf(fullMessage, args);
    }

}
