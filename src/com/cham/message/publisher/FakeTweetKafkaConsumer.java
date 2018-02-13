package com.cham.message.publisher;

import com.cham.message.dto.Tweet;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Thread.currentThread;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

// Publisher<T> is injected with a Subscription<T>
public class FakeTweetKafkaConsumer implements Publisher<Tweet>{

    private static final String LOG_MESSAGE_FORMAT = "Publisher >> [%s] %s%n";
    final ExecutorService executor = Executors.newFixedThreadPool(8);

    private QueueSubscription subscription;

    private final CompletableFuture<Void> terminated = new CompletableFuture<>();

    @Override
    public void subscribe(Subscriber<? super Tweet> subscriber) {
        subscription = new QueueSubscription(subscriber, executor);
        subscriber.onSubscribe(subscription);
    }

    public void waitUntilTerminated() throws InterruptedException {
        try {
            terminated.get();
        } catch (ExecutionException e) {
            System.out.println(e);
        }
    }

    private void log(String message, Object... args) {
        String fullMessage = String.format(LOG_MESSAGE_FORMAT, currentThread().getName(), message);
        System.out.printf(fullMessage, args);
    }

    // Subscription<T> is injected with a Subscriber<T>
    private class QueueSubscription implements Subscription {

        private final ExecutorService executor;
        private Subscriber<? super Tweet> subscriber;
        private final AtomicInteger value;
        private AtomicBoolean isCanceled;

        public QueueSubscription(Subscriber<? super Tweet> subscriber, ExecutorService executor) {
            this.subscriber = subscriber;
            this.executor = executor;
            value = new AtomicInteger();
            isCanceled = new AtomicBoolean(false);
        }

        @Override
        public void request(long n) {
            if (isCanceled.get())
                return;
            if (n < 0)
                executor.execute(() -> subscriber.onError(new IllegalArgumentException()));
            else
                publishItems(n);
        }

        @Override
        public void cancel() {
            isCanceled.set(true);
            shutdown();
        }

        private void publishItems(long n) {
            for (int i = 0; i < n; i++) {
                // this is where to put the Kafka consumer code to retreave n messages
                executor.execute(() -> {
                    int incur = value.incrementAndGet();
                    Tweet tweet = new Tweet(incur,"Chaminda W", "I still love Akka Streams");
                    log("publish tweet : [" + incur + "] ");
                    subscriber.onNext(tweet);
                });
            }
        }

        private void shutdown() {
            log("Shut down executor...");
            executor.shutdown();
            newSingleThreadExecutor().submit(() -> {

                log("Shutdown complete.");
                terminated.complete(null);
            });
        }
    }


}
