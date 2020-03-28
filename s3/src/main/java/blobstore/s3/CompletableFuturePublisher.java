package blobstore.s3;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Lift a {@link CompletableFuture} to a {@link org.reactivestreams.Publisher}
 */
class CompletableFuturePublisher<T> implements Publisher<T> {

    private final Supplier<CompletableFuture<T>> futureSupplier;

    /**
     * @param futureSupplier The function that supplies the future.
     */
    CompletableFuturePublisher(Supplier<CompletableFuture<T>> futureSupplier) {
        this.futureSupplier = futureSupplier;
    }

    public static <T> Publisher<T> from(CompletableFuture<T> future) {
        return new CompletableFuturePublisher<>(() -> future);
    }

    @Override
    public final void subscribe(Subscriber<? super T> subscriber) {
        Objects.requireNonNull(subscriber, "Subscriber cannot be null");
        subscriber.onSubscribe(new CompletableFutureSubscription(subscriber));
    }

    /**
     * CompletableFuture subscription.
     */
    class CompletableFutureSubscription implements Subscription {
        private final Subscriber<? super T> subscriber;
        private final AtomicBoolean completed = new AtomicBoolean(false);
        private CompletableFuture<T> future; // to allow cancellation

        /**
         * @param subscriber The subscriber
         */
        CompletableFutureSubscription(Subscriber<? super T> subscriber) {
            this.subscriber = subscriber;
        }

        /**
         * @param n Number of elements to request to the upstream
         */
        public synchronized void request(long n) {
            if (n != 0 && !completed.get()) {
                if (n < 0) {
                    IllegalArgumentException ex = new IllegalArgumentException("Cannot request a negative number");
                    subscriber.onError(ex);
                } else {
                    try {
                        CompletableFuture<T> future = futureSupplier.get();
                        if (future == null) {
                            subscriber.onComplete();
                        } else {
                            this.future = future;
                            future.whenComplete((s, throwable) -> {
                                if (completed.compareAndSet(false, true)) {
                                    if (throwable != null) {
                                        subscriber.onError(throwable);
                                    } else {
                                        if (s != null) {
                                            subscriber.onNext(s);
                                        }
                                        subscriber.onComplete();
                                    }
                                }
                            });
                        }
                    } catch (Throwable e) {
                        subscriber.onError(e);
                    }
                }
            }
        }

        /**
         * Request the publisher to stop sending data and clean up resources.
         */
        public synchronized void cancel() {
            if (completed.compareAndSet(false, true)) {
                if (future != null) {
                    future.cancel(false);
                }
            }
        }
    }
}