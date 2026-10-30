package com.moveinsync.tripupdates.config;

import com.moveinsync.tripupdates.notify.RedisNotificationSubscriber;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Subscribes to notifications in the background so the gateway can start before Redis is reachable. Retries the
 * initial subscription every {@link #RETRY_DELAY}; after that, Lettuce reconnects and re-subscribes by itself.
 */
final class NotificationSubscriberLifecycle implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(NotificationSubscriberLifecycle.class);
    private static final Duration RETRY_DELAY = Duration.ofSeconds(1);

    private final RedisNotificationSubscriber subscriber;
    private final String channel;
    private volatile Thread starter;
    private volatile boolean running;

    NotificationSubscriberLifecycle(RedisNotificationSubscriber subscriber, String channel) {
        this.subscriber = subscriber;
        this.channel = channel;
    }

    @Override
    public void start() {
        running = true;
        starter = Thread.ofVirtual().name("notification-subscriber-start-" + channel).start(() -> {
            boolean warned = false;
            while (running) {
                try {
                    subscriber.start();
                    log.info("Subscribed to event notifications on {}", channel);
                    return;
                } catch (RuntimeException e) {
                    if (!warned) {
                        log.warn("Cannot subscribe to event notifications on {} yet; retrying every {}", channel,
                                RETRY_DELAY);
                        warned = true;
                    }
                }
                try {
                    Thread.sleep(RETRY_DELAY);
                } catch (InterruptedException e) {
                    return;
                }
            }
        });
    }

    @Override
    public void stop() {
        running = false;
        Thread current = starter;
        if (current != null) {
            current.interrupt();
        }
        subscriber.close();
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
