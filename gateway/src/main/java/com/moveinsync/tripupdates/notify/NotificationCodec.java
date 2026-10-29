package com.moveinsync.tripupdates.notify;

import com.moveinsync.tripupdates.domain.DomainValidationException;
import com.moveinsync.tripupdates.domain.EventType;
import com.moveinsync.tripupdates.domain.TopicId;
import java.util.ArrayList;
import java.util.List;

/**
 * Wire form of an {@link EventNotification}: {@code v1|tenant|eventId|TYPE|cabId|topic=seq,topic=seq}.
 * Identifiers cannot contain {@code | , =}, so the format needs no escaping.
 */
public final class NotificationCodec {

    private static final String VERSION = "v1";

    public String encode(EventNotification notification) {
        StringBuilder out = new StringBuilder(VERSION)
                .append('|').append(notification.tenantId())
                .append('|').append(notification.eventId())
                .append('|').append(notification.type().name())
                .append('|').append(notification.cabId())
                .append('|');
        for (int i = 0; i < notification.topics().size(); i++) {
            EventNotification.TopicSeq topic = notification.topics().get(i);
            if (i > 0) {
                out.append(',');
            }
            out.append(topic.topic().name()).append('=').append(topic.seq());
        }
        return out.toString();
    }

    /** @throws DomainValidationException if the message is not a valid notification */
    public EventNotification decode(String message) {
        if (message == null) {
            throw new DomainValidationException("notification", "is required");
        }
        String[] parts = message.split("\\|", -1);
        if (parts.length != 6 || !VERSION.equals(parts[0])) {
            throw new DomainValidationException("notification", "has an unsupported format");
        }
        EventType type;
        try {
            type = EventType.valueOf(parts[3]);
        } catch (IllegalArgumentException e) {
            throw new DomainValidationException("notification.type", "has an unsupported value");
        }
        List<EventNotification.TopicSeq> topics = new ArrayList<>();
        for (String pair : parts[5].split(",", -1)) {
            int separator = pair.lastIndexOf('=');
            if (separator < 0) {
                throw new DomainValidationException("notification.topics", "has an unsupported format");
            }
            long seq;
            try {
                seq = Long.parseLong(pair.substring(separator + 1));
            } catch (NumberFormatException e) {
                throw new DomainValidationException("notification.seq", "must be a number");
            }
            topics.add(new EventNotification.TopicSeq(TopicId.parse(pair.substring(0, separator)), seq));
        }
        return new EventNotification(parts[1], parts[2], type, parts[4], topics);
    }
}
