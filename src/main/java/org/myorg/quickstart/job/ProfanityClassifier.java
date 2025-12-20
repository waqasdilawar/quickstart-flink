package org.myorg.quickstart.job;

import org.apache.flink.api.common.functions.MapFunction;
import org.myorg.quickstart.model.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class ProfanityClassifier implements MapFunction<MessageEvent, MessageEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(ProfanityClassifier.class);
    private final Set<String> profanities;

    public ProfanityClassifier(Set<String> profanities) {
        this.profanities = profanities;
    }

    @Override
    public MessageEvent map(MessageEvent event) {
        boolean isProfane = containsProfanity(event.getMessageBody(), profanities);
        event.setProfanityType(isProfane ? MessageEvent.ProfanityType.PROFANITY : MessageEvent.ProfanityType.SAFE);
        LOG.debug("Message {} classified as: {} (body: '{}')",
                event.getMessageId(), event.getProfanityType(), event.getMessageBody());
        return event;
    }

    public static boolean containsProfanity(String text, Set<String> profanities) {
        if (text == null || text.isEmpty()) {
            return false;
        }
        String lower = text.toLowerCase();
        for (String badWord : profanities) {
            if (lower.contains(badWord.toLowerCase())) {
                return true;
            }
        }
        return false;
    }
}
