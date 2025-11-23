package telegram.files;

import org.junit.jupiter.api.Test;
import telegram.files.autodownload.AutomationRuleEngine;
import telegram.files.repository.SettingAutoRecords;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class AutomationRuleEngineTest {

    private final AutomationRuleEngine engine = new AutomationRuleEngine();

    @Test
    void selectsLowestPriorityMatch() {
        SettingAutoRecords.Automation highPriority = automation(1L, 10L, 1);
        SettingAutoRecords.Automation lowPriority = automation(1L, 11L, 5);
        SettingAutoRecords.Automation differentChat = automation(1L, 99L, 0);

        Optional<SettingAutoRecords.Automation> match = engine.selectBestMatch(
                List.of(lowPriority, highPriority, differentChat),
                automation -> automation.telegramId == 1L && automation.chatId != 99L);

        assertTrue(match.isPresent());
        assertEquals(highPriority, match.get(), "Should prefer the lowest priority value");
    }

    @Test
    void returnsEmptyWhenNoPredicateMatches() {
        SettingAutoRecords.Automation automation = automation(2L, 20L, 2);

        Optional<SettingAutoRecords.Automation> match = engine.selectBestMatch(
                List.of(automation),
                auto -> auto.telegramId == 3L);

        assertTrue(match.isEmpty(), "No automation should match the predicate");
    }

    private SettingAutoRecords.Automation automation(long telegramId, long chatId, int priority) {
        SettingAutoRecords.Automation automation = new SettingAutoRecords.Automation();
        automation.telegramId = telegramId;
        automation.chatId = chatId;
        automation.priority = priority;
        automation.download = new SettingAutoRecords.DownloadConfig();
        return automation;
    }
}
