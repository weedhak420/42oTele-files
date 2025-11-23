package telegram.files.autodownload;

import telegram.files.repository.SettingAutoRecords;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Simple rule strategy host that can be expanded with richer rule types. The
 * engine currently surfaces the highest priority rule that matches the
 * provided predicate, keeping AutoDownloadVerticle free from rule-selection
 * boilerplate.
 */
public class AutomationRuleEngine {

    /**
     * Returns the highest priority automation that satisfies the supplied matcher.
     * Lower {@code priority} values are considered higher priority.
     *
     * @param automations all configured automations
     * @param matcher     predicate representing the current match criteria
     * @return an optional automation sorted by ascending priority
     */
    public Optional<SettingAutoRecords.Automation> selectBestMatch(List<SettingAutoRecords.Automation> automations,
                                                                   Predicate<SettingAutoRecords.Automation> matcher) {
        return automations.stream()
                .filter(matcher)
                .sorted(Comparator.comparingInt(auto -> auto.priority))
                .findFirst();
    }
}
