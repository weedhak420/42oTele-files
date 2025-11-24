package telegram.files;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;

public class ConfigValidator {

    private static final Log log = LogFactory.get();

    private ConfigValidator() {
    }

    public static boolean validate(ConfigurationService.ConfigDefinition definition, Object value) {
        if (definition == null) {
            log.error("Unknown configuration definition for value {}", value);
            return false;
        }
        if (value instanceof Number number) {
            if (definition.min() != null && number.doubleValue() < definition.min().doubleValue()) {
                log.error("Configuration {} is below min {} (value: {})", definition.key(), definition.min(), value);
                return false;
            }
            if (definition.max() != null && number.doubleValue() > definition.max().doubleValue()) {
                log.error("Configuration {} is above max {} (value: {})", definition.key(), definition.max(), value);
                return false;
            }
        }
        return true;
    }
}
