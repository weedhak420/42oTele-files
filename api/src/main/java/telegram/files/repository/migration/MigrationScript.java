package telegram.files.repository.migration;

public record MigrationScript(String version, String description, String[] upSql, String[] downSql) {
}
