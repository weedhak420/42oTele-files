package telegram.files.repository;

import cn.hutool.core.map.MapUtil;
import io.vertx.sqlclient.templates.RowMapper;
import io.vertx.sqlclient.templates.TupleMapper;

public record SchemaVersionRecord(String version, String description, long appliedAt) {

    public static final String SCHEME = """
            CREATE TABLE IF NOT EXISTS schema_version
            (
                version      VARCHAR(64) PRIMARY KEY,
                description  VARCHAR(255),
                applied_at   BIGINT
            )
            """;

    public static class SchemaVersionDefinition implements Definition {
        @Override
        public String getScheme() {
            return SCHEME;
        }
    }

    public static RowMapper<SchemaVersionRecord> ROW_MAPPER = row ->
            new SchemaVersionRecord(row.getString("version"), row.getString("description"), row.getLong("applied_at"));

    public static TupleMapper<SchemaVersionRecord> PARAM_MAPPER = TupleMapper.mapper(r -> MapUtil.ofEntries(
            MapUtil.entry("version", r.version()),
            MapUtil.entry("description", r.description()),
            MapUtil.entry("applied_at", r.appliedAt())
    ));
}
