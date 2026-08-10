using ChatApp.Performance.Orchestrator.Diagnostics;
using Npgsql;

namespace ChatApp.Performance.Orchestrator.Runtime;

/// <summary>
/// Captures PostgreSQL-native write attribution. Docker block I/O includes checkpoints,
/// WAL, relation files and temporary I/O, so it must not be treated as business bytes.
/// </summary>
internal static class PostgresDiagnosticSampler
{
    public static async Task<Dictionary<string, double>> CaptureMetricsAsync(
        string connectionString,
        CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct).ConfigureAwait(false);

        var metrics = new Dictionary<string, double>(StringComparer.Ordinal);
        await ReadKeyValueMetricsAsync(
            connection,
            """
            SELECT metric, value
            FROM (
                SELECT 'database.xact_commit', xact_commit::double precision FROM pg_stat_database WHERE datname = current_database()
                UNION ALL SELECT 'database.xact_rollback', xact_rollback::double precision FROM pg_stat_database WHERE datname = current_database()
                UNION ALL SELECT 'database.blks_read', blks_read::double precision FROM pg_stat_database WHERE datname = current_database()
                UNION ALL SELECT 'database.blks_hit', blks_hit::double precision FROM pg_stat_database WHERE datname = current_database()
                UNION ALL SELECT 'database.tup_returned', tup_returned::double precision FROM pg_stat_database WHERE datname = current_database()
                UNION ALL SELECT 'database.tup_fetched', tup_fetched::double precision FROM pg_stat_database WHERE datname = current_database()
                UNION ALL SELECT 'database.tup_inserted', tup_inserted::double precision FROM pg_stat_database WHERE datname = current_database()
                UNION ALL SELECT 'database.tup_updated', tup_updated::double precision FROM pg_stat_database WHERE datname = current_database()
                UNION ALL SELECT 'database.tup_deleted', tup_deleted::double precision FROM pg_stat_database WHERE datname = current_database()
                UNION ALL SELECT 'database.temp_files', temp_files::double precision FROM pg_stat_database WHERE datname = current_database()
                UNION ALL SELECT 'database.temp_bytes', temp_bytes::double precision FROM pg_stat_database WHERE datname = current_database()
                UNION ALL SELECT 'database.deadlocks', deadlocks::double precision FROM pg_stat_database WHERE datname = current_database()
                UNION ALL SELECT 'database.blk_read_time_ms', blk_read_time::double precision FROM pg_stat_database WHERE datname = current_database()
                UNION ALL SELECT 'database.blk_write_time_ms', blk_write_time::double precision FROM pg_stat_database WHERE datname = current_database()
                UNION ALL SELECT 'wal.records', wal_records::double precision FROM pg_stat_wal
                UNION ALL SELECT 'wal.full_page_images', wal_fpi::double precision FROM pg_stat_wal
                UNION ALL SELECT 'wal.bytes', wal_bytes::double precision FROM pg_stat_wal
                UNION ALL SELECT 'wal.buffers_full', wal_buffers_full::double precision FROM pg_stat_wal
                UNION ALL SELECT 'wal.writes', wal_write::double precision FROM pg_stat_wal
                UNION ALL SELECT 'wal.syncs', wal_sync::double precision FROM pg_stat_wal
                UNION ALL SELECT 'wal.write_time_ms', wal_write_time::double precision FROM pg_stat_wal
                UNION ALL SELECT 'wal.sync_time_ms', wal_sync_time::double precision FROM pg_stat_wal
                UNION ALL SELECT 'bgwriter.checkpoints_timed', checkpoints_timed::double precision FROM pg_stat_bgwriter
                UNION ALL SELECT 'bgwriter.checkpoints_requested', checkpoints_req::double precision FROM pg_stat_bgwriter
                UNION ALL SELECT 'bgwriter.checkpoint_write_time_ms', checkpoint_write_time::double precision FROM pg_stat_bgwriter
                UNION ALL SELECT 'bgwriter.checkpoint_sync_time_ms', checkpoint_sync_time::double precision FROM pg_stat_bgwriter
                UNION ALL SELECT 'bgwriter.buffers_checkpoint', buffers_checkpoint::double precision FROM pg_stat_bgwriter
                UNION ALL SELECT 'bgwriter.buffers_clean', buffers_clean::double precision FROM pg_stat_bgwriter
                UNION ALL SELECT 'bgwriter.buffers_backend', buffers_backend::double precision FROM pg_stat_bgwriter
                UNION ALL SELECT 'bgwriter.buffers_backend_fsync', buffers_backend_fsync::double precision FROM pg_stat_bgwriter
            ) AS samples(metric, value)
            """,
            metrics,
            ct).ConfigureAwait(false);

        await using (var command = new NpgsqlCommand(
            """
            SELECT
                schemaname,
                relname,
                seq_scan,
                idx_scan,
                n_tup_ins,
                n_tup_upd,
                n_tup_del,
                n_tup_hot_upd,
                n_live_tup,
                n_dead_tup,
                vacuum_count,
                autovacuum_count,
                analyze_count,
                autoanalyze_count,
                pg_total_relation_size(relid),
                pg_relation_size(relid),
                pg_indexes_size(relid)
            FROM pg_stat_user_tables
            WHERE schemaname = 'realtime'
               OR (schemaname = 'public' AND relname IN ('AspNetUsers', 'T_BlockRecords', 'T_UserFriendEntry'))
            ORDER BY schemaname, relname;
            """,
            connection))
        await using (var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                var prefix = $"table.{Normalize(reader.GetString(0))}.{Normalize(reader.GetString(1))}";
                metrics[$"{prefix}.seq_scans"] = reader.GetInt64(2);
                metrics[$"{prefix}.index_scans"] = reader.IsDBNull(3) ? 0 : reader.GetInt64(3);
                metrics[$"{prefix}.tuples_inserted"] = reader.GetInt64(4);
                metrics[$"{prefix}.tuples_updated"] = reader.GetInt64(5);
                metrics[$"{prefix}.tuples_deleted"] = reader.GetInt64(6);
                metrics[$"{prefix}.tuples_hot_updated"] = reader.GetInt64(7);
                metrics[$"{prefix}.live_tuples"] = reader.GetInt64(8);
                metrics[$"{prefix}.dead_tuples"] = reader.GetInt64(9);
                metrics[$"{prefix}.vacuum_count"] = reader.GetInt64(10);
                metrics[$"{prefix}.autovacuum_count"] = reader.GetInt64(11);
                metrics[$"{prefix}.analyze_count"] = reader.GetInt64(12);
                metrics[$"{prefix}.autoanalyze_count"] = reader.GetInt64(13);
                metrics[$"{prefix}.total_bytes"] = reader.GetInt64(14);
                metrics[$"{prefix}.heap_bytes"] = reader.GetInt64(15);
                metrics[$"{prefix}.index_bytes"] = reader.GetInt64(16);
            }
        }

        await using (var command = new NpgsqlCommand(
            """
            SELECT
                schemaname,
                relname,
                indexrelname,
                idx_scan,
                idx_tup_read,
                idx_tup_fetch,
                pg_relation_size(indexrelid)
            FROM pg_stat_user_indexes
            WHERE schemaname = 'realtime'
               OR (schemaname = 'public' AND relname IN ('AspNetUsers', 'T_BlockRecords', 'T_UserFriendEntry'))
            ORDER BY schemaname, relname, indexrelname;
            """,
            connection))
        await using (var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                var prefix =
                    $"index.{Normalize(reader.GetString(0))}.{Normalize(reader.GetString(1))}.{Normalize(reader.GetString(2))}";
                metrics[$"{prefix}.scans"] = reader.GetInt64(3);
                metrics[$"{prefix}.tuples_read"] = reader.GetInt64(4);
                metrics[$"{prefix}.tuples_fetched"] = reader.GetInt64(5);
                metrics[$"{prefix}.bytes"] = reader.GetInt64(6);
            }
        }

        return metrics;
    }

    public static async Task ResetStatementStatisticsAsync(
        string connectionString,
        CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            "SELECT pg_stat_statements_reset();",
            connection);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    public static async Task<IReadOnlyList<PostgresStatementSummary>> CaptureTopStatementsAsync(
        string connectionString,
        CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            """
            SELECT
                queryid::text,
                calls,
                total_exec_time,
                rows,
                shared_blks_hit,
                shared_blks_read,
                shared_blks_dirtied,
                shared_blks_written,
                temp_blks_read,
                temp_blks_written,
                wal_records,
                wal_fpi,
                wal_bytes::double precision,
                left(regexp_replace(query, E'\\s+', ' ', 'g'), 500)
            FROM pg_stat_statements
            WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
            ORDER BY wal_bytes DESC NULLS LAST, total_exec_time DESC
            LIMIT 50;
            """,
            connection);

        var statements = new List<PostgresStatementSummary>();
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            statements.Add(new PostgresStatementSummary
            {
                QueryId = reader.GetString(0),
                Calls = reader.GetInt64(1),
                TotalExecutionMilliseconds = reader.GetDouble(2),
                Rows = reader.GetInt64(3),
                SharedBlocksHit = reader.GetInt64(4),
                SharedBlocksRead = reader.GetInt64(5),
                SharedBlocksDirtied = reader.GetInt64(6),
                SharedBlocksWritten = reader.GetInt64(7),
                TempBlocksRead = reader.GetInt64(8),
                TempBlocksWritten = reader.GetInt64(9),
                WalRecords = reader.GetInt64(10),
                WalFullPageImages = reader.GetInt64(11),
                WalBytes = reader.GetDouble(12),
                Query = reader.GetString(13)
            });
        }

        return statements;
    }

    private static async Task ReadKeyValueMetricsAsync(
        NpgsqlConnection connection,
        string sql,
        Dictionary<string, double> destination,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
            destination[reader.GetString(0)] = reader.GetDouble(1);
    }

    private static string Normalize(string value) =>
        value.Replace('.', '_').Replace(' ', '_').ToLowerInvariant();
}
