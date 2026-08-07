using Npgsql;

namespace ChatApp.Performance.Orchestrator.Runtime;

/// <summary>
/// Creates the minimum identity projection required by Realtime direct-message
/// authorization. This bootstrap is intentionally fail-closed: it only creates
/// tables when all three identity tables are absent, so a benchmark can never
/// seed or alter an existing application identity schema.
/// </summary>
internal sealed class PostgresIdentityBootstrap : IAsyncDisposable
{
    private const string ExistingSchemaSafetyError =
        "TCP chat bootstrap refused to modify PostgreSQL because one or more public identity tables already exist. " +
        "Use an isolated fresh performance database.";

    private readonly string _connectionString;
    private bool _created;

    private PostgresIdentityBootstrap(string connectionString)
    {
        _connectionString = connectionString;
    }

    public int UserCount { get; private init; }
    public int FriendshipCount { get; private init; }

    public static Task<PostgresIdentityBootstrap> CreateAsync(
        string connectionString,
        IReadOnlyList<IReadOnlyList<long>> userPartitions,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentNullException.ThrowIfNull(userPartitions);

        var users = userPartitions
            .SelectMany(static partition => partition)
            .Distinct()
            .ToArray();
        if (users.Length == 0 || users.Any(static userId => userId <= 0))
            throw new ArgumentException("TCP chat bootstrap requires positive user ids.", nameof(userPartitions));
        if (users.Length != userPartitions.Sum(static partition => partition.Count))
        {
            throw new ArgumentException(
                "TCP chat bootstrap requires a distinct user for every healthy connection.",
                nameof(userPartitions));
        }

        var relationships = BuildRingRelationships(userPartitions);
        var bootstrap = new PostgresIdentityBootstrap(connectionString)
        {
            UserCount = users.Length,
            FriendshipCount = relationships.Length,
        };

        return CreateAndSeedAsync(bootstrap, users, relationships, cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        if (!_created)
            return;

        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync().ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            """
            DROP TABLE IF EXISTS public."T_UserFriendEntry";
            DROP TABLE IF EXISTS public."T_BlockRecords";
            DROP TABLE IF EXISTS public."AspNetUsers";
            """,
            connection);
        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
        _created = false;
    }

    /// <summary>
    /// item 五：跨 Gateway 配对的友谊边。每个 sender 用户与另一 Gateway 上的
    /// 目标接收用户直接建立双向 friendship，而不是在单个 Gateway 分区内成环。
    /// </summary>
    public static Task<PostgresIdentityBootstrap> CreateCrossGatewayAsync(
        string connectionString,
        IReadOnlyList<long> users,
        IReadOnlySet<(long UserId, long FriendId)> relationships,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentNullException.ThrowIfNull(users);
        ArgumentNullException.ThrowIfNull(relationships);

        var usersArray = users.ToArray();
        if (usersArray.Length == 0 || usersArray.Any(static userId => userId <= 0))
            throw new ArgumentException("TCP chat bootstrap requires positive user ids.", nameof(users));
        if (usersArray.Distinct().Count() != usersArray.Length)
            throw new ArgumentException("TCP chat bootstrap requires distinct user ids.", nameof(users));
        if (relationships.Count == 0)
            throw new ArgumentException("Cross-gateway bootstrap requires at least one friendship edge.", nameof(relationships));
        if (relationships.Any(static edge => edge.UserId == edge.FriendId))
            throw new ArgumentException("Cross-gateway bootstrap cannot contain a self edge.", nameof(relationships));
        if (relationships.Any(edge =>
                !usersArray.Contains(edge.UserId) || !usersArray.Contains(edge.FriendId)))
        {
            throw new ArgumentException(
                "Cross-gateway bootstrap friendship edges must reference seeded users.",
                nameof(relationships));
        }

        var orderedRelationships = relationships
            .OrderBy(static edge => edge.UserId)
            .ThenBy(static edge => edge.FriendId)
            .ToArray();
        var bootstrap = new PostgresIdentityBootstrap(connectionString)
        {
            UserCount = usersArray.Length,
            FriendshipCount = orderedRelationships.Length,
        };

        return CreateAndSeedAsync(bootstrap, usersArray, orderedRelationships, cancellationToken);
    }

    private static async Task<PostgresIdentityBootstrap> CreateAndSeedAsync(
        PostgresIdentityBootstrap bootstrap,
        long[] users,
        (long UserId, long FriendId)[] relationships,
        CancellationToken cancellationToken)
    {
        try
        {
            await bootstrap.CreateSchemaAndSeedAsync(users, relationships, cancellationToken)
                .ConfigureAwait(false);
            bootstrap._created = true;
            return bootstrap;
        }
        catch
        {
            await bootstrap.TryDropCreatedSchemaAsync().ConfigureAwait(false);
            throw;
        }
    }

    private async Task CreateSchemaAndSeedAsync(
        long[] users,
        (long UserId, long FriendId)[] relationships,
        CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(cancellationToken)
            .ConfigureAwait(false);

        await using (var safetyCheck = new NpgsqlCommand(
                         """
                         SELECT
                             to_regclass('public."AspNetUsers"')::text,
                             to_regclass('public."T_BlockRecords"')::text,
                             to_regclass('public."T_UserFriendEntry"')::text;
                         """,
                         connection,
                         transaction))
        await using (var reader = await safetyCheck
                         .ExecuteReaderAsync(cancellationToken)
                         .ConfigureAwait(false))
        {
            if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                throw new InvalidOperationException("Unable to inspect PostgreSQL identity schema.");
            if (!reader.IsDBNull(0) || !reader.IsDBNull(1) || !reader.IsDBNull(2))
                throw new InvalidOperationException(ExistingSchemaSafetyError);
        }

        await using (var create = new NpgsqlCommand(
                         """
                         CREATE TABLE public."AspNetUsers"
                         (
                             "Id" bigint PRIMARY KEY,
                             "FriendRequestPolicy" smallint NOT NULL DEFAULT 1
                         );

                         CREATE TABLE public."T_BlockRecords"
                         (
                             "BlockerId" bigint NOT NULL,
                             "BlockedUserId" bigint NOT NULL,
                             PRIMARY KEY ("BlockerId", "BlockedUserId")
                         );

                         CREATE TABLE public."T_UserFriendEntry"
                         (
                             "FriendshipId" bigint PRIMARY KEY,
                             "UserId" bigint NOT NULL,
                             "FriendId" bigint NOT NULL,
                             "IsDeleted" boolean NOT NULL DEFAULT FALSE,
                             UNIQUE ("UserId", "FriendId")
                         );
                         """,
                         connection,
                         transaction))
        {
            await create.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await using (var insertUsers = new NpgsqlCommand(
                         """
                         INSERT INTO public."AspNetUsers" ("Id", "FriendRequestPolicy")
                         SELECT user_id, 1::smallint
                         FROM unnest(@user_ids) AS user_id;
                         """,
                         connection,
                         transaction))
        {
            insertUsers.Parameters.AddWithValue("user_ids", users);
            await insertUsers.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        var friendshipIds = new long[relationships.Length];
        var userIds = new long[relationships.Length];
        var friendIds = new long[relationships.Length];
        for (var index = 0; index < relationships.Length; index++)
        {
            friendshipIds[index] = index + 1L;
            userIds[index] = relationships[index].UserId;
            friendIds[index] = relationships[index].FriendId;
        }

        await using (var insertFriendships = new NpgsqlCommand(
                         """
                         INSERT INTO public."T_UserFriendEntry"
                             ("FriendshipId", "UserId", "FriendId", "IsDeleted")
                         SELECT friendship_id, user_id, friend_id, FALSE
                         FROM unnest(@friendship_ids, @user_ids, @friend_ids)
                             AS edge(friendship_id, user_id, friend_id);
                         """,
                         connection,
                         transaction))
        {
            insertFriendships.Parameters.AddWithValue("friendship_ids", friendshipIds);
            insertFriendships.Parameters.AddWithValue("user_ids", userIds);
            insertFriendships.Parameters.AddWithValue("friend_ids", friendIds);
            await insertFriendships.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task TryDropCreatedSchemaAsync()
    {
        if (!_created)
            return;

        try
        {
            await DisposeAsync().ConfigureAwait(false);
        }
        catch
        {
            // Preserve the original initialization exception. Once schema
            // creation commits, _created is set before returning to the caller.
        }
    }

    private static (long UserId, long FriendId)[] BuildRingRelationships(
        IReadOnlyList<IReadOnlyList<long>> partitions)
    {
        var relationships = new HashSet<(long UserId, long FriendId)>();
        foreach (var partition in partitions)
        {
            if (partition.Count < 2)
            {
                throw new ArgumentException(
                    "Every TCP chat load partition requires at least two distinct users.",
                    nameof(partitions));
            }

            for (var index = 0; index < partition.Count; index++)
            {
                var userId = partition[index];
                var friendId = partition[(index + 1) % partition.Count];
                if (userId == friendId)
                    throw new ArgumentException("TCP chat ring cannot contain a self edge.", nameof(partitions));
                relationships.Add((userId, friendId));
                relationships.Add((friendId, userId));
            }
        }

        return relationships
            .OrderBy(static edge => edge.UserId)
            .ThenBy(static edge => edge.FriendId)
            .ToArray();
    }
}
