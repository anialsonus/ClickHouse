#include <Interpreters/BackupsStorage.h>

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/parseQuery.h>

namespace DB
{

BackupsStorage::BackupsStorage(ContextPtr context_, const String & database, const String & table, const String & engine, bool prepare)
    : SystemLogStorage(context_, database, table, engine, prepare)
{
}

void BackupsStorage::update(const BackupOperationInfo & info)
{
    /// Mask all "'" characters in info.error_message, e.g. Disk('backups', '1/') -> Disk(\'backups\', \'1/\')
    String error_message;
    error_message.reserve(info.error_message.capacity());
    for (auto ch : info.error_message)
    {
        if (ch == '\'')
            error_message.push_back('\\');
        error_message.push_back(ch);
    }

    /// Update current operation's entry
    String query =
        "ALTER TABLE " + table_id.getFullTableName() + " UPDATE"
        + " `status` = " + std::to_string(static_cast<Int8>(info.status))
        + ", `error` = '" + error_message + "'"
        + ", `end_time` = " + std::to_string(std::chrono::system_clock::to_time_t(info.end_time))
        + ", `num_files` = " + std::to_string(info.num_files)
        + ", `total_size` = " + std::to_string(info.total_size)
        + ", `num_entries` = " + std::to_string(info.num_entries)
        + ", `uncompressed_size` = " + std::to_string(info.uncompressed_size)
        + ", `compressed_size` = " + std::to_string(info.compressed_size)
        + ", `files_read` = " + std::to_string(info.num_read_files)
        + ", `bytes_read` = " + std::to_string(info.num_read_bytes)
        + " WHERE id = '" + info.id + "'";

    ParserAlterQuery parser;
    ASTPtr ast = parseQuery(
        parser,
        query.data(),
        query.data() + query.size(),
        "ALTER query",
        0,
        DBMS_DEFAULT_MAX_PARSER_DEPTH);

    auto query_context = Context::createCopy(context);
    query_context->makeQueryContext();
    InterpreterAlterQuery{ast, query_context}.execute();
}

}
