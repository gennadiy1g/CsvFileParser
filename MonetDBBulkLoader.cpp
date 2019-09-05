#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <iomanip>
#include <sstream>
#include <string>

using namespace std::string_literals;

#include "MonetDBBulkLoader.h"
#include "log.h"

MonetDBBulkLoader::MonetDBBulkLoader(const bfs::path& inputFile)
    : BulkLoader(inputFile)
{
}

std::wstring MonetDBBulkLoader::generateCopyIntoCommand(const std::wstring_view table) const
{
    auto escapeSequence = [](wchar_t separator) -> std::wstring {
        switch (separator) {
        case L'\'':
            return L"\\'";
        case L'\n':
            return L"\\n";
        case L'\r':
            return L"\\r";
        case L'\t':
            return L"\\t";
        case L'\v':
            return L"\\v";
        default:
            return std::wstring(1, separator);
        }
    };

    std::wostringstream buf(L"COPY ", std::ios_base::ate);
    buf << mParsingResults.numLines() << L" OFFSET 2 RECORDS INTO " << std::quoted(table)
        << L" FROM " << std::quoted(mInputFile.wstring(), L'\'')
        << L" USING DELIMITERS '" << escapeSequence(mSeparator) << L"','\\n','" << escapeSequence(mQuote) << L"\'  NULL AS \'\' LOCKED BEST EFFORT";
    return buf.str();
}

std::wstring MonetDBBulkLoader::generateCreateTableCommand(const std::wstring_view table) const
{
    std::wostringstream buf(L"CREATE TABLE ", std::ios_base::ate);
    buf << std::quoted(table) << L" (";
    auto firstColumn = true;
    assert(mParsingResults.columns().size() > 0);
    for (const auto& column : mParsingResults.columns()) {
        if (firstColumn) {
            firstColumn = false;
        } else {
            buf << L", ";
        };
        buf << std::quoted(boost::trim_copy(column.name())) << L' ';
        switch (column.type()) {
        case ColumnType::String:
            buf << (column.minLength() != column.maxLength() ? L"VARCHAR(" : L"CHAR(") << column.maxLength() << L')';
            break;
        case ColumnType::Float:
            buf << L"FLOAT(" << column.digitsBeforeDecimalPoint() + column.digitsAfterDecimalPoint() << L")";
            break;
        case ColumnType::Decimal:
            buf << L"DECIMAL(" << column.digitsBeforeDecimalPoint() + column.digitsAfterDecimalPoint() << L',' << column.digitsAfterDecimalPoint() << L')';
            break;
        case ColumnType::Int: {
            auto minValue = column.minValue();
            auto maxValue = column.maxValue();
            if ((-127 <= minValue) && (maxValue <= 127)) {
                buf << L"TINYINT";
            } else if ((-32767 <= minValue) && (maxValue <= 32767)) {
                buf << L"SMALLINT";
            } else if ((-2147483647 <= minValue) && (maxValue <= 2147483647)) {
                buf << L"INT";
            } else {
                buf << L"BIGINT";
            }
        } break;
        case ColumnType::TimeStamp:
            buf << L"TIMESTAMP";
            break;
        case ColumnType::Date:
            buf << L"DATE";
            break;
        case ColumnType::Time:
            buf << L"TIME";
            break;
        case ColumnType::Bool:
            buf << L"BOOL";
            break;
        }
        buf << (column.isNull() ? L" NULL" : L" NOT NULL");
    }
    buf << L')';
    return buf.str();
}

std::wstring MonetDBBulkLoader::generateDropTableCommand(const std::wstring_view table) const
{
    std::wostringstream buf(L"DROP TABLE IF EXISTS ", std::ios_base::ate);
    buf << std::quoted(table);
    return buf.str();
}

NanodbcMonetDBBulkLoader::NanodbcMonetDBBulkLoader(const bfs::path& inputFile)
    : MonetDBBulkLoader(inputFile)
{
    mConnectionParameters = { { ConnectionParameterName::Driver, L"MonetDB ODBC Driver" }, { ConnectionParameterName::Host, L"127.0.0.1" },
        { ConnectionParameterName::Port, L"50000" }, { ConnectionParameterName::User, L"monetdb" }, { ConnectionParameterName::Password, L"monetdb" } };
}

std::wstring NanodbcMonetDBBulkLoader::getConnectionString() const
{
    std::map<ConnectionParameterName, std::wstring> connectionParameters;
    for (const auto& param : mConnectionParameters) {
        connectionParameters[param.first] = param.second;
    }

    auto toString = [](ConnectionParameterName connectionParameterName) {
        switch (connectionParameterName) {
        case ConnectionParameterName::Driver:
            return L"DRIVER";
            break;
        case ConnectionParameterName::Host:
            return L"HOST";
            break;
        case ConnectionParameterName::Port:
            return L"PORT";
            break;
        case ConnectionParameterName::User:
            return L"UID";
            break;
        case ConnectionParameterName::Password:
            return L"PWD";
            break;
        default:
            throw std::logic_error("Unknown instance of enum class ConnectionParameterName!");
            break;
        }
    };

    std::wostringstream connectionString;
    for (const auto& param : connectionParameters) {
        connectionString << toString(param.first) << L'=' << param.second << L';';
    }
    return connectionString.str();
}

std::optional<std::size_t> NanodbcMonetDBBulkLoader::getRejectedRecords(nanodbc::connection& connection) const
{
    nanodbc::result results;
    results = nanodbc::execute(connection, boost::locale::conv::utf_to_utf<char16_t>(generateSelectNumberOfRejectedRecordsCommand()));
    results.next();
    return results.get<std::size_t>(0);
}

std::optional<std::size_t> NanodbcMonetDBBulkLoader::load(std::wstring_view table) const
{
    auto tableTrim { getTableName(table) };
    assert(tableTrim != L"");

    auto& gLogger = GlobalLogger::get();

    auto connectionString = getConnectionString();
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << connectionString << FUNCTION_FILE_LINE;
    nanodbc::connection connection(boost::locale::conv::utf_to_utf<char16_t>(connectionString));

    auto dropCommand { generateDropTableCommand(tableTrim) };
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << dropCommand << FUNCTION_FILE_LINE;
    nanodbc::execute(connection, boost::locale::conv::utf_to_utf<char16_t>(dropCommand));

    auto createCommand { generateCreateTableCommand(tableTrim) };
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << createCommand << FUNCTION_FILE_LINE;
    nanodbc::execute(connection, boost::locale::conv::utf_to_utf<char16_t>(createCommand));

    auto copyCommand { generateCopyIntoCommand(tableTrim) };
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << copyCommand << FUNCTION_FILE_LINE;
    nanodbc::execute(connection, boost::locale::conv::utf_to_utf<char16_t>(copyCommand));

    return getRejectedRecords(connection);
}

MclientMonetDBBulkLoader::MclientMonetDBBulkLoader(const bfs::path& inputFile)
    : MonetDBBulkLoader(inputFile)
{
    mConnectionParameters = { { ConnectionParameterName::Port, L"50000" }, { ConnectionParameterName::User, L"monetdb" },
        { ConnectionParameterName::Password, L"monetdb" } };
}

std::optional<std::size_t> MclientMonetDBBulkLoader::load(std::wstring_view table) const
{
    bfs::path uniquePath = bfs::unique_path();

    bfs::path sqlScriptPath = bfs::temp_directory_path() / bfs::path("mclient-");
    sqlScriptPath += uniquePath;
    sqlScriptPath += ".sql";
    auto& gLogger = GlobalLogger::get();
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << sqlScriptPath << FUNCTION_FILE_LINE;

    return std::nullopt;
}
