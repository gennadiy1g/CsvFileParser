#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/process.hpp>
#include <iomanip>
#include <sstream>
#include <string>

#include "MonetDBBulkLoader.h"
#include "log.h"

using namespace std::string_literals;

namespace bp = boost::process;

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
        { ConnectionParameterName::Port, L"50000" }, { ConnectionParameterName::User, DefaultUserPassword }, { ConnectionParameterName::Password, DefaultUserPassword } };
}

std::wstring NanodbcMonetDBBulkLoader::getConnectionString() const
{
    // Overwrite default connection parameters, which are added by the constructor at the beginning of the vector mConnectionParameters,
    // with user specified connection parameters, which are added by the main function at the end of the vector mConnectionParameters.
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
    mConnectionParameters = { { ConnectionParameterName::Host, L"127.0.0.1" }, { ConnectionParameterName::Port, L"50000" },
        { ConnectionParameterName::User, DefaultUserPassword }, { ConnectionParameterName::Password, DefaultUserPassword } };
}

std::optional<std::size_t> MclientMonetDBBulkLoader::load(std::wstring_view table) const
{
    bfs::path uniquePath { bfs::unique_path() };

    // Write SQL commands into a script file

    bfs::path sqlScript { bfs::temp_directory_path() / bfs::path("mclient-") };
    sqlScript += uniquePath;
    sqlScript.replace_extension("sql");
    auto& gLogger = GlobalLogger::get();
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << sqlScript << FUNCTION_FILE_LINE;
    auto tableTrim { getTableName(table) };
    assert(tableTrim != L"");
    {
        bfs::wofstream fileStream(sqlScript);
        fileStream << generateDropTableCommand(tableTrim) << L";\n";
        fileStream << generateCreateTableCommand(tableTrim) << L";\n";
        fileStream << generateCopyIntoCommand(tableTrim) << L";\n";
        fileStream << generateSelectNumberOfRejectedRecordsCommand() << L";\n";
    }

    // Overwrite default connection parameters, which are added by the constructor at the beginning of the vector mConnectionParameters,
    // with user specified connection parameters, which are added by the main function at the end of the vector mConnectionParameters.

    std::map<ConnectionParameterName, std::wstring> connectionParameters;
    for (const auto& param : mConnectionParameters) {
        connectionParameters[param.first] = param.second;
    }

    std::string dotMonetdbFile { "C:\\Program Files\\MonetDB\\MonetDB5\\etc\\.monetdb" }; // TODO: take from a config file
    if (!bfs::exists(dotMonetdbFile)) {
        throw std::runtime_error("File \""s + dotMonetdbFile + "\" does not exist"s);
    }

    // If user name and password are not default, write them into a custom .monetdb file

    auto user = connectionParameters.at(ConnectionParameterName::User);
    auto password = connectionParameters.at(ConnectionParameterName::Password);
    bfs::path custDotMonetdbFile;
    if (!((user == DefaultUserPassword) && (password == DefaultUserPassword))) {
        custDotMonetdbFile = bfs::temp_directory_path() / bfs::path(".monetdb-");
        custDotMonetdbFile += uniquePath;
        BOOST_LOG_SEV(gLogger, bltrivial::trace) << custDotMonetdbFile << FUNCTION_FILE_LINE;
        {
            bfs::wofstream fileStream(custDotMonetdbFile);
            fileStream << L"user=" << user << L'\n';
            fileStream << L"password=" << password << L'\n';
        }
        dotMonetdbFile = blocale::conv::utf_to_utf<char>(custDotMonetdbFile.native());
    }

    // Generate command to run mclient

    std::string mclientExecutable { "C:\\Program Files\\MonetDB\\MonetDB5\\mclient.bat" }; // TODO: take from a config file
    if (!bfs::exists(mclientExecutable)) {
        throw std::runtime_error("File \""s + mclientExecutable + "\" does not exist"s);
    }
    std::ostringstream mclientCommand;
    mclientCommand << '"' << mclientExecutable << '"';
    mclientCommand << " --host=" << blocale::conv::utf_to_utf<char>(connectionParameters.at(ConnectionParameterName::Host));
    mclientCommand << " --port=" << blocale::conv::utf_to_utf<char>(connectionParameters.at(ConnectionParameterName::Port));
    mclientCommand << " --format=csv " << sqlScript;
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << mclientCommand.str() << FUNCTION_FILE_LINE;

    // Run mclient

    bp::ipstream pipeStream;
    std::vector<std::string> lines;
    std::string line;

    bp::child mclientProcess(mclientCommand.str(), bp::env["DOTMONETDBFILE"] = dotMonetdbFile, bp::std_out > pipeStream);

    while (mclientProcess.running() && std::getline(pipeStream, line)) {
        lines.push_back(boost::trim_copy(line));
    }

    mclientProcess.wait();
    int mclientExitCode = mclientProcess.exit_code();
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << mclientExitCode << FUNCTION_FILE_LINE;

    for (auto line_ : lines) {
        BOOST_LOG_SEV(gLogger, bltrivial::trace) << line_ << FUNCTION_FILE_LINE;
    }

    if (mclientExitCode) {
        throw std::runtime_error("mclient exit code is not 0");
    }

    std::optional<std::size_t> rejectedRecords; // number of records, rejected by the server
    if (lines.size()) {
        try {
            rejectedRecords = boost::lexical_cast<std::size_t>(lines[lines.size() - 1]);
        } catch (const std::exception& e) {
            throw std::runtime_error("mclient has not returned number of records, rejected by the server");
        }
    } else {
        throw std::runtime_error("mclient has not returned anything");
    }

#ifdef NDEBUG
    // Delete temporary files

    bfs::remove(sqlScript);
    if (bfs::exists(custDotMonetdbFile)) {
        bfs::remove(custDotMonetdbFile);
    }
#endif

    return rejectedRecords;
}

std::wstring MclientMonetDBBulkLoader::generateCopyIntoCommand(const std::wstring_view table) const
{
    return boost::ireplace_first_copy(MonetDBBulkLoader::generateCopyIntoCommand(table), "USING", "ON CLIENT USING");
}
