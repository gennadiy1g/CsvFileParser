#include <boost/algorithm/string.hpp>
#include <boost/locale.hpp>
#include <map>
#include <sstream>

#include "BulkLoader.h"
#include "log.h"

BulkLoader::BulkLoader(const bfs::path& inputFile)
    : mInputFile(inputFile)
{
}

void BulkLoader::parse(wchar_t separator, wchar_t quote)
{
    mSeparator = separator;
    mQuote = quote;
    CsvFileParser parser(mInputFile);
    mParsingResults = parser.parse(separator, quote, L'\\');
}

std::optional<std::size_t> BulkLoader::load(std::wstring_view table) const
{
    auto tableTrim { getTableName(table) };
    assert(tableTrim != L"");

    auto& gLogger = GlobalLogger::get();

    auto connectionString = getConnectionString();
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << connectionString;
    nanodbc::connection connection(boost::locale::conv::utf_to_utf<char16_t>(connectionString));

    auto dropCommand { generateDropTableCommand(tableTrim) };
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << dropCommand;
    nanodbc::execute(connection, boost::locale::conv::utf_to_utf<char16_t>(dropCommand));

    auto createCommand { generateCreateTableCommand(tableTrim) };
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << createCommand;
    nanodbc::execute(connection, boost::locale::conv::utf_to_utf<char16_t>(createCommand));

    auto copyCommand { generateCopyIntoCommand(tableTrim) };
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << copyCommand;
    nanodbc::execute(connection, boost::locale::conv::utf_to_utf<char16_t>(copyCommand));

    return getRejectedRecords(connection);
}

std::wstring BulkLoader::getConnectionString() const
{
    std::map<std::wstring, std::wstring> connectionParameters;
    for (const auto& param : mConnectionParameters) {
        connectionParameters[boost::to_upper_copy(param.first)] = param.second;
    }
    std::wostringstream connectionString;
    for (const auto& param : connectionParameters) {
        connectionString << param.first << L'=' << param.second << L';';
    }
    return connectionString.str();
}

std::optional<std::size_t> BulkLoader::getRejectedRecords(nanodbc::connection& connection) const
{
    return std::nullopt;
};

std::wstring BulkLoader::getTableName(const std::wstring_view table) const
{
    std::wstring tableTrim(table);
    boost::trim(tableTrim);
    if (tableTrim == L"") {
        tableTrim = boost::trim_copy(mInputFile.stem().wstring());
    }
    return tableTrim;
}
