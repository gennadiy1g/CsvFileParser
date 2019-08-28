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

std::wstring BulkLoader::getConnectionString() const
{
    std::map<ConnectionParameterName, std::wstring> connectionParameters;
    for (const auto& param : mConnectionParameters) {
        connectionParameters[param.first] = param.second;
    }
    std::wostringstream connectionString;
    for (const auto& param : connectionParameters) {
        connectionString << int(param.first) << L'=' << param.second << L';';
    }
    return connectionString.str();
}

std::wstring BulkLoader::getTableName(const std::wstring_view table) const
{
    std::wstring tableTrim(table);
    boost::trim(tableTrim);
    if (tableTrim == L"") {
        tableTrim = boost::trim_copy(mInputFile.stem().wstring());
    }
    return tableTrim;
}
