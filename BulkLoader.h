#pragma once

#include <boost/filesystem.hpp>
#include <initializer_list>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "CsvFileParser.h"
#include "nanodbc.h"

namespace bfs = boost::filesystem;

using namespace std::literals;

enum class ConnectionParameterName {
    Driver,
    Host,
    Port,
    User,
    Password
};

typedef std::pair<ConnectionParameterName, std::wstring> ConnectionParameter;

class BulkLoader {

public:
    explicit BulkLoader(const bfs::path& inputFile); // Constructor
    virtual ~BulkLoader() = default; // Defaulted virtual destructor

    void parse(wchar_t separator, wchar_t quote);
    virtual std::optional<std::size_t> load(std::wstring_view table = L""sv) const = 0;
    const ParsingResults& parsingResults() const { return mParsingResults; };
    template <typename T>
    void setConnectionParameters(const T& connectionParameters);
    std::wstring getConnectionString() const;
    std::wstring getTableName(const std::wstring_view table) const;
    virtual std::wstring generateDropTableCommand(const std::wstring_view table) const = 0;
    virtual std::wstring generateCreateTableCommand(const std::wstring_view table) const = 0;
    virtual std::wstring generateCopyIntoCommand(const std::wstring_view table) const = 0;
    virtual std::optional<std::size_t> getRejectedRecords(nanodbc::connection& connection) const;

protected:
    bfs::path mInputFile;
    wchar_t mSeparator { L',' };
    wchar_t mQuote { L'"' };
    ParsingResults mParsingResults;
    std::vector<ConnectionParameter> mConnectionParameters;
};

template <typename T>
void BulkLoader::setConnectionParameters(const T& connectionParameters)
{
    mConnectionParameters.insert(mConnectionParameters.cend(), std::cbegin(connectionParameters), std::cend(connectionParameters));
}
