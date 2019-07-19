#pragma once

#include <boost/filesystem.hpp>
#include <initializer_list>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "CsvFileParser.h"

namespace bfs = boost::filesystem;

using namespace std::literals;

typedef std::pair<std::wstring, std::wstring> ConnectionParameter;

class BulkLoader {

public:
    BulkLoader(const bfs::path& inputFile); // Constructor
    virtual ~BulkLoader() = default; // Defaulted virtual destructor

    void parse(wchar_t separator, wchar_t quote);
    void load(std::wstring_view table = L""sv) const;
    virtual void loadOnClient(std::string_view host, int port, std::string_view database, std::string_view table, std::string_view user, std::string_view password) const = 0;
    const ParsingResults& parsingResults() const { return mParsingResults; };
    template <typename T>
    void setConnectionParameters(const T& connectionParameters);
    std::wstring getConnectionString() const;
    virtual std::wstring generateDropTableCommand(const std::wstring_view table) const = 0;
    virtual std::wstring generateCreateTableCommand(const std::wstring_view table) const = 0;
    virtual std::wstring generateCopyIntoCommand(const std::wstring_view table) const = 0;

protected:
    bfs::path mInputFile;
    wchar_t mSeparator;
    wchar_t mQuote;
    ParsingResults mParsingResults;
    std::vector<ConnectionParameter> mConnectionParameters;
};

template <typename T>
void BulkLoader::setConnectionParameters(const T& connectionParameters)
{
    mConnectionParameters.insert(mConnectionParameters.cend(), std::cbegin(connectionParameters), std::cend(connectionParameters));
}
