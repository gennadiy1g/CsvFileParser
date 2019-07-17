#pragma once

#include <boost/filesystem.hpp>
#include <string>
#include <string_view>

#include "CsvFileParser.h"

namespace bfs = boost::filesystem;

using namespace std::literals;

class BulkLoader {

public:
    BulkLoader(const bfs::path& inputFile); // Constructor
    virtual ~BulkLoader() = default; // Defaulted virtual destructor

    void parse(wchar_t separator, wchar_t quote);
    void load(std::wstring_view table = L""sv, int port = 50000, std::wstring_view user = L"monetdb"sv, std::wstring_view password = L"monetdb"sv) const;
    virtual void loadOnClient(std::string_view host, int port, std::string_view database, std::string_view table, std::string_view user, std::string_view password) const = 0;
    const ParsingResults& parsingResults() const { return mParsingResults; };

    std::wstring generateDropTableCommand_unit_testing(const std::wstring_view table) const { return generateDropTableCommand(table); };
    std::wstring generateCreateTableCommand_unit_testing(const std::wstring_view table) const { return generateCreateTableCommand(table); };
    std::wstring generateCopyIntoCommand_unit_testing(const std::wstring_view table) const { return generateCopyIntoCommand(table); };

protected:
    bfs::path mInputFile;
    wchar_t mSeparator;
    wchar_t mQuote;
    ParsingResults mParsingResults;

private:
    virtual std::wstring generateDropTableCommand(const std::wstring_view table) const = 0;
    virtual std::wstring generateCreateTableCommand(const std::wstring_view table) const = 0;
    virtual std::wstring generateCopyIntoCommand(const std::wstring_view table) const = 0;
};
