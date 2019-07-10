#pragma once

#include <boost/filesystem.hpp>
#include <string>
#include <string_view>

#include "CsvFileParser.h"

namespace bfs = boost::filesystem;

class BulkLoader {

public:
    BulkLoader(const bfs::path& inputFile); // Constructor
    virtual ~BulkLoader() = default; // Defaulted virtual destructor

    void parse(wchar_t separator, wchar_t quote);
    void load(std::string_view host, int port, std::string_view database, std::string_view table, std::string_view user, std::string_view password) const;
    virtual void loadOnClient(std::string_view host, int port, std::string_view database, std::string_view table, std::string_view user, std::string_view password) const = 0;

    std::wstring generateCreateTableCommand_unit_testing(const std::wstring_view table) const { return generateCreateTableCommand(table); };

protected:
    bfs::path mInputFile;
    ParsingResults mParsingResults;

private:
    virtual std::wstring generateCreateTableCommand(const std::wstring_view table) const = 0;
    virtual std::wstring generateCopyIntoCommand(const std::wstring_view table) const = 0;
};
