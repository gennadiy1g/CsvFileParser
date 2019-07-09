#pragma once

#include <boost/filesystem.hpp>
#include <string>
#include <string_view>

#include "CsvFileParser.h"

namespace bfs = boost::filesystem;

class BulkLoader {

public:
    BulkLoader(bfs::path inputFile); // Constructor
    virtual ~BulkLoader() = default; // Defaulted virtual destructor

    void parse(wchar_t separator, wchar_t quote);
    void load(std::string_view host, int port, std::string_view database, std::string_view table, std::string_view user, std::string_view password);
    virtual void loadOnClient(std::string_view host, int port, std::string_view database, std::string_view table, std::string_view user, std::string_view password) = 0;

protected:
    bfs::path mInputFile;
    ParsingResults mParsingResults;

private:
    virtual std::wstring generateCreateTableCommand() const = 0;
    virtual std::wstring generateCopyIntoCommand() const = 0;
};
