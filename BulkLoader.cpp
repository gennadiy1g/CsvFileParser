#include "BulkLoader.h"

BulkLoader::BulkLoader(bfs::path inputFile)
    : mInputFile(inputFile)
{
}

void BulkLoader::parse(wchar_t separator, wchar_t quote)
{
    CsvFileParser parser(mInputFile);
    mParsingResults = parser.parse(separator, quote, L'\\');
}

void BulkLoader::load(std::string_view host, int port, std::string_view database, std::string_view table, std::string_view user, std::string_view password) {}
