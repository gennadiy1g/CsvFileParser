#include <sstream>

#include "BulkLoader.h"
#include "nanodbc.h"

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

void BulkLoader::load(std::string_view table, int port, std::string_view user, std::string_view password) const
{
    // DRIVER=MonetDB ODBC Driver;PORT=50000;HOST=<your host>;DATABASE=<your db>;UID=monetdb;PWD=monetdb
    std::ostringstream buf("DRIVER=MonetDB ODBC Driver;HOST=127.0.0.1;", std::ios_base::ate);
}
