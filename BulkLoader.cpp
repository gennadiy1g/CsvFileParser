#include <boost/algorithm/string.hpp>
#include <boost/locale.hpp>
#include <sstream>

#include "BulkLoader.h"
#include "log.h"
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

void BulkLoader::load(std::wstring_view table, int port, std::wstring_view user, std::wstring_view password) const
{
    std::wstring tableTrim(table);
    boost::trim(tableTrim);
    if (tableTrim == L"") {
        tableTrim = boost::trim_copy(mInputFile.stem().wstring());
    }
    assert(tableTrim != L"");

    auto& gLogger = GlobalLogger::get();

    // DRIVER=MonetDB ODBC Driver;PORT=50000;HOST=<your host>;DATABASE=<your db>;UID=monetdb;PWD=monetdb
    std::wostringstream buf(L"DRIVER=MonetDB ODBC Driver;HOST=127.0.0.1;", std::ios_base::ate);
    buf << L"PORT=" << port << L";UID=" << user << L";PWD=" << password << L';';
    auto connectionString(buf.str());
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << connectionString;

    auto dropCommand(generateDropTableCommand(tableTrim));
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << dropCommand;

    auto createCommand(generateCreateTableCommand(tableTrim));
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << createCommand;

    auto copyCommand(generateCopyIntoCommand(tableTrim));
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << copyCommand;

    nanodbc::connection connection(boost::locale::conv::utf_to_utf<char16_t>(connectionString));
    execute(connection, boost::locale::conv::utf_to_utf<char16_t>(dropCommand));
}
