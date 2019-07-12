#include <boost/algorithm/string.hpp>
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

void BulkLoader::load(std::wstring_view table, int port, std::string_view user, std::string_view password) const
{
    std::wstring tableTrim(table);
    boost::trim(tableTrim);
    if (tableTrim == L"") {
        tableTrim = boost::trim_copy(mInputFile.stem().wstring());
    }
    assert(tableTrim != L"");

    auto& gLogger = GlobalLogger::get();

    // DRIVER=MonetDB ODBC Driver;PORT=50000;HOST=<your host>;DATABASE=<your db>;UID=monetdb;PWD=monetdb
    std::ostringstream buf("DRIVER=MonetDB ODBC Driver;HOST=127.0.0.1;", std::ios_base::ate);
    buf << "PORT=" << port << ";UID=" << user << ";PWD=" << password << ';';
    auto connectionString(buf.str());
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << connectionString;

    auto dropCommand(generateDropTableCommand(tableTrim));
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << dropCommand;

    auto createCommand(generateCreateTableCommand(tableTrim));
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << createCommand;

    auto copyCommand(generateCopyIntoCommand(tableTrim));
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << copyCommand;

    nanodbc::connection connection(u"foo");
    execute(connection, u"foo");
}
