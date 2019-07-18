#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <iomanip>
#include <sstream>
#include <string>

using namespace std::string_literals;

#include "MonetDBBulkLoader.h"

MonetDBBulkLoader::MonetDBBulkLoader(const bfs::path& inputFile)
    : BulkLoader(inputFile)
{
    mDefaultConnectionParameters = { { L"DRIVER", L"MonetDB ODBC Driver" }, { L"HOST", L"127.0.0.1" } };
}

std::wstring MonetDBBulkLoader::generateCopyIntoCommand(const std::wstring_view table) const
{
    std::wostringstream buf(L"COPY ", std::ios_base::ate);
    buf << mParsingResults.numLines() << L" OFFSET 2 RECORDS INTO " << std::quoted(table)
        << L" FROM " << std::quoted(mInputFile.wstring(), L'\'')
        << L" USING DELIMITERS '" << mSeparator << L"','\\n','" << mQuote << L"\'  NULL AS \'\' LOCKED BEST EFFORT";
    return buf.str();
}

std::wstring MonetDBBulkLoader::generateCreateTableCommand(const std::wstring_view table) const
{
    std::wostringstream buf(L"CREATE TABLE ", std::ios_base::ate);
    buf << std::quoted(table) << L" (";
    auto firstColumn = true;
    assert(mParsingResults.columns().size() > 0);
    for (const auto& column : mParsingResults.columns()) {
        if (firstColumn) {
            firstColumn = false;
        } else {
            buf << L", ";
        };
        buf << std::quoted(boost::trim_copy(column.name())) << L' ';
        switch (column.type()) {
        case ColumnType::String:
            buf << (column.minLength() != column.maxLength() ? L"VARCHAR(" : L"CHAR(") << column.maxLength() << L')';
            break;
        case ColumnType::Float:
            buf << L"FLOAT(" << column.digitsBeforeDecimalPoint() + column.digitsAfterDecimalPoint() << L")";
            break;
        case ColumnType::Decimal:
            buf << L"DECIMAL(" << column.digitsBeforeDecimalPoint() + column.digitsAfterDecimalPoint() << L',' << column.digitsAfterDecimalPoint() << L')';
            break;
        case ColumnType::Int: {
            auto minValue = column.minValue();
            auto maxValue = column.maxValue();
            if ((-127 <= minValue) && (maxValue <= 127)) {
                buf << L"TINYINT";
            } else if ((-32767 <= minValue) && (maxValue <= 32767)) {
                buf << L"SMALLINT";
            } else if ((-2147483647 <= minValue) && (maxValue <= 2147483647)) {
                buf << L"INT";
            } else {
                buf << L"BIGINT";
            }
        } break;
        case ColumnType::TimeStamp:
            buf << L"TIMESTAMP";
            break;
        case ColumnType::Date:
            buf << L"DATE";
            break;
        case ColumnType::Time:
            buf << L"TIME";
            break;
        case ColumnType::Bool:
            buf << L"BOOL";
            break;
        }
        buf << (column.isNull() ? L" NULL" : L" NOT NULL");
    }
    buf << L')';
    return buf.str();
}

std::wstring MonetDBBulkLoader::generateDropTableCommand(const std::wstring_view table) const
{
    std::wostringstream buf(L"DROP TABLE IF EXISTS ", std::ios_base::ate);
    buf << std::quoted(table);
    return buf.str();
}
