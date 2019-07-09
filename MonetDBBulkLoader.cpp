#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <sstream>
#include <string>

using namespace std::string_literals;

#include "MonetDBBulkLoader.h"

MonetDBBulkLoader::MonetDBBulkLoader(const bfs::path& inputFile)
    : BulkLoader(inputFile)
{
}

std::wstring MonetDBBulkLoader::generateCopyIntoCommand() const
{
    return L""s;
}

std::wstring MonetDBBulkLoader::generateCreateTableCommand() const
{
    std::wostringstream buf(L"CREATE TABLE ");
    buf << L'"' << boost::trim_copy(mInputFile.stem().wstring()) << L"\" (";
    auto firstColumn = true;
    for (const auto& column : mParsingResults.columns()) {
        if (firstColumn) {
            firstColumn = false;
        } else {
            buf << L", ";
        };
        buf << L'"' << boost::trim_copy(column.name()) << L"\" ";
        switch (column.type()) {
        case ColumnType::String:
            buf << (column.minLength() != column.maxLength() ? L"VARCHAR(" : L"CHAR(") << column.maxLength() << L')';
        case ColumnType::Float:
            buf << L"FLOAT(" << column.digitsBeforeDecimalPoint() + column.digitsAfterDecimalPoint() << L")";
        case ColumnType::Decimal:
            buf << L"DECIMAL(" << column.digitsBeforeDecimalPoint() + column.digitsAfterDecimalPoint() << L',' << column.digitsAfterDecimalPoint() << L')';
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
        }
        case ColumnType::TimeStamp:
            buf << L"TIMESTAMP";
        case ColumnType::Date:
            buf << L"DATE";
        case ColumnType::Time:
            buf << L"TIME";
        case ColumnType::Bool:
            buf << L"BOOL";
        }
    }
    buf << L')';
    return buf.str();
}
