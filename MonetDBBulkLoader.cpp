#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <sstream>
#include <string>

using namespace std::string_literals;

#include "MonetDBBulkLoader.h"

std::wstring MonetDBBulkLoader::generateCopyIntoCommand() const
{
    return L""s;
}

std::wstring MonetDBBulkLoader::generateCreateTableCommand() const
{
    std::wostringstream buf(L"CREATE TABLE ");
    buf << L'"' << boost::trim_copy(mInputFile.stem().wstring()) << L"\" (";
    for (const auto& column : mParsingResults.columns()) {
        buf << L'"' << boost::trim_copy(column.name()) << L"\" ";
        switch (column.type()) {
        case ColumnType::String:;
        case ColumnType::Float:;
        case ColumnType::Decimal:;
        case ColumnType::Int:;
        case ColumnType::TimeStamp:;
        case ColumnType::Date:;
        case ColumnType::Time:;
        case ColumnType::Bool:;
        }
    }
    buf << L')';
    return buf.str();
}
