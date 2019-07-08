#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <sstream>
#include <string>

using namespace std::string_literals;

#include "MonetDBBulkLoader.h"

std::wstring MonetDBBulkLoader::generateCopyIntoCommand()
{
    return L""s;
}

std::wstring MonetDBBulkLoader::generateCreateTableCommand()
{
    std::wostringstream buf(L"CREATE TABLE ");
    buf << mInputFile.stem();
    for (const auto& column : mParsingResults.columns()) {
        buf << L'"' << boost::trim_copy(column.name()) << L"\" ";
    }
    return buf.str();
}
