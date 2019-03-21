#include "CsvFileParser.h"
#include <string_view>
#include <thread>

CsvFileParser::CsvFileParser(std::wstring_view sourceFile)
{
}

void CsvFileParser::parse(wchar_t separator, wchar_t qoute, wchar_t escape, unsigned int numThreads_)
{
    unsigned int numThreads;
    if (numThreads_ == 0) {
        numThreads = std::thread::hardware_concurrency();
    } else {
        numThreads = numThreads_;
    }
}

void CsvFileParser::parseBuffer()
{
}
