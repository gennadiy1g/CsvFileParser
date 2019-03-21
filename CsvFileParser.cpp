#include "CsvFileParser.h"
#include <algorithm>
#include <string_view>
#include <thread>
#include <vector>

CsvFileParser::CsvFileParser(std::wstring_view sourceFile)
{
}

void CsvFileParser::parse(wchar_t separator, wchar_t qoute, wchar_t escape, unsigned int numThreads_)
{
    int numThreads;
    if (numThreads_ == 0) {
        numThreads = std::thread::hardware_concurrency();
    } else {
        numThreads = numThreads_;
    }

    // Launch threads
    std::vector<std::thread> threads(numThreads);
    std::generate(threads.begin(), threads.end(), [this] { return std::thread{ &CsvFileParser::parseBuffer, this }; });

    // Join on all threads
    std::for_each(threads.begin(), threads.end(), [](auto& t) { t.join(); });
}

void CsvFileParser::parseBuffer()
{
}
