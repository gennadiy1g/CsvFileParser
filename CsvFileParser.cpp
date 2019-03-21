#include "CsvFileParser.h"
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
    for (auto& t : threads) {
        t = std::thread{ &CsvFileParser::parseBuffer, this };
    }

    // Join on all threads
    for (auto& t : threads) {
        t.join();
    }
}

void CsvFileParser::parseBuffer()
{
}
