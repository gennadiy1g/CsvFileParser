#include "CsvFileParser.h"
#include "app.h"
#include <algorithm>
#include <string_view>
#include <thread>
#include <vector>

ParsingResults::ParsingResults()
{
}

ParserBuffer::ParserBuffer()
{
}

CsvFileParser::CsvFileParser(std::wstring_view sourceFile)
{
}

void CsvFileParser::parse(wchar_t separator, wchar_t qoute, wchar_t escape, unsigned int numThreads_or_0)
{
    auto& gLogger = GlobalLogger::get();
    BOOST_LOG_SEV(gLogger, triv::trace) << "->" << FUNCTION_FILE_LINE;

    int numThreads = numThreads_or_0 > 0 ? numThreads_or_0 : std::thread::hardware_concurrency();
    mBuffers.resize(numThreads);

    // Launch threads
    std::vector<std::thread> threads(numThreads);
    std::generate(threads.begin(), threads.end(), [this] { return std::thread{ &CsvFileParser::worker, this }; });

    // Join on all threads
    std::for_each(threads.begin(), threads.end(), [](auto& t) { t.join(); });

    BOOST_LOG_SEV(gLogger, triv::trace) << "<-" << FUNCTION_FILE_LINE;
}

void CsvFileParser::worker()
{
    auto& gLogger = GlobalLogger::get();
    BOOST_LOG_SEV(gLogger, triv::trace) << "->" << FUNCTION_FILE_LINE;

    BOOST_LOG_SEV(gLogger, triv::trace) << "<-" << FUNCTION_FILE_LINE;
}
