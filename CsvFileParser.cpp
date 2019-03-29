#include "CsvFileParser.h"
#include "app.h"
#include <algorithm>
#include <boost/locale/generator.hpp>
#include <fstream>
#include <string_view>
#include <thread>
#include <vector>

ParsingResults::ParsingResults()
{
}

ParserBuffer::ParserBuffer()
{
}

CsvFileParser::CsvFileParser(std::string_view inputFile)
    : mInputFile(inputFile)
{
}

ParsingResults CsvFileParser::parse(wchar_t separator, wchar_t qoute, wchar_t escape, unsigned int numThreads_or_0)
{
    auto& gLogger = GlobalLogger::get();
    BOOST_LOG_SEV(gLogger, triv::trace) << "->" << FUNCTION_FILE_LINE;

    auto numThreads = numThreads_or_0 > 0 ? numThreads_or_0 : std::thread::hardware_concurrency();

    mBuffers.resize(numThreads);

    for (unsigned int i = 0; i < numThreads; ++i) {
        mEmptyBuffers.push(i);
    }

    BOOST_LOG_SEV(gLogger, triv::debug) << mInputFile.data();
    std::wifstream inputFile(mInputFile.data());
    inputFile.imbue(boost::locale::generator()("en_US.UTF-8"));
    for (std::wstring line; std::getline(inputFile, line);) {
        BOOST_LOG_SEV(gLogger, triv::debug) << line;
    }

    // Launch threads
    std::vector<std::thread> threads(numThreads);
    std::generate(threads.begin(), threads.end(), [this] { return std::thread{ &CsvFileParser::worker, this }; });

    // Join on all threads
    std::for_each(threads.begin(), threads.end(), [](auto& t) { t.join(); });

    BOOST_LOG_SEV(gLogger, triv::trace) << "<-" << FUNCTION_FILE_LINE;

    return std::move(mResults);
}

void CsvFileParser::worker()
{
    auto& gLogger = GlobalLogger::get();
    BOOST_LOG_SEV(gLogger, triv::trace) << "->" << FUNCTION_FILE_LINE;

    BOOST_LOG_SEV(gLogger, triv::trace) << "<-" << FUNCTION_FILE_LINE;
}
