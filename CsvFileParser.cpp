#include "CsvFileParser.h"
#include "log.h"
#include <boost/locale.hpp>
#include <fstream>
#include <sstream>
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

ParsingResults CsvFileParser::parse(wchar_t separator, wchar_t qoute, wchar_t escape, unsigned int numThreadsOpt)
{
    auto& gLogger = GlobalLogger::get();
    BOOST_LOG_SEV(gLogger, trivia::trace) << "->" << FUNCTION_FILE_LINE;

    auto numThreads = numThreadsOpt > 0 ? numThreadsOpt : std::thread::hardware_concurrency();

    mBuffers.resize(numThreads);

    for (unsigned int i = 0; i < numThreads; ++i) {
        mEmptyBuffers.push(i);
    }

    BOOST_LOG_SEV(gLogger, trivia::debug) << mInputFile.data();
    std::wifstream inputFile;
    inputFile.open(mInputFile.data());
    std::wstring line;
    unsigned int numLines{ 0 };
    while (std::getline(inputFile, line)) {
        ++numLines;
        //        BOOST_LOG_SEV(gLogger, trivia::debug) << numLines << ' ' << line;
    }
    if (!inputFile.eof()) {
        std::stringstream message;
        message << "Character set conversions error! File: " << mInputFile.data() << ", line: " << numLines + 1 << ", column: " << line.length() + 1 << '.';
        BOOST_LOG_SEV(gLogger, trivia::error) << message.str();
        BOOST_LOG_SEV(gLogger, trivia::debug) << line;
        throw std::runtime_error(message.str());
    } else {
        BOOST_LOG_SEV(gLogger, trivia::debug) << "All " << numLines << " lines processed.";
    }

    // Launch threads
    std::vector<std::thread> threads(numThreads);
    std::generate(threads.begin(), threads.end(), [this] { return std::thread{ &CsvFileParser::worker, this }; });

    // Join on all threads
    std::for_each(threads.begin(), threads.end(), [](auto& t) { t.join(); });

    BOOST_LOG_SEV(gLogger, trivia::trace) << "<-" << FUNCTION_FILE_LINE;

    return std::move(mResults);
}

void CsvFileParser::worker()
{
    auto& gLogger = GlobalLogger::get();
    BOOST_LOG_SEV(gLogger, trivia::trace) << "->" << FUNCTION_FILE_LINE;

    BOOST_LOG_SEV(gLogger, trivia::trace) << "<-" << FUNCTION_FILE_LINE;
}
