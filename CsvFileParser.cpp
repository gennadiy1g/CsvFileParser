#include "CsvFileParser.h"
#include "app.h"
#include <algorithm>
#include <boost/locale/generator.hpp>
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
    std::locale loc = boost::locale::generator().generate("fr_CA.CP-863");
    std::wifstream inputFile;
    inputFile.imbue(loc);
    inputFile.open(mInputFile.data());
    std::wstring line;
    unsigned int numLines{ 0 };
    while (inputFile) {
        std::getline(inputFile, line);
        ++numLines;
        if (inputFile.fail()) {
            std::stringstream message;
            message << "Character set conversions error! File: " << mInputFile.data() << ", line: " << numLines << ", column: " << line.length() + 1 << '.';
            BOOST_LOG_SEV(gLogger, triv::error) << message.str();
            BOOST_LOG_SEV(gLogger, triv::debug) << line;
            throw std::runtime_error(message.str());
        }
    }
    BOOST_LOG_SEV(gLogger, triv::debug) << "All" << numLines << " lines processed.";

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
