#include "CsvFileParser.h"
#include "log.h"
#include <boost/locale.hpp>
#include <cassert>
#include <condition_variable>
#include <fstream>
#include <mutex>
#include <optional>
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

void ParserBuffer::addLine(std::wstring&& line)
{
    mLines.push_back(std::move(line));
}

std::size_t ParserBuffer::getSize()
{
    return mLines.size();
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
    std::size_t numInputFileLines{ 0 };
    std::optional<unsigned int> numBufferToFill{ std::nullopt };
    const std::size_t kMaxBufferLines{ 100 };

    auto addToFullBuffers = [this, numBufferToFill, &gLogger]() {
        std::lock_guard<std::mutex> lock(mMutexFullBuffers);
        mFullBuffers.push(numBufferToFill.value());
        BOOST_LOG_SEV(gLogger, trivia::trace) << "Buffer #" << numBufferToFill.value() << " is added into the queue of full buffers.";
        mMutexFullBuffers.unlock();
        mConditionVarFullBuffers.notify_one();

    };

    while (std::getline(inputFile, line)) {
        ++numInputFileLines;
        //        BOOST_LOG_SEV(gLogger, trivia::debug) << numInputFileLines << ' ' << line;

        if (!numBufferToFill.has_value()) {
            BOOST_LOG_SEV(gLogger, trivia::trace) << "Buffer to fill is not set.";
            std::unique_lock<std::mutex> lock(mMutexEmptyBuffers);
            if (mEmptyBuffers.size() == 0) {
                BOOST_LOG_SEV(gLogger, trivia::trace) << "Starting to wait for an empty buffer.";
                mConditionVarEmptyBuffers.wait(lock, [this] { return mEmptyBuffers.size() > 0; });
                BOOST_LOG_SEV(gLogger, trivia::trace) << "Finished waiting.";
            }
            assert(mEmptyBuffers.size() > 0);
            numBufferToFill = mEmptyBuffers.front();
            BOOST_LOG_SEV(gLogger, trivia::trace) << "Buffer #" << numBufferToFill.value() << " is set to be filled.";
            mEmptyBuffers.pop();
        }

        mBuffers.at(numBufferToFill.value()).addLine(std::move(line));
        if (mBuffers.at(numBufferToFill.value()).getSize() == kMaxBufferLines) {
            // The buffer is full
            addToFullBuffers();
            numBufferToFill = std::nullopt;
        }
    }

    if (!inputFile.eof()) {
        std::stringstream message;
        message << "Character set conversions error! File: " << mInputFile.data() << ", line: " << numInputFileLines + 1 << ", column: " << line.length() + 1 << '.';
        BOOST_LOG_SEV(gLogger, trivia::error) << message.str();
        BOOST_LOG_SEV(gLogger, trivia::debug) << line;
        throw std::runtime_error(message.str());
    } else {
        if (mBuffers.at(numBufferToFill.value()).getSize() > 0) {
            // The last, partially filled, buffer
            addToFullBuffers();
        }
        BOOST_LOG_SEV(gLogger, trivia::debug) << "All " << numInputFileLines << " lines processed.";
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
