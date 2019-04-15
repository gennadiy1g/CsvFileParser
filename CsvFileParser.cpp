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
    std::optional<unsigned int> numBufferToFill{ 0 };
    const std::size_t kMaxBufferLines{ 100 };

    auto addToFullBuffers = [this, numBufferToFill, &gLogger]() {
        {
            std::lock_guard<std::mutex> lock(mMutexFullBuffers);
            mFullBuffers.push(numBufferToFill.value());
            BOOST_LOG_SEV(gLogger, trivia::trace) << "Buffer #" << numBufferToFill.value() << " is added into the queue of full buffers.";
        }
        mConditionVarFullBuffers.notify_one();

    };

    mMainLoopIsDone = false;

    // Launch worker/parser threads
    std::vector<std::thread> threads(numThreads);
    std::generate(threads.begin(), threads.end(), [this] { return std::thread{ &CsvFileParser::worker, this }; });

    // The main/reader loop
    while (std::getline(inputFile, line)) {
        ++numInputFileLines;
        BOOST_LOG_SEV(gLogger, trivia::debug) << numInputFileLines << ' ' << line;

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
            mEmptyBuffers.pop();
            BOOST_LOG_SEV(gLogger, trivia::trace) << "Buffer #" << numBufferToFill.value() << " is removed from the queue of empty buffers to be filled.";
        }

        mBuffers.at(numBufferToFill.value()).addLine(std::move(line));
        if (mBuffers[numBufferToFill.value()].getSize() == kMaxBufferLines) {
            // The buffer is full
            addToFullBuffers();
            numBufferToFill = std::nullopt;
        }
    }

    BOOST_LOG_SEV(gLogger, trivia::trace) << "The main/reader loop is done, notifying all worker/parser threads.";
    mMainLoopIsDone = true;
    mConditionVarFullBuffers.notify_all();

    // Wait for all worker/parser threads to finish
    std::for_each(threads.begin(), threads.end(), [](auto& t) { t.join(); });

    if (!inputFile.eof()) {
        std::stringstream message;
        message << "Character set conversions error! File: " << mInputFile.data() << ", line: " << numInputFileLines + 1 << ", column: " << line.length() + 1 << '.';
        BOOST_LOG_SEV(gLogger, trivia::debug) << line;
        BOOST_LOG_SEV(gLogger, trivia::error) << message.str() << std::flush;
        throw std::runtime_error(message.str());
    }

    if (mBuffers[numBufferToFill.value()].getSize() > 0) {
        // The last, partially filled, buffer
        addToFullBuffers();
    }

    BOOST_LOG_SEV(gLogger, trivia::debug) << "All " << numInputFileLines << " lines processed.";
    BOOST_LOG_SEV(gLogger, trivia::trace) << "<-" << FUNCTION_FILE_LINE;

    return std::move(mResults);
}

void CsvFileParser::worker()
{
    auto& gLogger = GlobalLogger::get();
    BOOST_LOG_SEV(gLogger, trivia::trace) << "->" << FUNCTION_FILE_LINE;

    while (!mMainLoopIsDone) {
        unsigned int numBufferToParse;
        {
            BOOST_LOG_SEV(gLogger, trivia::trace) << "Starting to wait for a full buffer.";
            std::unique_lock<std::mutex> lock(mMutexFullBuffers);
            mConditionVarFullBuffers.wait(lock, [this] { return mFullBuffers.size() > 0; });
            BOOST_LOG_SEV(gLogger, trivia::trace) << "Finished waiting.";

            if (mMainLoopIsDone) {
                BOOST_LOG_SEV(gLogger, trivia::trace) << "Exiting the worker/parser loop.";
                break;
            }

            assert(mFullBuffers.size() > 0);
            numBufferToParse = mFullBuffers.front();
            mFullBuffers.pop();
            BOOST_LOG_SEV(gLogger, trivia::trace) << "Buffer #" << numBufferToParse << " is removed from the queue of full buffers to be parsed.";
        }

        parseBuffer(numBufferToParse);

        {
            std::lock_guard<std::mutex> lock(mMutexEmptyBuffers);
            mEmptyBuffers.push(numBufferToParse);
            BOOST_LOG_SEV(gLogger, trivia::trace) << "Buffer #" << numBufferToParse << " is added into the queue of empty buffers.";
        }
        mConditionVarEmptyBuffers.notify_one();
    }
    BOOST_LOG_SEV(gLogger, trivia::trace) << "<-" << FUNCTION_FILE_LINE;
}

void CsvFileParser::parseBuffer(unsigned int numBufferToParse)
{
    auto& gLogger = GlobalLogger::get();
    BOOST_LOG_SEV(gLogger, trivia::trace) << "Buffer #" << numBufferToParse << " is parsed." << FUNCTION_FILE_LINE;
}
