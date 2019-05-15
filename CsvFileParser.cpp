#include "CsvFileParser.h"
#include "log.h"
#include <boost/algorithm/string.hpp>
#include <cassert>
#include <sstream>
#include <thread>

using namespace std::string_literals;

ColumnInfo::ColumnInfo(std::wstring_view name)
    : mName(name)
{
}

void ParsingResults::Update(const ParsingResults& results)
{
}

void ParsingResults::addColumn(std::wstring_view name)
{
    mColumns.emplace_back(name);
}

ParserBuffer::ParserBuffer()
{
}

void ParserBuffer::addLine(std::wstring&& line)
{
    mLines.push_back(std::move(line));
}

std::size_t ParserBuffer::size()
{
    return mLines.size();
}

void ParserBuffer::clear()
{
    mLines.clear();
}

CsvFileParser::CsvFileParser(bfs::path inputFile)
    : mInputFile(inputFile)
{
}

void CsvFileParser::checkInputFile()
{
    bfs::file_status inputFileStatus = bfs::status(mInputFile);

    if (!bfs::exists(inputFileStatus)) {
        throw std::runtime_error("File \""s + blocale::conv::utf_to_utf<char>(mInputFile.native()) + "\" does not exist!"s);
    }

    if (!bfs::is_regular_file(inputFileStatus)) {
        throw std::runtime_error("File \""s + blocale::conv::utf_to_utf<char>(mInputFile.native()) + "\" is not a regular file!"s);
    }

    if (bfs::file_size(mInputFile) == 0) {
        throw std::runtime_error("File \""s + blocale::conv::utf_to_utf<char>(mInputFile.native()) + "\" is empty!"s);
    }
};

ParsingResults CsvFileParser::parse(wchar_t separator, wchar_t quote, wchar_t escape, unsigned int numThreadsOpt)
{
    checkInputFile();

    auto& gLogger = GlobalLogger::get();
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "->" << FUNCTION_FILE_LINE;

    auto numThreads = numThreadsOpt > 0 ? numThreadsOpt : std::thread::hardware_concurrency();

    mBuffers.resize(numThreads);

    BOOST_LOG_SEV(gLogger, bltrivial::debug) << mInputFile.native();
    bfs::wifstream inputFile(mInputFile);
    if (inputFile.fail()) {
        BOOST_LOG_SEV(gLogger, bltrivial::error) << "Throwing exception @" << FUNCTION_FILE_LINE << std::flush;
        throw std::runtime_error("Unable to open file \""s + blocale::conv::utf_to_utf<char>(mInputFile.native()) + "\" for reading!"s);
    }
    std::wstring line;
    std::size_t numInputFileLines{ 0 }; // Counter of lines in the file.

    // Maximum number of lines in one buffer.
#ifdef NDEBUG
    const std::size_t kMaxBufferLines{ 1000 };
#else
    const std::size_t kMaxBufferLines{ 10 };
#endif

    unsigned int numBufferToFill{ 0 }; // The buffer #0 is going to be filled first.
    for (unsigned int i = 1; i < numThreads; ++i) { // Do not add the buffer #0 into the queue of empty buffers.
        mEmptyBuffers.push(i);
    }

    auto addToFullBuffers = [this, &numBufferToFill, &gLogger]() {
        {
            std::unique_lock lock(mMutexFullBuffers);
            mFullBuffers.push(numBufferToFill);
            BOOST_LOG_SEV(gLogger, bltrivial::trace) << "The buffer #" << numBufferToFill << " is added into the queue of full buffers.";
        }
        mConditionVarFullBuffers.notify_one();
    };

    mMainLoopIsDone = false;
    mCharSetConversionError = false;

    mSeparator = separator;
    mQuote = quote;
    mEscape = escape;

    // Launch parser threads
    std::vector<std::thread> threads(numThreads);
    std::generate(threads.begin(), threads.end(), [this] { return std::thread{ &CsvFileParser::parser, this }; });

    // Reader loop
    while (std::getline(inputFile, line)) {
        ++numInputFileLines;
        BOOST_LOG_SEV(gLogger, bltrivial::trace) << numInputFileLines << ' ' << line;

        if (numInputFileLines == 1) {
            parseColumnNames(line, separator, quote, escape);
            continue;
        }

        mBuffers.at(numBufferToFill).addLine(std::move(line));

        if (mBuffers[numBufferToFill].size() == kMaxBufferLines) {
            // The buffer is full, add it into the queue of full buffers.
            addToFullBuffers();

            // Get the number of the next empty buffer to fill.
            std::unique_lock lock(mMutexEmptyBuffers);
            if (mEmptyBuffers.size() == 0) {
                BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Starting to wait for an empty buffer.";
                mConditionVarEmptyBuffers.wait(lock, [this] { return mEmptyBuffers.size() > 0; });
                BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Finished waiting.";
                assert(mEmptyBuffers.size() > 0);
            }
            numBufferToFill = mEmptyBuffers.front();
            assert(mBuffers[numBufferToFill].size() == 0);
            mEmptyBuffers.pop();
            BOOST_LOG_SEV(gLogger, bltrivial::trace) << "The buffer #" << numBufferToFill << " is removed from the queue of empty buffers.";
        }
    }

    std::stringstream message;
    if (!inputFile.eof()) {
        message << "Character set conversion error! File: \"" << blocale::conv::utf_to_utf<char>(mInputFile.native())
                << "\", line: " << numInputFileLines + 1 << ", column: " << line.length() + 1 << '.';
        BOOST_LOG_SEV(gLogger, bltrivial::debug) << line;
        BOOST_LOG_SEV(gLogger, bltrivial::error) << message.str() << std::flush;

        // Even if the shared variable is atomic, it must be modified under the mutex in order to correctly publish
        // the modification to the waiting thread (https://en.cppreference.com/w/cpp/thread/condition_variable).
        std::unique_lock lock(mMutexFullBuffers);
        mCharSetConversionError = true;
    } else {
        if (mBuffers[numBufferToFill].size() > 0) {
            // The last buffer is partially filled, add it into the queue of full buffers.
            addToFullBuffers();
        }
        BOOST_LOG_SEV(gLogger, bltrivial::trace) << "It has been the last buffer.";
    }

    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "The reader loop is done, notifying all parser threads.";
    {
        // Even if the shared variable is atomic, it must be modified under the mutex in order to correctly publish
        // the modification to the waiting thread (https://en.cppreference.com/w/cpp/thread/condition_variable).
        std::unique_lock lock(mMutexFullBuffers);
        mMainLoopIsDone = true;
    }
    mConditionVarFullBuffers.notify_all();

    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Starting to wait for all parser threads to finish." << std::flush;
    std::for_each(threads.begin(), threads.end(), [](auto& t) { t.join(); });
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Finished waiting. All parser threads finished." << std::flush;

    if (!inputFile.eof()) {
        BOOST_LOG_SEV(gLogger, bltrivial::error) << "Throwing exception @" << FUNCTION_FILE_LINE << std::flush;
        throw std::runtime_error(message.str());
    } else {
        BOOST_LOG_SEV(gLogger, bltrivial::debug) << "All " << numInputFileLines << " lines processed.";
        BOOST_LOG_SEV(gLogger, bltrivial::trace) << "<-" << FUNCTION_FILE_LINE;
        return std::move(mResults);
    }
}

void CsvFileParser::parser()
{
    auto& gLogger = GlobalLogger::get();
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "->" << FUNCTION_FILE_LINE;

    auto pred = [this] { return (mMainLoopIsDone && (mFullBuffers.size() == 0)) || mCharSetConversionError; };

    // Parser loop
    while (true) {
        unsigned int numBufferToParse;
        {
            // Get the number of the next full buffer to parse.
            std::unique_lock lock(mMutexFullBuffers);
            if (mFullBuffers.size() == 0) {
                BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Starting to wait for a full buffer.";
                mConditionVarFullBuffers.wait(lock, [this] { return (mFullBuffers.size() > 0) || mMainLoopIsDone || mCharSetConversionError; });
                BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Finished waiting.";
            }

            if (pred()) {
                BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Exiting the parser loop.";
                break;
            }

            assert(mFullBuffers.size() > 0);
            numBufferToParse = mFullBuffers.front();
            assert(mBuffers[numBufferToParse].size() > 0);
            mFullBuffers.pop();
            BOOST_LOG_SEV(gLogger, bltrivial::trace) << "The buffer #" << numBufferToParse << " is removed from the queue of full buffers.";
        }

        ParsingResults results;
        {
            std::shared_lock lock(mMutexResults);
            results = mResults;
        }
        parseBuffer(numBufferToParse, results);
        {
            std::unique_lock lock(mMutexResults);
            mResults.Update(results);
        }

        mBuffers[numBufferToParse].clear();

        {
            std::lock_guard lock(mMutexEmptyBuffers);
            assert(mBuffers[numBufferToParse].size() == 0);
            mEmptyBuffers.push(numBufferToParse);
            BOOST_LOG_SEV(gLogger, bltrivial::trace) << "The buffer #" << numBufferToParse << " is added into the queue of empty buffers.";
        }
        mConditionVarEmptyBuffers.notify_one();

        {
            std::shared_lock lock(mMutexFullBuffers);
            if (pred()) {
                BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Exiting the parser loop.";
                break;
            }
        }
    }
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "<-" << FUNCTION_FILE_LINE;
}

void CsvFileParser::parseBuffer(unsigned int numBufferToParse, ParsingResults& results)
{
    auto& gLogger = GlobalLogger::get();
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "->" << FUNCTION_FILE_LINE;
    assert(mBuffers[numBufferToParse].size() > 0);
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Starting to parse the buffer #" << numBufferToParse << ".";
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "The buffer #" << numBufferToParse << " is parsed.";
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "<-" << FUNCTION_FILE_LINE;
}

void CsvFileParser::parseColumnNames(std::wstring_view line, wchar_t separator, wchar_t quote, wchar_t escape)
{
    auto& gLogger = GlobalLogger::get();
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << line << FUNCTION_FILE_LINE;
    CsvSeparator sep(escape, separator, quote);
    CsvTokenizer tok(line, sep);
    for (auto beg = tok.begin(); beg != tok.end(); ++beg) {
        BOOST_LOG_SEV(gLogger, bltrivial::trace) << *beg;
        auto name = *beg;
        boost::trim(name);
        mResults.addColumn(name);
    }
}
