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

void ParsingResults::update(const ParsingResults& results)
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

    BOOST_LOG_SEV(gLogger, bltrivial::debug) << mInputFile.native() << FUNCTION_FILE_LINE;
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
            BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Lock" << FUNCTION_FILE_LINE;
            std::unique_lock lock(mMutexFullBuffers);
            mFullBuffers.push(numBufferToFill);
            BOOST_LOG_SEV(gLogger, bltrivial::trace) << "The buffer #" << numBufferToFill << " is added into the queue of full buffers."
                                                     << FUNCTION_FILE_LINE;
        }
        mConditionVarFullBuffers.notify_one();
    };

    mReaderLoopIsDone = false;
    mCharSetConversionError = false;

    mSeparator = separator;
    mQuote = quote;
    mEscape = escape;

    // Launch parser threads
    std::vector<std::thread> threads(numThreads);
    std::generate(threads.begin(), threads.end(), [this] { return std::thread{ &CsvFileParser::parser, this }; });

    // Reader loop
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Starting the reader loop." << FUNCTION_FILE_LINE << std::flush;
    while (std::getline(inputFile, line)) {
        ++numInputFileLines;
        BOOST_LOG_SEV(gLogger, bltrivial::trace) << numInputFileLines << ' ' << line << FUNCTION_FILE_LINE;

        if (numInputFileLines == 1) {
            parseColumnNames(line, separator, quote, escape);
            continue;
        }

        mBuffers.at(numBufferToFill).addLine(std::move(line));

        if (mBuffers.at(numBufferToFill).size() == kMaxBufferLines) {
            // The buffer is full, add it into the queue of full buffers.
            addToFullBuffers();

            // Get the number of the next empty buffer to fill.
            BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Lock" << FUNCTION_FILE_LINE;
            std::unique_lock lock(mMutexEmptyBuffers);
            if (mEmptyBuffers.size() == 0) {
                BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Starting to wait for an empty buffer." << FUNCTION_FILE_LINE;
                mConditionVarEmptyBuffers.wait(lock, [this] { return mEmptyBuffers.size() > 0; });
                BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Finished waiting for an empty buffer." << FUNCTION_FILE_LINE;
                assert(mEmptyBuffers.size() > 0);
            }
            numBufferToFill = mEmptyBuffers.front();
            assert(mBuffers.at(numBufferToFill).size() == 0);
            mEmptyBuffers.pop();
            BOOST_LOG_SEV(gLogger, bltrivial::trace) << "The buffer #" << numBufferToFill << " is removed from the queue of empty buffers."
                                                     << FUNCTION_FILE_LINE;
        }
    }
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Finished the reader loop." << FUNCTION_FILE_LINE << std::flush;

    std::stringstream message;
    if (!inputFile.eof()) {
        message << "Character set conversion error! File: \"" << blocale::conv::utf_to_utf<char>(mInputFile.native())
                << "\", line: " << numInputFileLines + 1 << ", column: " << line.length() + 1 << '.';
        BOOST_LOG_SEV(gLogger, bltrivial::debug) << line;
        BOOST_LOG_SEV(gLogger, bltrivial::error) << message.str() << FUNCTION_FILE_LINE << std::flush;

        mCharSetConversionError = true;
    } else {
        if (mBuffers.at(numBufferToFill).size() > 0) {
            // The last buffer is partially filled, add it into the queue of full buffers.
            addToFullBuffers();
        }
        BOOST_LOG_SEV(gLogger, bltrivial::trace) << "It has been the last buffer." << FUNCTION_FILE_LINE;
    }

    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "The reader loop is done, notifying all parser threads." << FUNCTION_FILE_LINE << std::flush;
    mReaderLoopIsDone = true;
    mConditionVarFullBuffers.notify_all();

    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Starting to wait for all parser threads to finish." << FUNCTION_FILE_LINE << std::flush;
    std::for_each(threads.begin(), threads.end(), [](auto& t) { t.join(); });
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Finished waiting. All parser threads finished." << FUNCTION_FILE_LINE << std::flush;

    if (!inputFile.eof()) {
        BOOST_LOG_SEV(gLogger, bltrivial::error) << "Throwing exception @" << FUNCTION_FILE_LINE << std::flush;
        throw std::runtime_error(message.str());
    } else {
        BOOST_LOG_SEV(gLogger, bltrivial::debug) << "All " << numInputFileLines << " lines processed." << FUNCTION_FILE_LINE;
        BOOST_LOG_SEV(gLogger, bltrivial::trace) << "<-" << FUNCTION_FILE_LINE << std::flush;
        return std::move(mResults);
    }
}

void CsvFileParser::parser()
{
    auto& gLogger = GlobalLogger::get();

    // Parser loop
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Starting the parser loop." << FUNCTION_FILE_LINE << std::flush;
    while (true) {
        unsigned int numBufferToParse;
        {
            BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Lock" << FUNCTION_FILE_LINE;
            std::unique_lock lock(mMutexFullBuffers);
            if ((mReaderLoopIsDone && (mFullBuffers.size() == 0)) || mCharSetConversionError) {
                BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Exiting the parser loop." << FUNCTION_FILE_LINE;
                break;
            }

            if (mFullBuffers.size() == 0) {
                BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Starting to wait for a full buffer." << FUNCTION_FILE_LINE;
                mConditionVarFullBuffers.wait(lock);
                BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Finished waiting for a full buffer." << FUNCTION_FILE_LINE;
            }

            if (mFullBuffers.size() > 0) {
                // Get the number of the next full buffer to parse.
                numBufferToParse = mFullBuffers.front();
                assert(mBuffers.at(numBufferToParse).size() > 0);
                mFullBuffers.pop();
                BOOST_LOG_SEV(gLogger, bltrivial::trace) << "The buffer #" << numBufferToParse << " is removed from the queue of full buffers."
                                                         << FUNCTION_FILE_LINE;
            }
        }

        ParsingResults results;
        {
            BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Lock" << FUNCTION_FILE_LINE;
            std::shared_lock lock(mMutexResults);
            results = mResults;
        }
        parseBuffer(numBufferToParse, results);
        {
            BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Lock" << FUNCTION_FILE_LINE;
            std::unique_lock lock(mMutexResults);
            mResults.update(results);
        }

        mBuffers.at(numBufferToParse).clear();

        {
            BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Lock" << FUNCTION_FILE_LINE;
            std::unique_lock lock(mMutexEmptyBuffers);
            assert(mBuffers.at(numBufferToParse).size() == 0);
            mEmptyBuffers.push(numBufferToParse);
            BOOST_LOG_SEV(gLogger, bltrivial::trace) << "The buffer #" << numBufferToParse << " is added into the queue of empty buffers."
                                                     << FUNCTION_FILE_LINE;
        }
        mConditionVarEmptyBuffers.notify_one();
    }
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Finished the parser loop." << FUNCTION_FILE_LINE << std::flush;
}

void CsvFileParser::parseBuffer(unsigned int numBufferToParse, ParsingResults& results)
{
    auto& gLogger = GlobalLogger::get();
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "->" << FUNCTION_FILE_LINE;
    assert(mBuffers.at(numBufferToParse).size() > 0);
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Starting to parse the buffer #" << numBufferToParse << "." << FUNCTION_FILE_LINE;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "The buffer #" << numBufferToParse << " is parsed." << FUNCTION_FILE_LINE;
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "<-" << FUNCTION_FILE_LINE;
}

void CsvFileParser::parseColumnNames(std::wstring_view line, wchar_t separator, wchar_t quote, wchar_t escape)
{
    auto& gLogger = GlobalLogger::get();
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "->" << FUNCTION_FILE_LINE << std::flush;
    CsvSeparator sep(escape, separator, quote);
    CsvTokenizer tok(line, sep);
    for (auto beg = tok.begin(); beg != tok.end(); ++beg) {
        BOOST_LOG_SEV(gLogger, bltrivial::trace) << *beg;
        auto name = *beg;
        boost::trim(name);
        mResults.addColumn(name);
    }
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "<-" << FUNCTION_FILE_LINE << std::flush;
}
