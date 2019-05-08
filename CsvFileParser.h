#pragma once

#include <atomic>
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>
#include <boost/locale.hpp>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <vector>

namespace bfs = boost::filesystem;
namespace blocale = boost::locale;

class ParserBuffer {
public:
    ParserBuffer(); // Constructor
    virtual ~ParserBuffer() = default; // Defaulted virtual destructor

    // Disallow assignment and pass-by-value.
    ParserBuffer(const ParserBuffer& src) = delete;
    ParserBuffer& operator=(const ParserBuffer& rhs) = delete;

    // Explicitly default move constructor and move assignment operator.
    ParserBuffer(ParserBuffer&& src) = default;
    ParserBuffer& operator=(ParserBuffer&& rhs) = default;

    void addLine(std::wstring&& line);
    std::size_t getSize();
    void clear();

private:
    std::vector<std::wstring> mLines;
};

class ParsingResults {
public:
    ParsingResults() = default; // Constructor
    virtual ~ParsingResults() = default; // Defaulted virtual destructor

    // Explicitly default copy constructor and copy assignment operator.
    ParsingResults(const ParsingResults& src) = default;
    ParsingResults& operator=(const ParsingResults& rhs) = default;

    // Explicitly default move constructor and move assignment operator.
    ParsingResults(ParsingResults&& src) = default;
    ParsingResults& operator=(ParsingResults&& rhs) = default;

private:
};

class CsvFileParser {
public:
    explicit CsvFileParser(bfs::path inputFile); // Constructor
    virtual ~CsvFileParser() = default; // Defaulted virtual destructor

    // Disallow assignment and pass-by-value.
    CsvFileParser(const CsvFileParser& src) = delete;
    CsvFileParser& operator=(const CsvFileParser& rhs) = delete;

    // Explicitly default move constructor and move assignment operator.
    CsvFileParser(CsvFileParser&& src) = default;
    CsvFileParser& operator=(CsvFileParser&& rhs) = default;

    ParsingResults parse(wchar_t separator, wchar_t qoute, wchar_t escape, unsigned int numThreadsOpt = 0);

private:
    void parser();
    void parseBuffer(unsigned int numBufferToParse);
    void checkInputFile();

    bfs::path mInputFile;

    std::atomic_bool mMainLoopIsDone;
    std::atomic_bool mCharSetConversionError;

    std::vector<ParserBuffer> mBuffers;

    std::queue<unsigned int> mEmptyBuffers;
    std::mutex mMutexEmptyBuffers;
    std::condition_variable mConditionVarEmptyBuffers;

    std::queue<unsigned int> mFullBuffers;
    std::shared_mutex mMutexFullBuffers;
    std::condition_variable_any mConditionVarFullBuffers;

    ParsingResults mResults;
    std::shared_mutex mMutexResults;
};
