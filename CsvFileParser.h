#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <string>
#include <string_view>
#include <vector>

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

private:
    std::vector<std::wstring> mLines;
};

class ParsingResults {
public:
    ParsingResults(); // Constructor
    virtual ~ParsingResults() = default; // Defaulted virtual destructor

    // Disallow assignment and pass-by-value.
    ParsingResults(const ParsingResults& src) = delete;
    ParsingResults& operator=(const ParsingResults& rhs) = delete;

    // Explicitly default move constructor and move assignment operator.
    ParsingResults(ParsingResults&& src) = default;
    ParsingResults& operator=(ParsingResults&& rhs) = default;

private:
};

class CsvFileParser {
public:
    explicit CsvFileParser(std::string_view inputFile); // Constructor
    virtual ~CsvFileParser() = default; // Defaulted virtual destructor

    // Disallow assignment and pass-by-value.
    CsvFileParser(const CsvFileParser& src) = delete;
    CsvFileParser& operator=(const CsvFileParser& rhs) = delete;

    // Explicitly default move constructor and move assignment operator.
    CsvFileParser(CsvFileParser&& src) = default;
    CsvFileParser& operator=(CsvFileParser&& rhs) = default;

    ParsingResults parse(wchar_t separator, wchar_t qoute, wchar_t escape, unsigned int numThreadsOpt = 0);

private:
    void worker();

    std::string_view mInputFile;

    std::atomic_bool mNoMoreFullBuffers;
    std::vector<ParserBuffer> mBuffers;

    std::queue<unsigned int> mEmptyBuffers;
    std::mutex mMutexEmptyBuffers;
    std::condition_variable mConditionVarEmptyBuffers;

    std::queue<unsigned int> mFullBuffers;
    std::mutex mMutexFullBuffers;
    std::condition_variable mConditionVarFullBuffers;

    ParsingResults mResults;
    std::mutex mMutexResults;
//    std::condition_variable mConditionVarResults;
};
