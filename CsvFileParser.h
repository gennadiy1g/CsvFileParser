#pragma once

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

    ParsingResults parse(wchar_t separator, wchar_t qoute, wchar_t escape, unsigned int numThreads_or_0 = 0);

private:
    void worker();

    std::vector<ParserBuffer> mBuffers;

    std::queue<unsigned int> mEmptyBuffers;
    std::mutex mEmpBuffMutex;
    std::condition_variable mEmpBuffCondVar;

    std::queue<unsigned int> mFullBuffers;
    std::mutex mFullBuffMutex;
    std::condition_variable mFullBuffCondVar;

    ParsingResults mResults;
    std::mutex mResultsMutex;
    std::condition_variable mResultsCondVar;
};
