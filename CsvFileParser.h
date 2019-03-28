#pragma once

#include <string>
#include <string_view>
#include <vector>

class ParserBuffer {
public:
    explicit ParserBuffer(); // Constructor
    virtual ~ParserBuffer(); // Defaulted virtual destructor

    // Disallow assignment and pass-by-value.
    ParserBuffer(const ParserBuffer& src) = delete;
    ParserBuffer& operator=(const ParserBuffer& rhs) = delete;

    // Explicitly default move constructor and move assignment operator.
    ParserBuffer(ParserBuffer&& src) = default;
    ParserBuffer& operator=(ParserBuffer&& rhs) = default;

private:
    std::vector<std::wstring> mLines;
};

class CsvFileParser {
public:
    explicit CsvFileParser(std::wstring_view sourceFile); // Constructor
    virtual ~CsvFileParser() = default; // Defaulted virtual destructor

    // Disallow assignment and pass-by-value.
    CsvFileParser(const CsvFileParser& src) = delete;
    CsvFileParser& operator=(const CsvFileParser& rhs) = delete;

    // Explicitly default move constructor and move assignment operator.
    CsvFileParser(CsvFileParser&& src) = default;
    CsvFileParser& operator=(CsvFileParser&& rhs) = default;

    void parse(wchar_t separator, wchar_t qoute, wchar_t escape, unsigned int numThreads_ = 0);

private:
    void worker();
    std::vector<ParserBuffer> mBuffers;
};
