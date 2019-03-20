#pragma once

#include <string_view>

class CsvFileParser
{
public:
    CsvFileParser(std::wstring_view sourceFile); // Constructor
    virtual ~CsvFileParser() = default;          // Defaulted virtual destructor

    // Disallow assignment and pass-by-value.
    CsvFileParser(const CsvFileParser& src) = delete;
    CsvFileParser& operator=(const CsvFileParser& rhs) = delete;

    // Explicitly default move constructor and move assignment operator.
    CsvFileParser(CsvFileParser&& src) = default;
    CsvFileParser& operator=(CsvFileParser&& rhs) = default;

    void parse(wchar_t separator, wchar_t qoute, wchar_t escape, int numberThreads = 0);

private:
    void parseBuffer();
};
