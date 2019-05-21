#pragma once

#include <atomic>
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>
#include <boost/locale.hpp>
#include <boost/tokenizer.hpp>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <vector>

namespace bfs = boost::filesystem;
namespace blocale = boost::locale;

typedef boost::escaped_list_separator<wchar_t, std::char_traits<wchar_t>> CsvSeparator;
typedef boost::tokenizer<CsvSeparator, std::wstring::const_iterator, std::wstring> CsvTokenizer;

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
    std::size_t size();
    void clear();
    const std::vector<std::wstring>& Lines();

private:
    std::vector<std::wstring> mLines;
};

class ColumnInfo {
public:
    explicit ColumnInfo(std::wstring_view name); // Constructor
    virtual ~ColumnInfo() = default; // Defaulted virtual destructor

    //  Explicitly default assignment and pass-by-value.
    ColumnInfo(const ColumnInfo& src) = default;
    ColumnInfo& operator=(const ColumnInfo& rhs) = default;

    // Explicitly default move constructor and move assignment operator.
    ColumnInfo(ColumnInfo&& src) = default;
    ColumnInfo& operator=(ColumnInfo&& rhs) = default;

protected:
    std::wstring mName;
    bool mIsFloat { true };
    bool mIsDecimal { true };
    bool mIsInt { true };
    bool mIsBool { true };
    bool mIsDate { true };
    bool mIsTime { true };
    bool mIsTimeStamp { true };
    bool mIsNull { false };
    std::optional<unsigned short> mPrec;
    std::optional<unsigned short> mScale;
    unsigned short mLength { 0 };
    std::optional<long long> mMinVal;
    std::optional<long long> mMaxVal;
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

    void update(const ParsingResults& results);
    void addColumn(std::wstring_view name);

private:
    std::vector<ColumnInfo> mColumns;
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

    ParsingResults parse(wchar_t separator, wchar_t quote, wchar_t escape, unsigned int numThreadsOpt = 0);

private:
    void parser();
    void parseBuffer(unsigned int numBufferToParse, ParsingResults& results);
    void checkInputFile();
    void parseColumnNames(std::wstring_view line);

    bfs::path mInputFile;

    std::atomic_bool mReaderLoopIsDone;
    std::atomic_bool mCharSetConversionError;

    std::vector<ParserBuffer> mBuffers;

    std::queue<unsigned int> mEmptyBuffers;
    std::mutex mMutexEmptyBuffers;
    std::condition_variable mConditionVarEmptyBuffers;

    std::queue<unsigned int> mFullBuffers;
    std::mutex mMutexFullBuffers;
    std::condition_variable_any mConditionVarFullBuffers;

    ParsingResults mResults;
    std::shared_mutex mMutexResults;

    CsvSeparator mSep;
};
