#pragma once

#include <atomic>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/time_facet.hpp>
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>
#include <boost/locale.hpp>
#include <boost/tokenizer.hpp>
#include <condition_variable>
#include <locale>
#include <mutex>
#include <optional>
#include <ostream>
#include <queue>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <vector>

namespace bfs = boost::filesystem;
namespace blocale = boost::locale;
namespace bpt = boost::posix_time;

typedef boost::escaped_list_separator<wchar_t, std::char_traits<wchar_t>> CsvSeparator;
typedef boost::tokenizer<CsvSeparator, std::wstring::const_iterator, std::wstring> CsvTokenizer;

class ParserBuffer {
    friend class CsvFileParser;

public:
    ParserBuffer() = default; // Constructor
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

private:
    std::vector<std::wstring> mLines;
};

enum class ColumnType {
    String,

    Float, // includes Decimal and Int
    Decimal, // includes Int
    Int,

    TimeStamp, // incudes Date
    Date,

    Time,
    Bool
};

template <typename T>
T& operator<<(T& ostr, const ColumnType& columnType)
{
    switch (columnType) {
    case ColumnType::Float:
        ostr << "ColumnType::Float";
        break;
    case ColumnType::Decimal:
        ostr << "ColumnType::Decimal";
        break;
    case ColumnType::Int:
        ostr << "ColumnType::Int";
        break;
    case ColumnType::TimeStamp:
        ostr << "ColumnType::TimeStamp";
        break;
    case ColumnType::Date:
        ostr << "ColumnType::Date";
        break;
    case ColumnType::Time:
        ostr << "ColumnType::Time";
        break;
    case ColumnType::Bool:
        ostr << "ColumnType::Bool";
        break;
    case ColumnType::String:
        ostr << "ColumnType::String";
        break;
    default:
        ostr << "uknown ColumnType";
        break;
    }
    return ostr;
};

class ColumnInfo {
    friend std::wostream& operator<<(std::wostream& wostr, const ColumnInfo& columnInfo);

public:
    explicit ColumnInfo(std::wstring_view name); // Constructor
    virtual ~ColumnInfo() = default; // Defaulted virtual destructor

    //  Explicitly default assignment and pass-by-value.
    ColumnInfo(const ColumnInfo& src) = default;
    ColumnInfo& operator=(const ColumnInfo& rhs) = default;

    // Explicitly default move constructor and move assignment operator.
    ColumnInfo(ColumnInfo&& src) = default;
    ColumnInfo& operator=(ColumnInfo&& rhs) = default;

    void analyzeToken(std::wstring_view token);
    static void initializeLocales();
    void update(const ColumnInfo& columnInfo);

    const std::wstring& name() const { return mName; };
    ColumnType type() const;
    bool isNull() const { return mIsAnalyzed ? mIsNull : true; };
    std::size_t maxLength() const { return mMaxLength; };
    std::size_t minLength() const { return mMinLength.value_or(0); };
    std::size_t digitsBeforeDecimalPoint() const { return mDigitsBeforeDecimalPoint.value_or(0); };
    std::size_t digitsAfterDecimalPoint() const { return mDigitsAfterDecimalPoint.value_or(0); };
    double minValue() const { return mMinValue.value_or(0); };
    double maxValue() const { return mMaxValue.value_or(0); };

private:
    void analyzeTemporal(const std::wstring& token, const std::locale& temporalLocale, bool& isTemporal);

    std::wstring mName;

    bool mIsAnalyzed { false };
    bool mIsFloat { true };
    bool mIsDecimal { true };
    bool mIsInt { true };
    bool mIsBool { true };
    bool mIsBoolInLowerCase { false };
    bool mIsDate { true };
    bool mIsTime { true };
    bool mIsTimeStamp { true };
    bool mIsNull { false };
    std::size_t mMaxLength { 0 };
    std::optional<std::size_t> mMinLength;
    std::optional<std::size_t> mDigitsBeforeDecimalPoint;
    std::optional<std::size_t> mDigitsAfterDecimalPoint;
    std::optional<double> mMinValue;
    std::optional<double> mMaxValue;

    static inline std::locale sLocaleTimeStamp;
    static inline std::locale sLocaleTime;
    static inline std::locale sLocaleDate;
};

std::wostream& operator<<(std::wostream& wostr, const ColumnInfo& columnInfo);

class ParsingResults {
    friend class CsvFileParser;
    friend std::wostream& operator<<(std::wostream& wostr, const ParsingResults& parsingResults);

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

    const std::vector<ColumnInfo>& columns() const { return mColumns; }
    std::size_t numLines() const { return mNumLines; };
    std::size_t numMalformedLines() const { return mNumMalformedLines; };

private:
    std::vector<ColumnInfo> mColumns;
    std::size_t mNumLines { 0 };
    std::size_t mNumMalformedLines { 0 };
};

std::wostream& operator<<(std::wostream& wostr, const ParsingResults& parsingResults);

class CsvFileParser {
public:
    explicit CsvFileParser(const bfs::path& inputFile); // Constructor
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

    std::atomic_bool mReaderLoopIsDone { false };
    std::atomic_bool mCharSetConversionError { false };

    std::vector<ParserBuffer> mBuffers;

    std::queue<unsigned int> mEmptyBuffers;
    std::mutex mMutexEmptyBuffers;
    std::condition_variable mConditionVarEmptyBuffers;

    std::queue<unsigned int> mFullBuffers;
    std::mutex mMutexFullBuffers;
    std::condition_variable_any mConditionVarFullBuffers;

    ParsingResults mResults;
    bool mIsResultsMovedFrom { false };
    std::shared_mutex mMutexResults;

    CsvSeparator mSep;
};
