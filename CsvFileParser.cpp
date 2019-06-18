#include "CsvFileParser.h"
#include "log.h"
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <cassert>
#include <optional>
#include <sstream>
#include <thread>

using namespace std::string_literals;

ColumnInfo::ColumnInfo(std::wstring_view name)
    : mName(name)
{
    boost::trim(mName);
}

void ParsingResults::update(const ParsingResults& results)
{
    for (std::size_t i = 0; i < mColumns.size(); ++i) {
        mColumns[i].update(results.mColumns[i]);
    };
    mNumLines = mNumLines + results.mNumLines;
    mNumMalformedLines = mNumMalformedLines + results.mNumMalformedLines;
}

void ParsingResults::addColumn(std::wstring_view name)
{
    mColumns.emplace_back(name);
}

std::wostream& operator<<(std::wostream& wostr, const ParsingResults& parsingResults)
{
    wostr << "Parsing results: " << parsingResults.mColumns.size() << " columns: ";
    auto isFirstIteration = true;
    for (const auto& columnInfo : parsingResults.mColumns) {
        if (!isFirstIteration) {
            wostr << ", ";
        }
        wostr << columnInfo;
        isFirstIteration = false;
    }
    wostr << "; " << parsingResults.mNumLines << " lines; " << parsingResults.mNumMalformedLines << " malformed lines.";
    return wostr;
};

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
    std::size_t numLines { 0 }; // Counter of lines in the file.

    // Maximum number of lines in one buffer.
#ifdef NDEBUG
    const std::size_t kMaxBufferLines { 1000 };
#else
    const std::size_t kMaxBufferLines { 10 };
#endif

    unsigned int numBufferToFill { 0 }; // The buffer #0 is going to be filled first.
    for (unsigned int i = 1; i < numThreads; ++i) { // Do not add the buffer #0 into the queue of empty buffers.
        mEmptyBuffers.push(i);
        BOOST_LOG_SEV(gLogger, bltrivial::trace) << "The buffer #" << i << " is added into the queue of empty buffers."
                                                 << FUNCTION_FILE_LINE;
    }

    auto addToFullBuffers = [this, &gLogger](unsigned int numBufferToFill) {
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
    mSep = CsvSeparator(escape, separator, quote);

    // Launch parser threads
    std::vector<std::thread> threads(numThreads);
    std::generate(threads.begin(), threads.end(), [this] { return std::thread { &CsvFileParser::parser, this }; });

    // Reader loop
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Starting the reader loop." << FUNCTION_FILE_LINE << std::flush;
    while (std::getline(inputFile, line)) {
        ++numLines;
        BOOST_LOG_SEV(gLogger, bltrivial::trace) << numLines << ' ' << line << FUNCTION_FILE_LINE;

        if (numLines == 1) {
            parseColumnNames(line);
            continue;
        }

        mBuffers.at(numBufferToFill).addLine(std::move(line));

        if (mBuffers.at(numBufferToFill).size() == kMaxBufferLines) {
            // The buffer is full, add it into the queue of full buffers.
            addToFullBuffers(numBufferToFill);

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
                << "\", line: " << numLines + 1 << ", column: " << line.length() + 1 << '.';
        BOOST_LOG_SEV(gLogger, bltrivial::debug) << line;
        BOOST_LOG_SEV(gLogger, bltrivial::error) << message.str() << FUNCTION_FILE_LINE << std::flush;
        {
            std::unique_lock lock(mMutexFullBuffers);
            mCharSetConversionError = true;
            mReaderLoopIsDone = true;
        }
    } else {
        if (mBuffers.at(numBufferToFill).size() > 0) {
            // The last buffer is partially filled, add it into the queue of full buffers.
            addToFullBuffers(numBufferToFill);
        }
        BOOST_LOG_SEV(gLogger, bltrivial::trace) << "It has been the last buffer." << FUNCTION_FILE_LINE;
    }

    {
        std::unique_lock lock(mMutexFullBuffers);
        mReaderLoopIsDone = true;
    }
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "The reader loop is done, notifying all parser threads." << FUNCTION_FILE_LINE << std::flush;
    mConditionVarFullBuffers.notify_all();

    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Starting to wait for all parser threads to finish." << FUNCTION_FILE_LINE << std::flush;
    std::for_each(threads.begin(), threads.end(), [](auto& t) { t.join(); });
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Finished waiting. All parser threads finished." << FUNCTION_FILE_LINE << std::flush;
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << mResults << FUNCTION_FILE_LINE;

    if (!inputFile.eof()) {
        BOOST_LOG_SEV(gLogger, bltrivial::error) << "Throwing exception @" << FUNCTION_FILE_LINE << std::flush;
        throw std::runtime_error(message.str());
    } else {
        BOOST_LOG_SEV(gLogger, bltrivial::debug) << "All " << numLines << " lines processed." << FUNCTION_FILE_LINE;
        BOOST_LOG_SEV(gLogger, bltrivial::trace) << "<-" << FUNCTION_FILE_LINE << std::flush;
        return std::move(mResults);
    }
}

void CsvFileParser::parser()
{
    auto& gLogger = GlobalLogger::get();
    auto exitParserLoop = [this] { return (mReaderLoopIsDone && (mFullBuffers.size() == 0)) || mCharSetConversionError; };

    // Parser loop
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Starting the parser loop." << FUNCTION_FILE_LINE << std::flush;
    while (true) {
        std::optional<unsigned int> numBufferToParse;
        numBufferToParse.reset(); // TODO: comment out this line and rerun the tests

        // Part 1. Try to get the number of the next full buffer to parse.
        {
            BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Lock" << FUNCTION_FILE_LINE;
            std::unique_lock lock(mMutexFullBuffers);

            if (exitParserLoop()) {
                BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Exiting the parser loop." << FUNCTION_FILE_LINE;
                break;
            }

            if (mFullBuffers.size() == 0) {
                BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Starting to wait for a full buffer." << FUNCTION_FILE_LINE;
                mConditionVarFullBuffers.wait(lock);
                BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Finished waiting for a full buffer." << FUNCTION_FILE_LINE;
            }

            if (exitParserLoop()) {
                BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Exiting the parser loop." << FUNCTION_FILE_LINE;
                break;
            }

            if (mFullBuffers.size() > 0) {
                // Get the number of the next full buffer to parse.
                numBufferToParse = mFullBuffers.front();
                assert(mBuffers.at(numBufferToParse.value()).size() > 0);
                mFullBuffers.pop();
                BOOST_LOG_SEV(gLogger, bltrivial::trace) << "The buffer #" << numBufferToParse.value() << " is removed from the queue of full buffers."
                                                         << FUNCTION_FILE_LINE;
            }
        }

        if (numBufferToParse.has_value()) {
            // Part 2. Parse the full buffer given its number.

            ParsingResults results;
            {
                BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Lock" << FUNCTION_FILE_LINE;
                std::shared_lock lock(mMutexResults);
                results = mResults;
            }
            BOOST_LOG_SEV(gLogger, bltrivial::trace) << results << FUNCTION_FILE_LINE;
            parseBuffer(numBufferToParse.value(), results);
            BOOST_LOG_SEV(gLogger, bltrivial::trace) << results << FUNCTION_FILE_LINE;
            {
                BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Lock" << FUNCTION_FILE_LINE;
                std::unique_lock lock(mMutexResults);
                mResults.update(results);
            }

            // Part 3. Clear the parsed buffer and return it into the queue of empty buffers.

            mBuffers.at(numBufferToParse.value()).clear();
            BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Cleared the buffer #" << numBufferToParse.value() << FUNCTION_FILE_LINE;

            {
                BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Lock" << FUNCTION_FILE_LINE;
                std::unique_lock lock(mMutexEmptyBuffers);
                assert(mBuffers.at(numBufferToParse.value()).size() == 0);
                mEmptyBuffers.push(numBufferToParse.value());
                BOOST_LOG_SEV(gLogger, bltrivial::trace) << "The buffer #" << numBufferToParse.value() << " is added into the queue of empty buffers."
                                                         << FUNCTION_FILE_LINE;
            }
            mConditionVarEmptyBuffers.notify_one();
        }
    }
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Finished the parser loop." << FUNCTION_FILE_LINE << std::flush;
}

void CsvFileParser::parseBuffer(unsigned int numBufferToParse, ParsingResults& results)
{
    auto& gLogger = GlobalLogger::get();
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "->" << FUNCTION_FILE_LINE;
    assert(mBuffers.at(numBufferToParse).size() > 0);
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "Starting to parse the buffer #" << numBufferToParse << "." << FUNCTION_FILE_LINE;

    CsvTokenizer tok(L""s, mSep);
    for (const auto& line : mBuffers.at(numBufferToParse).mLines) {
        std::size_t columnNumber { 0 };
        tok.assign(line);
        for (auto beg = tok.begin(); beg != tok.end(); ++beg) {
            // BOOST_LOG_SEV(gLogger, bltrivial::trace) << *beg;
            auto token = *beg;
            if (columnNumber < results.mColumns.size()) {
                results.mColumns[columnNumber].analyzeToken(token);
            }
            ++columnNumber;
        }
        ++results.mNumLines;
        if (columnNumber != results.mColumns.size()) {
            ++results.mNumMalformedLines;
        }
    }

    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "The buffer #" << numBufferToParse << " is parsed." << FUNCTION_FILE_LINE;
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "<-" << FUNCTION_FILE_LINE;
}

void CsvFileParser::parseColumnNames(std::wstring_view line)
{
    auto& gLogger = GlobalLogger::get();
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "->" << FUNCTION_FILE_LINE << std::flush;
    CsvTokenizer tok(line, mSep);
    for (auto beg = tok.begin(); beg != tok.end(); ++beg) {
        BOOST_LOG_SEV(gLogger, bltrivial::trace) << *beg;
        auto name = *beg;
        boost::trim(name);
        mResults.addColumn(name);
    }
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << "<-" << FUNCTION_FILE_LINE << std::flush;
}

void ColumnInfo::initializeLocales()
{
    static bool initialized { false };
    if (!initialized) {
        auto timeStampFacet = new bpt::wtime_input_facet(L"%Y-%m-%d %H:%M:%S%F");
        sLocaleTimeStamp = std::locale(std::locale(), timeStampFacet);

        auto timeFacet = new bpt::wtime_input_facet(L"%H:%M:%S%F");
        sLocaleTime = std::locale(std::locale(), timeFacet);

        auto dateFacet = new bpt::wtime_input_facet(L"%Y-%m-%d");
        sLocaleDate = std::locale(std::locale(), dateFacet);

        initialized = true;
    }
};

void ColumnInfo::analyzeTemporal(const std::wstring& token, const std::locale& temporalLocale, bool& isTemporal)
{
    std::wistringstream stringStream(token);
    stringStream.imbue(temporalLocale);
    assert(!stringStream.eof());
    bpt::ptime posixTime;
    stringStream >> posixTime;
    if (!stringStream.fail()) {
        stringStream.get();
        isTemporal = stringStream.eof(); // there must be no more characters after the parsed ones
    } else {
        isTemporal = false;
    }
};

void ColumnInfo::analyzeToken(std::wstring_view token)
{
    if (!mAnalyzed) {
        mAnalyzed = true;
    };

    auto lengthToken = token.length();
    if (lengthToken > mMaxLength) {
        mMaxLength = lengthToken;
    }
    if ((lengthToken > 0) && ((!mMinLength.has_value()) || (lengthToken < mMinLength))) {
        mMinLength = lengthToken;
    }

    std::wstring tokenTrim(token);
    boost::trim(tokenTrim);
    auto lengthTokenTrim = tokenTrim.length();

    if (lengthTokenTrim > 0) {
        if (mIsFloat) {
            if (boost::istarts_with(tokenTrim, L"0x") || boost::icontains(tokenTrim, L"p")) {
                // Hexadecimal floating-point literals are accepted in C++17
                // (https://en.cppreference.com/w/cpp/language/floating_literal), but may not be accepted
                // by a database during import from a CSV file, so we treat them as strings.
                mIsFloat = mIsDecimal = mIsInt = false;
            } else {
                try {
                    auto value = boost::lexical_cast<double>(tokenTrim);

                    if ((!mMinVal.has_value()) || (value < mMinVal)) {
                        mMinVal = value;
                    }

                    if ((!mMaxVal.has_value()) || (value > mMaxVal)) {
                        mMaxVal = value;
                    }

                    if (mIsDecimal) {
                        if (boost::icontains(tokenTrim, L"e")) {
                            mIsDecimal = mIsInt = false;
                        } else {
                            if (mIsInt) {
                                try {
                                    boost::lexical_cast<long long>(tokenTrim);
                                } catch (const boost::bad_lexical_cast& e) {
                                    mIsInt = false;
                                }
                            }

                            auto posDecimalPoint = tokenTrim.find(L'.');
                            if (posDecimalPoint != std::string::npos) {
                                if ((!mDigitsBeforeDecimalPoint.has_value()) || (posDecimalPoint > mDigitsBeforeDecimalPoint.value())) {
                                    mDigitsBeforeDecimalPoint = posDecimalPoint;
                                }

                                auto len = lengthTokenTrim - posDecimalPoint - 1;
                                if ((!mDigitsAfterDecimalPoint.has_value()) || (len > mDigitsAfterDecimalPoint)) {
                                    mDigitsAfterDecimalPoint = len;
                                }
                            } else {
                                if ((!mDigitsBeforeDecimalPoint.has_value()) || (lengthTokenTrim > mDigitsBeforeDecimalPoint.value())) {
                                    mDigitsBeforeDecimalPoint = lengthTokenTrim;
                                }

                                if (!mDigitsAfterDecimalPoint.has_value()) {
                                    mDigitsAfterDecimalPoint = 0;
                                }
                            }
                            if (value < 0) {
                                mDigitsBeforeDecimalPoint = mDigitsBeforeDecimalPoint.value() - 1;
                            }
                            assert(mDigitsBeforeDecimalPoint.value() >= 0);
                            assert(mDigitsAfterDecimalPoint.value() >= 0);
                        }
                    }
                } catch (const boost::bad_lexical_cast& e) {
                    mIsFloat = mIsDecimal = mIsInt = false;
                }
            }
        }

        if (mIsTimeStamp) {
            analyzeTemporal(tokenTrim, ColumnInfo::sLocaleTimeStamp, mIsTimeStamp);
            if (mIsTimeStamp) {
                if (mIsDate) {
                    analyzeTemporal(tokenTrim, ColumnInfo::sLocaleDate, mIsDate);
                }
            } else {
                mIsDate = false;
            }
        }

        if (mIsTime) {
            analyzeTemporal(tokenTrim, ColumnInfo::sLocaleTime, mIsTime);
        }

        if (mIsBool) {
            if ((tokenTrim != L"true") && (tokenTrim != L"false")) {
                mIsBool = false;
            }
        }
    } else {
        if (!mIsNull) {
            mIsNull = true;
        }
    }
};

ColumnType ColumnInfo::type() const
{
    if (mAnalyzed) {
        if (mIsFloat) {
            if (mIsInt) {
                return ColumnType::Int;
            } else if (mIsDecimal) {
                return ColumnType::Decimal;
            } else {
                return ColumnType::Float;
            }
        } else if (mIsTimeStamp) {
            if (mIsDate) {
                return ColumnType::Date;
            } else {
                return ColumnType::TimeStamp;
            }
        } else if (mIsTime) {
            return ColumnType::Time;
        } else if (mIsBool) {
            return ColumnType::Bool;
        } else {
            return ColumnType::String;
        }
    } else {
        return ColumnType::String;
    }
};

bool ColumnInfo::IsNull()
{
    if (mAnalyzed) {
        return mIsNull;
    } else {
        return true;
    }
};

void ColumnInfo::update(const ColumnInfo& columnInfo)
{
    assert(columnInfo.mAnalyzed);
    if (!mAnalyzed) {
        mAnalyzed = columnInfo.mAnalyzed;
    }

    mIsFloat = mIsFloat && columnInfo.mIsFloat;
    mIsDecimal = mIsDecimal && columnInfo.mIsDecimal;
    mIsInt = mIsInt && columnInfo.mIsInt;
    mIsBool = mIsBool && columnInfo.mIsBool;
    mIsDate = mIsDate && columnInfo.mIsDate;
    mIsTime = mIsTime && columnInfo.mIsTime;
    mIsTimeStamp = mIsTimeStamp && columnInfo.mIsTimeStamp;

    mIsNull = mIsNull || columnInfo.mIsNull;

    if (mMaxLength < columnInfo.mMaxLength) {
        mMaxLength = columnInfo.mMaxLength;
    }
    if ((columnInfo.mMinLength.has_value()) && (mMinLength > columnInfo.mMinLength)) {
        mMinLength = columnInfo.mMinLength;
    }
    if ((columnInfo.mDigitsBeforeDecimalPoint.has_value()) && (mDigitsBeforeDecimalPoint < columnInfo.mDigitsBeforeDecimalPoint)) {
        mDigitsBeforeDecimalPoint = columnInfo.mDigitsBeforeDecimalPoint;
    }
    if ((columnInfo.mDigitsAfterDecimalPoint.has_value()) && (mDigitsAfterDecimalPoint < columnInfo.mDigitsAfterDecimalPoint)) {
        mDigitsAfterDecimalPoint = columnInfo.mDigitsAfterDecimalPoint;
    }
    if ((columnInfo.mMinVal.has_value()) && (mMinVal > columnInfo.mMinVal)) {
        mMinVal = columnInfo.mMinVal;
    }
    if ((columnInfo.mMaxVal.has_value()) && (mMaxVal < columnInfo.mMaxVal)) {
        mMaxVal = columnInfo.mMaxVal;
    }
};

std::wostream& operator<<(std::wostream& wostr, const ColumnInfo& columnInfo)
{
    wostr << columnInfo.mName;
    if (columnInfo.mAnalyzed) {
        wostr << " (" << columnInfo.type();
        if (columnInfo.mMinLength.has_value()) {
            wostr << ", min length = " << columnInfo.mMinLength.value();
        }
        wostr << ", max length = " << columnInfo.mMaxLength;
        if (columnInfo.mDigitsBeforeDecimalPoint.has_value()) {
            wostr << ", digits before decimal = " << columnInfo.mDigitsBeforeDecimalPoint.value();
        }
        if (columnInfo.mDigitsAfterDecimalPoint.has_value()) {
            wostr << ", digits after decimal = " << columnInfo.mDigitsAfterDecimalPoint.value();
        }
        if (columnInfo.mMinVal.has_value()) {
            wostr << ", min value = " << columnInfo.mMinVal.value();
        }
        if (columnInfo.mMaxVal.has_value()) {
            wostr << ", max value = " << columnInfo.mMaxVal.value();
        }
        wostr << ')';
    }
    return wostr;
};
