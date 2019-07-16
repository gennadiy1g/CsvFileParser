// for /l %i in (1,1,100) do echo %i & time /t & CsvFileParser.exe

#define BOOST_TEST_MODULE Master Test Suite

#include <boost/algorithm/string.hpp>
#include <boost/test/unit_test.hpp>
#include <numeric>
#include <string>
#include <thread>

#include "CsvFileParser.h"
#include "MonetDBBulkLoader.h"
#include "log.h"
#include "utilities.h"

using namespace std::string_literals;
using namespace std::literals;

struct GlobalFixture {
    GlobalFixture() {}

    ~GlobalFixture() {}

    void setup()
    {
        initLocalization();

        initLogging();

        auto& gLogger = GlobalLogger::get();
        BOOST_LOG_SEV(gLogger, bltrivial::trace) << "->" << FUNCTION_FILE_LINE;
        BOOST_LOG_SEV(gLogger, bltrivial::trace) << L"Привіт Світ! " << FUNCTION_FILE_LINE;

        auto backends = blocale::localization_backend_manager::global().get_all_backends();
        std::string backendsList = std::accumulate(backends.cbegin(), backends.cend(), ""s,
            [](const std::string& a, const std::string& b) { return a + (a == "" ? "" : ", ") + b; });
        BOOST_LOG_SEV(gLogger, bltrivial::debug) << "Localization backends: " << backendsList << '.';

        BOOST_LOG_SEV(gLogger, bltrivial::info) << std::thread::hardware_concurrency() << " concurrent threads are supported.";

        BOOST_TEST_MESSAGE("Calling ColumnInfo::initializeLocales()");
        ColumnInfo::initializeLocales();
    }

    void teardown() {}
};

BOOST_TEST_GLOBAL_FIXTURE(GlobalFixture);

BOOST_AUTO_TEST_SUITE(CsvFileParser_parse);

BOOST_AUTO_TEST_CASE(ZX0training_UTF8)
{
    CsvFileParser parser(LR"^(C:\Users\genna_000\Documents\Experiments\test data\ZX0training_UTF-8.csv)^");
    BOOST_REQUIRE_NO_THROW(parser.parse(L',', L'"', L'\\'));
}

BOOST_AUTO_TEST_CASE(russian_UTF8)
{
    CsvFileParser parser(LR"^(C:\Users\genna_000\Documents\Experiments\test data\russian_UTF-8.csv)^");
    BOOST_REQUIRE_NO_THROW(parser.parse(L',', L'"', L'\\'));
}

BOOST_AUTO_TEST_CASE(ZX0training_CP863)
{
    auto pred = [](const std::exception& e) {
        return boost::contains(e.what(), "Character set conversion error!"s) && boost::contains(e.what(), "line: 111, column: 144."s);
    };

    // Processing of this file causes an exception to be thrown
    CsvFileParser parser(LR"^(C:\Users\genna_000\Documents\Experiments\test data\ZX0training_CP-863.csv)^");
    BOOST_REQUIRE_EXCEPTION(parser.parse(L',', L'"', L'\\'), std::runtime_error, pred);
}

BOOST_AUTO_TEST_SUITE_END();

struct TestSuiteFixtureAnalyzeToken {
    TestSuiteFixtureAnalyzeToken() {}
    ~TestSuiteFixtureAnalyzeToken() {}
};

BOOST_AUTO_TEST_SUITE(ColumnInfo_analyzeToken, *boost::unit_test::fixture<TestSuiteFixtureAnalyzeToken>());

BOOST_AUTO_TEST_CASE(float_decimal_int)
{
    {
        ColumnInfo columnInfo(L"column1"s);
        BOOST_TEST(columnInfo.type() == ColumnType::String);
        BOOST_CHECK(columnInfo.isNull());

        columnInfo.analyzeToken(L" 0 "s);
        BOOST_TEST(columnInfo.type() == ColumnType::Int);
        BOOST_CHECK(!columnInfo.isNull());
        BOOST_TEST(columnInfo.digitsBeforeDecimalPoint() == 1);
        BOOST_TEST(columnInfo.digitsAfterDecimalPoint() == 0);
        BOOST_TEST(columnInfo.minLength() == 3);

        columnInfo.analyzeToken(L" "s);
        BOOST_TEST(columnInfo.type() == ColumnType::Int);
        BOOST_CHECK(columnInfo.isNull());
        BOOST_TEST(columnInfo.minLength() == 1);

        columnInfo.analyzeToken(L""s);
        BOOST_TEST(columnInfo.type() == ColumnType::Int);
        BOOST_CHECK(columnInfo.isNull());
        BOOST_TEST(columnInfo.minLength() == 1);

        columnInfo.analyzeToken(L" 1 "s);
        BOOST_TEST(columnInfo.type() == ColumnType::Int);
        BOOST_CHECK(columnInfo.isNull());
        BOOST_TEST(columnInfo.digitsBeforeDecimalPoint() == 1);
        BOOST_TEST(columnInfo.digitsAfterDecimalPoint() == 0);

        columnInfo.analyzeToken(L" -25 "s);
        BOOST_TEST(columnInfo.type() == ColumnType::Int);
        BOOST_CHECK(columnInfo.isNull());
        BOOST_TEST(columnInfo.digitsBeforeDecimalPoint() == 2);
        BOOST_TEST(columnInfo.digitsAfterDecimalPoint() == 0);

        columnInfo.analyzeToken(L" 100 "s);
        BOOST_TEST(columnInfo.type() == ColumnType::Int);
        BOOST_CHECK(columnInfo.isNull());
        BOOST_TEST(columnInfo.digitsBeforeDecimalPoint() == 3);
        BOOST_TEST(columnInfo.digitsAfterDecimalPoint() == 0);

        columnInfo.analyzeToken(L" -123 "s);
        BOOST_TEST(columnInfo.type() == ColumnType::Int);
        BOOST_CHECK(columnInfo.isNull());
        BOOST_TEST(columnInfo.digitsBeforeDecimalPoint() == 3);
        BOOST_TEST(columnInfo.digitsAfterDecimalPoint() == 0);

        columnInfo.analyzeToken(L" 25 "s);
        BOOST_TEST(columnInfo.type() == ColumnType::Int);
        BOOST_CHECK(columnInfo.isNull());
        BOOST_TEST(columnInfo.digitsBeforeDecimalPoint() == 3);
        BOOST_TEST(columnInfo.digitsAfterDecimalPoint() == 0);

        columnInfo.analyzeToken(L" 25. "s);
        BOOST_TEST(columnInfo.type() == ColumnType::Decimal);
        BOOST_CHECK(columnInfo.isNull());
        BOOST_TEST(columnInfo.digitsBeforeDecimalPoint() == 3);
        BOOST_TEST(columnInfo.digitsAfterDecimalPoint() == 0);

        columnInfo.analyzeToken(L" 25.00 "s);
        BOOST_TEST(columnInfo.type() == ColumnType::Decimal);
        BOOST_CHECK(columnInfo.isNull());
        BOOST_TEST(columnInfo.digitsBeforeDecimalPoint() == 3);
        BOOST_TEST(columnInfo.digitsAfterDecimalPoint() == 2);

        columnInfo.analyzeToken(L" -150.00 "s);
        BOOST_TEST(columnInfo.type() == ColumnType::Decimal);
        BOOST_CHECK(columnInfo.isNull());
        BOOST_TEST(columnInfo.digitsBeforeDecimalPoint() == 3);
        BOOST_TEST(columnInfo.digitsAfterDecimalPoint() == 2);

        columnInfo.analyzeToken(L" -1234.123 "s);
        BOOST_TEST(columnInfo.type() == ColumnType::Decimal);
        BOOST_CHECK(columnInfo.isNull());
        BOOST_TEST(columnInfo.digitsBeforeDecimalPoint() == 4);
        BOOST_TEST(columnInfo.digitsAfterDecimalPoint() == 3);

        columnInfo.analyzeToken(L" -12345 "s);
        BOOST_TEST(columnInfo.type() == ColumnType::Decimal);
        BOOST_CHECK(columnInfo.isNull());
        BOOST_TEST(columnInfo.digitsBeforeDecimalPoint() == 5);
        BOOST_TEST(columnInfo.digitsAfterDecimalPoint() == 3);

        columnInfo.analyzeToken(L"0.1e-1"s);
        BOOST_TEST(columnInfo.type() == ColumnType::Float);
        BOOST_CHECK(columnInfo.isNull());

        columnInfo.analyzeToken(L"0X0p-1"s);
        BOOST_TEST(columnInfo.type() == ColumnType::String);
        BOOST_CHECK(columnInfo.isNull());
        BOOST_TEST(columnInfo.maxLength() == 11);

        columnInfo.analyzeToken(L"123456789"s);
        BOOST_TEST(columnInfo.type() == ColumnType::String);
        BOOST_CHECK(columnInfo.isNull());
        BOOST_TEST(columnInfo.digitsBeforeDecimalPoint() == 5);
        BOOST_TEST(columnInfo.digitsAfterDecimalPoint() == 3);
        BOOST_TEST(columnInfo.maxLength() == 11);
        BOOST_TEST(columnInfo.minLength() == 1);
    }
    {
        ColumnInfo columnInfo(L"column2"s);
        BOOST_TEST(columnInfo.type() == ColumnType::String);
        BOOST_CHECK(columnInfo.isNull());

        columnInfo.analyzeToken(L" -.00001 "s);
        BOOST_TEST(columnInfo.type() == ColumnType::Decimal);
        BOOST_CHECK(!columnInfo.isNull());
        BOOST_TEST(columnInfo.digitsBeforeDecimalPoint() == 0);
        BOOST_TEST(columnInfo.digitsAfterDecimalPoint() == 5);

        columnInfo.analyzeToken(L"123456789"s);
        BOOST_TEST(columnInfo.type() == ColumnType::Decimal);
        BOOST_CHECK(!columnInfo.isNull());
        BOOST_TEST(columnInfo.digitsBeforeDecimalPoint() == 9);
        BOOST_TEST(columnInfo.digitsAfterDecimalPoint() == 5);
    }
}

BOOST_AUTO_TEST_CASE(time_stamp)
{
    ColumnInfo columnInfo(L"column1"s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(columnInfo.isNull());

    columnInfo.analyzeToken(L" 2019-02-28 23:59:59.999 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::TimeStamp);
    BOOST_CHECK(!columnInfo.isNull());

    columnInfo.analyzeToken(L" "s);
    BOOST_TEST(columnInfo.type() == ColumnType::TimeStamp);
    BOOST_CHECK(columnInfo.isNull());

    columnInfo.analyzeToken(L" 2019-02-28 23:59:59.999 foo "s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(columnInfo.isNull());

    columnInfo.analyzeToken(L" 2019-02-28 23:59:59.999 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(columnInfo.isNull());
}

BOOST_AUTO_TEST_CASE(time_stamp2)
{
    ColumnInfo columnInfo(L"column1"s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(columnInfo.isNull());

    columnInfo.analyzeToken(L" 2019-02-28 23:59:59.999 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::TimeStamp);
    BOOST_CHECK(!columnInfo.isNull());

    columnInfo.analyzeToken(L" 2019-02-28 23:59:59 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::TimeStamp);
    BOOST_CHECK(!columnInfo.isNull());

    columnInfo.analyzeToken(L" 2019-02-28 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::TimeStamp);
    BOOST_CHECK(!columnInfo.isNull());

    columnInfo.analyzeToken(L" 2019-02-28 23:59:59.999 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::TimeStamp);
    BOOST_CHECK(!columnInfo.isNull());

    columnInfo.analyzeToken(L" 2019-02-28 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::TimeStamp);
    BOOST_CHECK(!columnInfo.isNull());

    columnInfo.analyzeToken(L" 23:59:59.999 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(!columnInfo.isNull());

    columnInfo.analyzeToken(L" 2019-02-28 23:59:59.999 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(!columnInfo.isNull());
}

BOOST_AUTO_TEST_CASE(time)
{
    ColumnInfo columnInfo(L"column1"s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(columnInfo.isNull());

    columnInfo.analyzeToken(L" 23:59:59.999 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::Time);
    BOOST_CHECK(!columnInfo.isNull());

    columnInfo.analyzeToken(L" "s);
    BOOST_TEST(columnInfo.type() == ColumnType::Time);
    BOOST_CHECK(columnInfo.isNull());

    columnInfo.analyzeToken(L" 23:59:59.999 foo "s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(columnInfo.isNull());

    columnInfo.analyzeToken(L" 23:59:59.999 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(columnInfo.isNull());
}

BOOST_AUTO_TEST_CASE(time_2)
{
    ColumnInfo columnInfo(L"column1"s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(columnInfo.isNull());

    columnInfo.analyzeToken(L" 23:59:59.999 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::Time);
    BOOST_CHECK(!columnInfo.isNull());

    columnInfo.analyzeToken(L" 23:59:59 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::Time);
    BOOST_CHECK(!columnInfo.isNull());

    columnInfo.analyzeToken(L" 2019-02-28 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(!columnInfo.isNull());

    columnInfo.analyzeToken(L" 23:59:59.999 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(!columnInfo.isNull());
}

BOOST_AUTO_TEST_CASE(date)
{
    ColumnInfo columnInfo(L"column1"s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(columnInfo.isNull());

    columnInfo.analyzeToken(L" 2019-02-28 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::Date);
    BOOST_CHECK(!columnInfo.isNull());

    columnInfo.analyzeToken(L" "s);
    BOOST_TEST(columnInfo.type() == ColumnType::Date);
    BOOST_CHECK(columnInfo.isNull());

    columnInfo.analyzeToken(L" 2019-02-28 23:59:59.999 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::TimeStamp);
    BOOST_CHECK(columnInfo.isNull());

    columnInfo.analyzeToken(L" 2019-02-28 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::TimeStamp);
    BOOST_CHECK(columnInfo.isNull());

    columnInfo.analyzeToken(L" 23:59:59.999 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(columnInfo.isNull());

    columnInfo.analyzeToken(L" 2019-02-28 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(columnInfo.isNull());
}

BOOST_AUTO_TEST_CASE(date2)
{
    ColumnInfo columnInfo(L"column1"s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(columnInfo.isNull());

    columnInfo.analyzeToken(L" 2019-02-28 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::Date);
    BOOST_CHECK(!columnInfo.isNull());

    columnInfo.analyzeToken(L" 23:59:59 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(!columnInfo.isNull());

    columnInfo.analyzeToken(L" 2019-02-28 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(!columnInfo.isNull());
}

BOOST_AUTO_TEST_CASE(bool_1)
{
    ColumnInfo columnInfo(L"column1"s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(columnInfo.isNull());

    columnInfo.analyzeToken(L" true "s);
    BOOST_TEST(columnInfo.type() == ColumnType::Bool);
    BOOST_CHECK(!columnInfo.isNull());

    columnInfo.analyzeToken(L" false "s);
    BOOST_TEST(columnInfo.type() == ColumnType::Bool);
    BOOST_CHECK(!columnInfo.isNull());

    columnInfo.analyzeToken(L" TRUE "s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(!columnInfo.isNull());

    columnInfo.analyzeToken(L" false "s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(!columnInfo.isNull());
}

BOOST_AUTO_TEST_CASE(bool_2)
{
    ColumnInfo columnInfo(L"column1"s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(columnInfo.isNull());

    columnInfo.analyzeToken(L" TRUE "s);
    BOOST_TEST(columnInfo.type() == ColumnType::Bool);
    BOOST_CHECK(!columnInfo.isNull());

    columnInfo.analyzeToken(L" FALSE "s);
    BOOST_TEST(columnInfo.type() == ColumnType::Bool);
    BOOST_CHECK(!columnInfo.isNull());

    columnInfo.analyzeToken(L" true "s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(!columnInfo.isNull());

    columnInfo.analyzeToken(L" FALSE "s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(!columnInfo.isNull());
}

BOOST_AUTO_TEST_CASE(bool_3)
{
    ColumnInfo columnInfo(L"column1"s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(columnInfo.isNull());

    columnInfo.analyzeToken(L" true "s);
    BOOST_TEST(columnInfo.type() == ColumnType::Bool);
    BOOST_CHECK(!columnInfo.isNull());

    columnInfo.analyzeToken(L" false "s);
    BOOST_TEST(columnInfo.type() == ColumnType::Bool);
    BOOST_CHECK(!columnInfo.isNull());

    columnInfo.analyzeToken(L" True "s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(!columnInfo.isNull());

    columnInfo.analyzeToken(L" false "s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(!columnInfo.isNull());
}

BOOST_AUTO_TEST_SUITE_END();

BOOST_AUTO_TEST_SUITE(parsing_results);

BOOST_AUTO_TEST_CASE(parsing_results_1)
{
    CsvFileParser parser(LR"^(C:\Users\genna_000\Documents\Experiments\test data\parsing_results_1.csv)^");
    auto parsingResults = parser.parse(L',', L'"', L'\\');
    BOOST_TEST(parsingResults.numLines() == 10);
    BOOST_TEST(parsingResults.numMalformedLines() == 0);
    const auto& columns = parsingResults.columns();
    BOOST_TEST(columns.size() == 7);

    BOOST_TEST(columns[0].name().compare(L"col_str") == 0);
    BOOST_TEST(columns[0].type() == ColumnType::String);
    BOOST_TEST(columns[0].minLength() == 3);
    BOOST_TEST(columns[0].maxLength() == 5);
    BOOST_CHECK(!columns[0].isNull());

    BOOST_TEST(columns[1].name().compare(L"col_int") == 0);
    BOOST_TEST(columns[1].type() == ColumnType::Int);
    BOOST_TEST(columns[1].minValue() == -789);
    BOOST_TEST(columns[1].maxValue() == 1200);
    BOOST_TEST(columns[1].digitsBeforeDecimalPoint() == 4);
    BOOST_TEST(columns[1].digitsAfterDecimalPoint() == 0);
    BOOST_TEST(columns[1].minLength() == 2);
    BOOST_TEST(columns[1].maxLength() == 4);
    BOOST_CHECK(!columns[1].isNull());

    BOOST_TEST(columns[2].name().compare(L"col_date") == 0);
    BOOST_TEST(columns[2].type() == ColumnType::Date);
    BOOST_TEST(columns[2].minLength() == 10);
    BOOST_TEST(columns[2].maxLength() == 10);
    BOOST_CHECK(!columns[2].isNull());

    BOOST_TEST(columns[3].name().compare(L"col_time") == 0);
    BOOST_TEST(columns[3].type() == ColumnType::Time);
    BOOST_TEST(columns[3].minLength() == 8);
    BOOST_TEST(columns[3].maxLength() == 8);
    BOOST_CHECK(!columns[3].isNull());

    BOOST_TEST(columns[4].name().compare(L"col_time_stamp") == 0);
    BOOST_TEST(columns[4].type() == ColumnType::TimeStamp);
    BOOST_TEST(columns[4].minLength() == 16);
    BOOST_TEST(columns[4].maxLength() == 16);
    BOOST_CHECK(!columns[4].isNull());

    BOOST_TEST(columns[5].name().compare(L"col_decimal") == 0);
    BOOST_TEST(columns[5].type() == ColumnType::Decimal);
    BOOST_TEST(columns[5].minValue() == -48.05);
    BOOST_TEST(columns[5].maxValue() == 125.66);
    BOOST_TEST(columns[5].digitsBeforeDecimalPoint() == 3);
    BOOST_TEST(columns[5].digitsAfterDecimalPoint() == 5);
    BOOST_TEST(columns[5].minLength() == 2);
    BOOST_TEST(columns[5].maxLength() == 8);
    BOOST_CHECK(!columns[5].isNull());

    BOOST_TEST(columns[6].name().compare(L"col_bool") == 0);
    BOOST_CHECK(!columns[6].isNull());
}

BOOST_AUTO_TEST_CASE(parsing_results_2)
{
    CsvFileParser parser(LR"^(C:\Users\genna_000\Documents\Experiments\test data\parsing_results_2.csv)^");
    auto parsingResults = parser.parse(L',', L'"', L'\\');
    BOOST_TEST(parsingResults.numLines() == 20);
    BOOST_TEST(parsingResults.numMalformedLines() == 0);
    const auto& columns = parsingResults.columns();
    BOOST_TEST(columns.size() == 7);

    BOOST_TEST(columns[0].name().compare(L"col_str") == 0);
    BOOST_TEST(columns[0].type() == ColumnType::String);
    BOOST_TEST(columns[0].minLength() == 1);
    BOOST_TEST(columns[0].maxLength() == 7);
    BOOST_CHECK(columns[0].isNull());

    BOOST_TEST(columns[1].name().compare(L"col_int") == 0);
    BOOST_TEST(columns[1].type() == ColumnType::Int);
    BOOST_TEST(columns[1].minValue() == -1031);
    BOOST_TEST(columns[1].maxValue() == 1201);
    BOOST_TEST(columns[1].digitsBeforeDecimalPoint() == 4);
    BOOST_TEST(columns[1].digitsAfterDecimalPoint() == 0);
    BOOST_TEST(columns[1].minLength() == 2);
    BOOST_TEST(columns[1].maxLength() == 5);
    BOOST_CHECK(columns[1].isNull());

    BOOST_TEST(columns[2].name().compare(L"col_date") == 0);
    BOOST_TEST(columns[2].type() == ColumnType::Date);
    BOOST_TEST(columns[2].minLength() == 10);
    BOOST_TEST(columns[2].maxLength() == 10);
    BOOST_CHECK(columns[2].isNull());

    BOOST_TEST(columns[3].name().compare(L"col_time") == 0);
    BOOST_TEST(columns[3].type() == ColumnType::Time);
    BOOST_TEST(columns[3].minLength() == 8);
    BOOST_TEST(columns[3].maxLength() == 8);
    BOOST_CHECK(columns[3].isNull());

    BOOST_TEST(columns[4].name().compare(L"col_time_stamp") == 0);
    BOOST_TEST(columns[4].type() == ColumnType::TimeStamp);
    BOOST_TEST(columns[4].minLength() == 16);
    BOOST_TEST(columns[4].maxLength() == 16);
    BOOST_CHECK(columns[4].isNull());

    BOOST_TEST(columns[5].name().compare(L"col_decimal") == 0);
    BOOST_TEST(columns[5].type() == ColumnType::Decimal);
    BOOST_TEST(columns[5].minValue() == -48.123456);
    BOOST_TEST(columns[5].maxValue() == 1234.1234);
    BOOST_TEST(columns[5].digitsBeforeDecimalPoint() == 4);
    BOOST_TEST(columns[5].digitsAfterDecimalPoint() == 6);
    BOOST_TEST(columns[5].minLength() == 2);
    BOOST_TEST(columns[5].maxLength() == 10);
    BOOST_CHECK(columns[5].isNull());

    BOOST_TEST(columns[6].name().compare(L"col_bool") == 0);
    BOOST_TEST(columns[6].type() == ColumnType::Bool);
    BOOST_CHECK(columns[6].isNull());
}

BOOST_AUTO_TEST_CASE(parsing_results_3)
{
    CsvFileParser parser(LR"^(C:\Users\genna_000\Documents\Experiments\test data\parsing_results_3.csv)^");
    auto parsingResults = parser.parse(L'\t', L'"', L'\\');
    BOOST_TEST(parsingResults.numLines() == 65);
    BOOST_TEST(parsingResults.numMalformedLines() == 3);
    const auto& columns = parsingResults.columns();
    BOOST_TEST(columns.size() == 7);

    BOOST_TEST(columns[0].name().compare(L"Col_1") == 0);
    BOOST_TEST(columns[0].type() == ColumnType::TimeStamp);
    BOOST_TEST(columns[0].minLength() == 23);
    BOOST_TEST(columns[0].maxLength() == 23);
    BOOST_CHECK(!columns[0].isNull());

    BOOST_TEST(columns[1].name().compare(L"Col_2") == 0);
    BOOST_TEST(columns[1].type() == ColumnType::Date);
    BOOST_TEST(columns[1].minLength() == 10);
    BOOST_TEST(columns[1].maxLength() == 10);
    BOOST_CHECK(!columns[1].isNull());

    BOOST_TEST(columns[2].name().compare(L"Col_3") == 0);
    BOOST_TEST(columns[2].type() == ColumnType::Time);
    BOOST_TEST(columns[2].minLength() == 12);
    BOOST_TEST(columns[2].maxLength() == 12);
    BOOST_CHECK(!columns[2].isNull());

    BOOST_TEST(columns[3].name().compare(L"Col_4") == 0);
    BOOST_TEST(columns[3].type() == ColumnType::Int);
    BOOST_TEST(columns[3].minValue() == -500);
    BOOST_TEST(columns[3].maxValue() == 4812);
    BOOST_TEST(columns[3].digitsBeforeDecimalPoint() == 4);
    BOOST_TEST(columns[3].digitsAfterDecimalPoint() == 0);
    BOOST_TEST(columns[3].minLength() == 2);
    BOOST_TEST(columns[3].maxLength() == 4);
    BOOST_CHECK(!columns[3].isNull());

    BOOST_TEST(columns[4].name().compare(L"Col_5") == 0);
    BOOST_TEST(columns[4].type() == ColumnType::Decimal);
    BOOST_TEST(columns[4].minValue() == -32186.5697);
    BOOST_TEST(columns[4].maxValue() == 71687.7855);
    BOOST_TEST(columns[4].digitsBeforeDecimalPoint() == 5);
    BOOST_TEST(columns[4].digitsAfterDecimalPoint() == 12);
    BOOST_TEST(columns[4].minLength() == 9);
    BOOST_TEST(columns[4].maxLength() == 16);
    BOOST_CHECK(!columns[4].isNull());

    BOOST_TEST(columns[5].name().compare(L"Col_6") == 0);
    BOOST_TEST(columns[5].type() == ColumnType::Bool);
    BOOST_CHECK(!columns[5].isNull());

    BOOST_TEST(columns[6].name().compare(L"Col_7") == 0);
    BOOST_TEST(columns[6].type() == ColumnType::String);
    BOOST_TEST(columns[6].minLength() == 10);
    BOOST_TEST(columns[6].maxLength() == 12);
    BOOST_CHECK(!columns[6].isNull());
}

BOOST_AUTO_TEST_CASE(parsing_results_4)
{
    CsvFileParser parser(LR"^(C:\Users\genna_000\Documents\Experiments\test data\parsing_results_4.csv)^");
    auto parsingResults = parser.parse(L',', L'"', L'\\');
    BOOST_TEST(parsingResults.numLines() == 66);
    BOOST_TEST(parsingResults.numMalformedLines() == 3);
    const auto& columns = parsingResults.columns();
    BOOST_TEST(columns.size() == 7);

    BOOST_TEST(columns[0].name().compare(L"Col_1") == 0);
    BOOST_TEST(columns[0].type() == ColumnType::String);
    BOOST_TEST(columns[0].minLength() == 1);
    BOOST_TEST(columns[0].maxLength() == 23);
    BOOST_CHECK(!columns[0].isNull());

    BOOST_TEST(columns[1].name().compare(L"Col_2") == 0);
    BOOST_TEST(columns[1].type() == ColumnType::String);
    BOOST_TEST(columns[1].minLength() == 1);
    BOOST_TEST(columns[1].maxLength() == 10);
    BOOST_CHECK(!columns[1].isNull());

    BOOST_TEST(columns[2].name().compare(L"Col_3") == 0);
    BOOST_TEST(columns[2].type() == ColumnType::String);
    BOOST_TEST(columns[2].minLength() == 1);
    BOOST_TEST(columns[2].maxLength() == 12);
    BOOST_CHECK(!columns[2].isNull());

    BOOST_TEST(columns[3].name().compare(L"Col_4") == 0);
    BOOST_TEST(columns[3].type() == ColumnType::String);
    BOOST_TEST(columns[3].minLength() == 1);
    BOOST_TEST(columns[3].maxLength() == 4);
    BOOST_CHECK(!columns[3].isNull());

    BOOST_TEST(columns[4].name().compare(L"Col_5") == 0);
    BOOST_TEST(columns[4].type() == ColumnType::String);
    BOOST_TEST(columns[4].minLength() == 1);
    BOOST_TEST(columns[4].maxLength() == 16);
    BOOST_CHECK(!columns[4].isNull());

    BOOST_TEST(columns[5].name().compare(L"Col_6") == 0);
    BOOST_TEST(columns[5].type() == ColumnType::String);
    BOOST_TEST(columns[5].minLength() == 1);
    BOOST_TEST(columns[5].maxLength() == 5);
    BOOST_CHECK(!columns[5].isNull());

    BOOST_TEST(columns[6].name().compare(L"Col_7") == 0);
    BOOST_TEST(columns[6].type() == ColumnType::String);
    BOOST_TEST(columns[6].minLength() == 1);
    BOOST_TEST(columns[6].maxLength() == 12);
    BOOST_CHECK(!columns[6].isNull());
}

BOOST_AUTO_TEST_CASE(parsing_results_5)
{
    CsvFileParser parser(LR"^(C:\Users\genna_000\Documents\Experiments\test data\parsing_results_5.csv)^");
    auto parsingResults = parser.parse(L',', L'"', L'\\');
    BOOST_TEST(parsingResults.numLines() == 66);
    BOOST_TEST(parsingResults.numMalformedLines() == 4);
    const auto& columns = parsingResults.columns();
    BOOST_TEST(columns.size() == 7);

    BOOST_TEST(columns[0].name().compare(L"Col_1") == 0);
    BOOST_TEST(columns[0].type() == ColumnType::TimeStamp);
    BOOST_TEST(columns[0].minLength() == 23);
    BOOST_TEST(columns[0].maxLength() == 23);
    BOOST_CHECK(!columns[0].isNull());

    BOOST_TEST(columns[1].name().compare(L"Col_2") == 0);
    BOOST_TEST(columns[1].type() == ColumnType::Date);
    BOOST_TEST(columns[1].minLength() == 10);
    BOOST_TEST(columns[1].maxLength() == 10);
    BOOST_CHECK(!columns[1].isNull());

    BOOST_TEST(columns[2].name().compare(L"Col_3") == 0);
    BOOST_TEST(columns[2].type() == ColumnType::Time);
    BOOST_TEST(columns[2].minLength() == 12);
    BOOST_TEST(columns[2].maxLength() == 12);
    BOOST_CHECK(!columns[2].isNull());

    BOOST_TEST(columns[3].name().compare(L"Col_4") == 0);
    BOOST_TEST(columns[3].type() == ColumnType::Int);
    BOOST_TEST(columns[3].minValue() == -500);
    BOOST_TEST(columns[3].maxValue() == 4812);
    BOOST_TEST(columns[3].digitsBeforeDecimalPoint() == 4);
    BOOST_TEST(columns[3].digitsAfterDecimalPoint() == 0);
    BOOST_TEST(columns[3].minLength() == 2);
    BOOST_TEST(columns[3].maxLength() == 4);
    BOOST_CHECK(!columns[3].isNull());

    BOOST_TEST(columns[4].name().compare(L"Col_5") == 0);
    BOOST_TEST(columns[4].type() == ColumnType::Decimal);
    BOOST_TEST(columns[4].minValue() == -32186.5697);
    BOOST_TEST(columns[4].maxValue() == 71687.7855);
    BOOST_TEST(columns[4].digitsBeforeDecimalPoint() == 5);
    BOOST_TEST(columns[4].digitsAfterDecimalPoint() == 12);
    BOOST_TEST(columns[4].minLength() == 9);
    BOOST_TEST(columns[4].maxLength() == 16);
    BOOST_CHECK(!columns[4].isNull());

    BOOST_TEST(columns[5].name().compare(L"Col_6") == 0);
    BOOST_TEST(columns[5].type() == ColumnType::Bool);
    BOOST_CHECK(!columns[5].isNull());

    BOOST_TEST(columns[6].name().compare(L"Col_7") == 0);
    BOOST_TEST(columns[6].type() == ColumnType::String);
    BOOST_TEST(columns[6].minLength() == 10);
    BOOST_TEST(columns[6].maxLength() == 12);
    BOOST_CHECK(!columns[6].isNull());
}

BOOST_AUTO_TEST_CASE(parsing_results_6)
{
    CsvFileParser parser(LR"^(C:\Users\genna_000\Documents\Experiments\test data\parsing_results_6.csv)^");
    auto parsingResults = parser.parse(L',', L'"', L'\\');
    BOOST_TEST(parsingResults.numLines() == 66);
    BOOST_TEST(parsingResults.numMalformedLines() == 4);
    const auto& columns = parsingResults.columns();
    BOOST_TEST(columns.size() == 7);

    BOOST_TEST(columns[0].name().compare(L"Col_1") == 0);
    BOOST_TEST(columns[0].type() == ColumnType::TimeStamp);
    BOOST_TEST(columns[0].minLength() == 1);
    BOOST_TEST(columns[0].maxLength() == 23);
    BOOST_CHECK(columns[0].isNull());

    BOOST_TEST(columns[1].name().compare(L"Col_2") == 0);
    BOOST_TEST(columns[1].type() == ColumnType::Date);
    BOOST_TEST(columns[1].minLength() == 10);
    BOOST_TEST(columns[1].maxLength() == 10);
    BOOST_CHECK(!columns[1].isNull());

    BOOST_TEST(columns[2].name().compare(L"Col_3") == 0);
    BOOST_TEST(columns[2].type() == ColumnType::Time);
    BOOST_TEST(columns[2].minLength() == 12);
    BOOST_TEST(columns[2].maxLength() == 12);
    BOOST_CHECK(!columns[2].isNull());

    BOOST_TEST(columns[3].name().compare(L"Col_4") == 0);
    BOOST_TEST(columns[3].type() == ColumnType::Int);
    BOOST_TEST(columns[3].minValue() == -500);
    BOOST_TEST(columns[3].maxValue() == 4812);
    BOOST_TEST(columns[3].digitsBeforeDecimalPoint() == 4);
    BOOST_TEST(columns[3].digitsAfterDecimalPoint() == 0);
    BOOST_TEST(columns[3].minLength() == 2);
    BOOST_TEST(columns[3].maxLength() == 4);
    BOOST_CHECK(!columns[3].isNull());

    BOOST_TEST(columns[4].name().compare(L"Col_5") == 0);
    BOOST_TEST(columns[4].type() == ColumnType::Decimal);
    BOOST_TEST(columns[4].minValue() == -32186.5697);
    BOOST_TEST(columns[4].maxValue() == 71687.7855);
    BOOST_TEST(columns[4].digitsBeforeDecimalPoint() == 5);
    BOOST_TEST(columns[4].digitsAfterDecimalPoint() == 12);
    BOOST_TEST(columns[4].minLength() == 9);
    BOOST_TEST(columns[4].maxLength() == 16);
    BOOST_CHECK(!columns[4].isNull());

    BOOST_TEST(columns[5].name().compare(L"Col_6") == 0);
    BOOST_TEST(columns[5].type() == ColumnType::Bool);
    BOOST_CHECK(!columns[5].isNull());

    BOOST_TEST(columns[6].name().compare(L"Col_7") == 0);
    BOOST_TEST(columns[6].type() == ColumnType::String);
    BOOST_TEST(columns[6].minLength() == 10);
    BOOST_TEST(columns[6].maxLength() == 12);
    BOOST_CHECK(!columns[6].isNull());
}

BOOST_AUTO_TEST_CASE(parsing_results_7)
{
    CsvFileParser parser(LR"^(C:\Users\genna_000\Documents\Experiments\test data\parsing_results_7.csv)^");
    auto parsingResults = parser.parse(L',', L'"', L'\\');
    BOOST_TEST(parsingResults.numLines() == 66);
    BOOST_TEST(parsingResults.numMalformedLines() == 4);
    const auto& columns = parsingResults.columns();
    BOOST_TEST(columns.size() == 7);

    BOOST_TEST(columns[0].name().compare(L"Col_1") == 0);
    BOOST_TEST(columns[0].type() == ColumnType::TimeStamp);
    BOOST_TEST(columns[0].minLength() == 1);
    BOOST_TEST(columns[0].maxLength() == 23);
    BOOST_CHECK(columns[0].isNull());

    BOOST_TEST(columns[1].name().compare(L"Col_2") == 0);
    BOOST_TEST(columns[1].type() == ColumnType::Date);
    BOOST_TEST(columns[1].minLength() == 10);
    BOOST_TEST(columns[1].maxLength() == 10);
    BOOST_CHECK(columns[1].isNull());

    BOOST_TEST(columns[2].name().compare(L"Col_3") == 0);
    BOOST_TEST(columns[2].type() == ColumnType::Time);
    BOOST_TEST(columns[2].minLength() == 12);
    BOOST_TEST(columns[2].maxLength() == 12);
    BOOST_CHECK(!columns[2].isNull());

    BOOST_TEST(columns[3].name().compare(L"Col_4") == 0);
    BOOST_TEST(columns[3].type() == ColumnType::Int);
    BOOST_TEST(columns[3].minValue() == -500);
    BOOST_TEST(columns[3].maxValue() == 4812);
    BOOST_TEST(columns[3].digitsBeforeDecimalPoint() == 4);
    BOOST_TEST(columns[3].digitsAfterDecimalPoint() == 0);
    BOOST_TEST(columns[3].minLength() == 2);
    BOOST_TEST(columns[3].maxLength() == 4);
    BOOST_CHECK(!columns[3].isNull());

    BOOST_TEST(columns[4].name().compare(L"Col_5") == 0);
    BOOST_TEST(columns[4].type() == ColumnType::Decimal);
    BOOST_TEST(columns[4].minValue() == -32186.5697);
    BOOST_TEST(columns[4].maxValue() == 71687.7855);
    BOOST_TEST(columns[4].digitsBeforeDecimalPoint() == 5);
    BOOST_TEST(columns[4].digitsAfterDecimalPoint() == 12);
    BOOST_TEST(columns[4].minLength() == 9);
    BOOST_TEST(columns[4].maxLength() == 16);
    BOOST_CHECK(!columns[4].isNull());

    BOOST_TEST(columns[5].name().compare(L"Col_6") == 0);
    BOOST_TEST(columns[5].type() == ColumnType::Bool);
    BOOST_CHECK(!columns[5].isNull());

    BOOST_TEST(columns[6].name().compare(L"Col_7") == 0);
    BOOST_TEST(columns[6].type() == ColumnType::String);
    BOOST_TEST(columns[6].minLength() == 10);
    BOOST_TEST(columns[6].maxLength() == 12);
    BOOST_CHECK(!columns[6].isNull());
}

BOOST_AUTO_TEST_CASE(parsing_results_bare_headers)
{
    CsvFileParser parser(LR"^(C:\Users\genna_000\Documents\Experiments\test data\bare_headers.csv)^");
    auto parsingResults = parser.parse(L'\t', L'"', L'\\');
    BOOST_TEST(parsingResults.numLines() == 0);
    BOOST_TEST(parsingResults.numMalformedLines() == 0);
    const auto& columns = parsingResults.columns();
    BOOST_TEST(columns.size() == 7);
    BOOST_TEST(columns[0].name().compare(L"Col_1") == 0);
    BOOST_TEST(columns[1].name().compare(L"Col_2") == 0);
    BOOST_TEST(columns[2].name().compare(L"Col_3") == 0);
    BOOST_TEST(columns[3].name().compare(L"Col_4") == 0);
    BOOST_TEST(columns[4].name().compare(L"Col_5") == 0);
    BOOST_TEST(columns[5].name().compare(L"Col_6") == 0);
    BOOST_TEST(columns[6].name().compare(L"Col_7") == 0);
}

BOOST_AUTO_TEST_CASE(parsing_results_boolean)
{
    CsvFileParser parser(LR"^(C:\Users\genna_000\Documents\Experiments\test data\boolean.csv)^");
    auto parsingResults = parser.parse(L' ', L'"', L'\\');
    BOOST_TEST(parsingResults.numLines() == 4);
    BOOST_TEST(parsingResults.numMalformedLines() == 0);
    const auto& columns = parsingResults.columns();
    BOOST_TEST(columns.size() == 7);

    BOOST_TEST(columns[0].name().compare(L"a") == 0);
    BOOST_TEST(columns[0].type() == ColumnType::Bool);
    BOOST_CHECK(!columns[0].isNull());

    BOOST_TEST(columns[1].name().compare(L"b") == 0);
    BOOST_TEST(columns[1].type() == ColumnType::Bool);
    BOOST_CHECK(!columns[1].isNull());

    BOOST_TEST(columns[2].name().compare(L"c") == 0);
    BOOST_TEST(columns[2].type() == ColumnType::String);
    BOOST_CHECK(!columns[2].isNull());

    BOOST_TEST(columns[3].name().compare(L"d") == 0);
    BOOST_TEST(columns[3].type() == ColumnType::String);
    BOOST_CHECK(!columns[3].isNull());

    BOOST_TEST(columns[4].name().compare(L"e") == 0);
    BOOST_TEST(columns[4].type() == ColumnType::String);
    BOOST_CHECK(!columns[4].isNull());

    BOOST_TEST(columns[5].name().compare(L"f") == 0);
    BOOST_TEST(columns[5].type() == ColumnType::String);
    BOOST_CHECK(!columns[5].isNull());

    BOOST_TEST(columns[6].name().compare(L"g") == 0);
    BOOST_TEST(columns[6].type() == ColumnType::String);
    BOOST_CHECK(!columns[6].isNull());
}

BOOST_AUTO_TEST_SUITE_END();

BOOST_AUTO_TEST_SUITE(generate_create_table_command);

BOOST_AUTO_TEST_CASE(generate_create_table_command_1)
{
    const std::wstring sourceFile(LR"^(C:\Users\genna_000\Documents\Experiments\test data\parsing_results_1.csv)^");
    auto& gLogger = GlobalLogger::get();
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << sourceFile << FUNCTION_FILE_LINE;
    MonetDBBulkLoader bulkLoader(sourceFile);
    bulkLoader.parse(L',', L'"');
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << bulkLoader.parsingResults().numLines() << L" lines, "
                                             << bulkLoader.parsingResults().numMalformedLines() << L" malformed lines, "
                                             << bulkLoader.parsingResults().columns().size() << L" columns" << FUNCTION_FILE_LINE;
    bulkLoader.load();
    bulkLoader.load(L"Parsing Results 1");
    bulkLoader.load(L"Разбор результатов 1");
}

BOOST_AUTO_TEST_CASE(generate_create_table_command_2)
{
    const std::wstring sourceFile(LR"^(C:\Users\genna_000\Documents\Experiments\test data\parsing_results_2.csv)^");
    auto& gLogger = GlobalLogger::get();
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << sourceFile << FUNCTION_FILE_LINE;
    MonetDBBulkLoader bulkLoader(sourceFile);
    bulkLoader.parse(L',', L'"');
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << bulkLoader.parsingResults().numLines() << L" lines, "
                                             << bulkLoader.parsingResults().numMalformedLines() << L" malformed lines, "
                                             << bulkLoader.parsingResults().columns().size() << L" columns" << FUNCTION_FILE_LINE;
    bulkLoader.load();
}

BOOST_AUTO_TEST_CASE(generate_create_table_command_3)
{
    const std::wstring sourceFile(LR"^(C:\Users\genna_000\Documents\Experiments\test data\parsing_results_3.csv)^");
    auto& gLogger = GlobalLogger::get();
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << sourceFile << FUNCTION_FILE_LINE;
    MonetDBBulkLoader bulkLoader(sourceFile);
    bulkLoader.parse(L'\t', L'"');
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << bulkLoader.parsingResults().numLines() << L" lines, "
                                             << bulkLoader.parsingResults().numMalformedLines() << L" malformed lines, "
                                             << bulkLoader.parsingResults().columns().size() << L" columns" << FUNCTION_FILE_LINE;
    bulkLoader.load();
}

BOOST_AUTO_TEST_CASE(generate_create_table_command_3_missing)
{
    const std::wstring sourceFile(LR"^(C:\Users\genna_000\Documents\Experiments\test data\parsing_results_3_missing.csv)^");
    auto& gLogger = GlobalLogger::get();
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << sourceFile << FUNCTION_FILE_LINE;
    MonetDBBulkLoader bulkLoader(sourceFile);
    bulkLoader.parse(L'\t', L'"');
    BOOST_TEST(bulkLoader.parsingResults().numMalformedLines() == 4);
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << bulkLoader.parsingResults().numLines() << L" lines, "
                                             << bulkLoader.parsingResults().numMalformedLines() << L" malformed lines, "
                                             << bulkLoader.parsingResults().columns().size() << L" columns" << FUNCTION_FILE_LINE;
    bulkLoader.load();
}

BOOST_AUTO_TEST_CASE(generate_create_table_command_5)
{
    const std::wstring sourceFile(LR"^(C:\Users\genna_000\Documents\Experiments\test data\parsing_results_5.csv)^");
    auto& gLogger = GlobalLogger::get();
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << sourceFile << FUNCTION_FILE_LINE;
    MonetDBBulkLoader bulkLoader(sourceFile);
    bulkLoader.parse(L',', L'"');
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << bulkLoader.parsingResults().numLines() << L" lines, "
                                             << bulkLoader.parsingResults().numMalformedLines() << L" malformed lines, "
                                             << bulkLoader.parsingResults().columns().size() << L" columns" << FUNCTION_FILE_LINE;
    bulkLoader.load();
}

BOOST_AUTO_TEST_CASE(generate_create_table_command_7)
{
    const std::wstring sourceFile(LR"^(C:\Users\genna_000\Documents\Experiments\test data\parsing_results_7.csv)^");
    auto& gLogger = GlobalLogger::get();
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << sourceFile << FUNCTION_FILE_LINE;
    MonetDBBulkLoader bulkLoader(sourceFile);
    bulkLoader.parse(L',', L'"');
    BOOST_LOG_SEV(gLogger, bltrivial::trace) << bulkLoader.parsingResults().numLines() << L" lines, "
                                             << bulkLoader.parsingResults().numMalformedLines() << L" malformed lines, "
                                             << bulkLoader.parsingResults().columns().size() << L" columns" << FUNCTION_FILE_LINE;
    bulkLoader.load();
}

BOOST_AUTO_TEST_SUITE_END();
