// for /l %i in (1,1,100) do echo %i & time /t & CsvFileParser.exe

#define BOOST_TEST_MODULE Test Module CsvFileParser
#include <boost/test/unit_test.hpp>

#include "CsvFileParser.h"
#include "log.h"
#include "utilities.h"
#include <boost/algorithm/string.hpp>
#include <numeric>
#include <string>
#include <thread>

using namespace std::string_literals;

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
    TestSuiteFixtureAnalyzeToken()
    {
        BOOST_TEST_MESSAGE("setup fixture TestSuiteFixtureAnalyzeToken");
        ColumnInfo::initializeLocales();
    }
    ~TestSuiteFixtureAnalyzeToken() { BOOST_TEST_MESSAGE("teardown fixture TestSuiteFixtureAnalyzeToken"); }
};

BOOST_AUTO_TEST_SUITE(ColumnInfo_analyzeToken, *boost::unit_test::fixture<TestSuiteFixtureAnalyzeToken>());

BOOST_AUTO_TEST_CASE(float_decimal_int)
{
    {
        ColumnInfo columnInfo(L"column1"s);
        BOOST_TEST(columnInfo.type() == ColumnType::String);
        BOOST_CHECK(columnInfo.IsNull());

        columnInfo.analyzeToken(L" 0 "s);
        BOOST_TEST(columnInfo.type() == ColumnType::Int);
        BOOST_CHECK(!columnInfo.IsNull());
        BOOST_TEST(columnInfo.DigitsBeforeDecimalPoint() == 1);
        BOOST_TEST(columnInfo.DigitsAfterDecimalPoint() == 0);
        BOOST_TEST(columnInfo.minLength() == 3);

        columnInfo.analyzeToken(L" "s);
        BOOST_TEST(columnInfo.type() == ColumnType::Int);
        BOOST_CHECK(columnInfo.IsNull());
        BOOST_TEST(columnInfo.minLength() == 1);

        columnInfo.analyzeToken(L""s);
        BOOST_TEST(columnInfo.type() == ColumnType::Int);
        BOOST_CHECK(columnInfo.IsNull());
        BOOST_TEST(columnInfo.minLength() == 0);

        columnInfo.analyzeToken(L" 1 "s);
        BOOST_TEST(columnInfo.type() == ColumnType::Int);
        BOOST_CHECK(columnInfo.IsNull());
        BOOST_TEST(columnInfo.DigitsBeforeDecimalPoint() == 1);
        BOOST_TEST(columnInfo.DigitsAfterDecimalPoint() == 0);

        columnInfo.analyzeToken(L" 100 "s);
        BOOST_TEST(columnInfo.type() == ColumnType::Int);
        BOOST_CHECK(columnInfo.IsNull());
        BOOST_TEST(columnInfo.DigitsBeforeDecimalPoint() == 3);
        BOOST_TEST(columnInfo.DigitsAfterDecimalPoint() == 0);

        columnInfo.analyzeToken(L" 25 "s);
        BOOST_TEST(columnInfo.type() == ColumnType::Int);
        BOOST_CHECK(columnInfo.IsNull());
        BOOST_TEST(columnInfo.DigitsBeforeDecimalPoint() == 3);
        BOOST_TEST(columnInfo.DigitsAfterDecimalPoint() == 0);

        columnInfo.analyzeToken(L" 25. "s);
        BOOST_TEST(columnInfo.type() == ColumnType::Decimal);
        BOOST_CHECK(columnInfo.IsNull());
        BOOST_TEST(columnInfo.DigitsBeforeDecimalPoint() == 3);
        BOOST_TEST(columnInfo.DigitsAfterDecimalPoint() == 0);

        columnInfo.analyzeToken(L" 25.00 "s);
        BOOST_TEST(columnInfo.type() == ColumnType::Decimal);
        BOOST_CHECK(columnInfo.IsNull());
        BOOST_TEST(columnInfo.DigitsBeforeDecimalPoint() == 3);
        BOOST_TEST(columnInfo.DigitsAfterDecimalPoint() == 2);

        columnInfo.analyzeToken(L" -150.00 "s);
        BOOST_TEST(columnInfo.type() == ColumnType::Decimal);
        BOOST_CHECK(columnInfo.IsNull());
        BOOST_TEST(columnInfo.DigitsBeforeDecimalPoint() == 3);
        BOOST_TEST(columnInfo.DigitsAfterDecimalPoint() == 2);

        columnInfo.analyzeToken(L" -1234.123 "s);
        BOOST_TEST(columnInfo.type() == ColumnType::Decimal);
        BOOST_CHECK(columnInfo.IsNull());
        BOOST_TEST(columnInfo.DigitsBeforeDecimalPoint() == 4);
        BOOST_TEST(columnInfo.DigitsAfterDecimalPoint() == 3);

        columnInfo.analyzeToken(L" -12345 "s);
        BOOST_TEST(columnInfo.type() == ColumnType::Decimal);
        BOOST_CHECK(columnInfo.IsNull());
        BOOST_TEST(columnInfo.DigitsBeforeDecimalPoint() == 5);
        BOOST_TEST(columnInfo.DigitsAfterDecimalPoint() == 3);

        columnInfo.analyzeToken(L"0.1e-1"s);
        BOOST_TEST(columnInfo.type() == ColumnType::Float);
        BOOST_CHECK(columnInfo.IsNull());

        columnInfo.analyzeToken(L"0X0p-1"s);
        BOOST_TEST(columnInfo.type() == ColumnType::String);
        BOOST_CHECK(columnInfo.IsNull());
        BOOST_TEST(columnInfo.maxLength() == 11);

        columnInfo.analyzeToken(L"123456789"s);
        BOOST_TEST(columnInfo.type() == ColumnType::String);
        BOOST_CHECK(columnInfo.IsNull());
        BOOST_TEST(columnInfo.DigitsBeforeDecimalPoint() == 5);
        BOOST_TEST(columnInfo.DigitsAfterDecimalPoint() == 3);
        BOOST_TEST(columnInfo.maxLength() == 11);
        BOOST_TEST(columnInfo.minLength() == 0);
    }
    {
        ColumnInfo columnInfo(L"column2"s);
        BOOST_TEST(columnInfo.type() == ColumnType::String);
        BOOST_CHECK(columnInfo.IsNull());

        columnInfo.analyzeToken(L" -.00001 "s);
        BOOST_TEST(columnInfo.type() == ColumnType::Decimal);
        BOOST_CHECK(!columnInfo.IsNull());
        BOOST_TEST(columnInfo.DigitsBeforeDecimalPoint() == 0);
        BOOST_TEST(columnInfo.DigitsAfterDecimalPoint() == 5);

        columnInfo.analyzeToken(L"123456789"s);
        BOOST_TEST(columnInfo.type() == ColumnType::Decimal);
        BOOST_CHECK(!columnInfo.IsNull());
        BOOST_TEST(columnInfo.DigitsBeforeDecimalPoint() == 9);
        BOOST_TEST(columnInfo.DigitsAfterDecimalPoint() == 5);
    }
}

BOOST_AUTO_TEST_CASE(time_stamp)
{
    ColumnInfo columnInfo(L"column1"s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(columnInfo.IsNull());

    columnInfo.analyzeToken(L" 2019-02-28 23:59:59.999 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::TimeStamp);
    BOOST_CHECK(!columnInfo.IsNull());

    columnInfo.analyzeToken(L" "s);
    BOOST_TEST(columnInfo.type() == ColumnType::TimeStamp);
    BOOST_CHECK(columnInfo.IsNull());

    columnInfo.analyzeToken(L" 2019-02-28 23:59:59.999 foo "s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(columnInfo.IsNull());

    columnInfo.analyzeToken(L" 2019-02-28 23:59:59.999 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(columnInfo.IsNull());
}

BOOST_AUTO_TEST_CASE(time_stamp2)
{
    ColumnInfo columnInfo(L"column1"s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(columnInfo.IsNull());

    columnInfo.analyzeToken(L" 2019-02-28 23:59:59.999 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::TimeStamp);
    BOOST_CHECK(!columnInfo.IsNull());

    columnInfo.analyzeToken(L" 2019-02-28 23:59:59 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::TimeStamp);
    BOOST_CHECK(!columnInfo.IsNull());

    columnInfo.analyzeToken(L" 2019-02-28 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(!columnInfo.IsNull());

    columnInfo.analyzeToken(L" 2019-02-28 23:59:59.999 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(!columnInfo.IsNull());
}

BOOST_AUTO_TEST_CASE(time)
{
    ColumnInfo columnInfo(L"column1"s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(columnInfo.IsNull());

    columnInfo.analyzeToken(L" 23:59:59.999 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::Time);
    BOOST_CHECK(!columnInfo.IsNull());

    columnInfo.analyzeToken(L" "s);
    BOOST_TEST(columnInfo.type() == ColumnType::Time);
    BOOST_CHECK(columnInfo.IsNull());

    columnInfo.analyzeToken(L" 23:59:59.999 foo "s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(columnInfo.IsNull());

    columnInfo.analyzeToken(L" 23:59:59.999 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(columnInfo.IsNull());
}

BOOST_AUTO_TEST_CASE(time_2)
{
    ColumnInfo columnInfo(L"column1"s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(columnInfo.IsNull());

    columnInfo.analyzeToken(L" 23:59:59.999 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::Time);
    BOOST_CHECK(!columnInfo.IsNull());

    columnInfo.analyzeToken(L" 23:59:59 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::Time);
    BOOST_CHECK(!columnInfo.IsNull());

    columnInfo.analyzeToken(L" 2019-02-28 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(!columnInfo.IsNull());

    columnInfo.analyzeToken(L" 23:59:59.999 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(!columnInfo.IsNull());
}

BOOST_AUTO_TEST_CASE(date)
{
    ColumnInfo columnInfo(L"column1"s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(columnInfo.IsNull());

    columnInfo.analyzeToken(L" 2019-02-28 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::Date);
    BOOST_CHECK(!columnInfo.IsNull());

    columnInfo.analyzeToken(L" "s);
    BOOST_TEST(columnInfo.type() == ColumnType::Date);
    BOOST_CHECK(columnInfo.IsNull());

    columnInfo.analyzeToken(L" 2019-02-28 23:59:59.999 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(columnInfo.IsNull());

    columnInfo.analyzeToken(L" 2019-02-28 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(columnInfo.IsNull());
}

BOOST_AUTO_TEST_CASE(date2)
{
    ColumnInfo columnInfo(L"column1"s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(columnInfo.IsNull());

    columnInfo.analyzeToken(L" 2019-02-28 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::Date);
    BOOST_CHECK(!columnInfo.IsNull());

    columnInfo.analyzeToken(L" 23:59:59 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(!columnInfo.IsNull());

    columnInfo.analyzeToken(L" 2019-02-28 "s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(!columnInfo.IsNull());
}

BOOST_AUTO_TEST_CASE(bool_)
{
    ColumnInfo columnInfo(L"column1"s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(columnInfo.IsNull());

    columnInfo.analyzeToken(L" true "s);
    BOOST_TEST(columnInfo.type() == ColumnType::Bool);
    BOOST_CHECK(!columnInfo.IsNull());

    columnInfo.analyzeToken(L" false "s);
    BOOST_TEST(columnInfo.type() == ColumnType::Bool);
    BOOST_CHECK(!columnInfo.IsNull());

    columnInfo.analyzeToken(L" yes "s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(!columnInfo.IsNull());

    columnInfo.analyzeToken(L" true "s);
    BOOST_TEST(columnInfo.type() == ColumnType::String);
    BOOST_CHECK(!columnInfo.IsNull());
}

BOOST_AUTO_TEST_SUITE_END();
