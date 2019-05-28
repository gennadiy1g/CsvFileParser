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
