/*
#include "CsvFileParser.h"
#include "log.h"
#include "utilities.h"
#include <numeric>
#include <string>
#include <thread>

using namespace std::string_literals;

int main(int argc, char** argv)
{
    try {
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

        {
            CsvFileParser parser(LR"^(C:\Users\genna_000\Documents\Experiments\test data\ZX0training_UTF-8.csv)^");
            parser.parse(L',', L'"', L'\\');
        }
        {
            CsvFileParser parser(LR"^(C:\Users\genna_000\Documents\Experiments\test data\russian_UTF-8.csv)^");
            parser.parse(L',', L'"', L'\\');
        }
        {
            // Processing of this file causes an exception to be thrown
            CsvFileParser parser(LR"^(C:\Users\genna_000\Documents\Experiments\test data\ZX0training_CP-863.csv)^");
            parser.parse(L',', L'"', L'\\');
        }

        BOOST_LOG_SEV(gLogger, bltrivial::trace) << "<-" << FUNCTION_FILE_LINE;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
        return 1;
    }
}
*/

#define BOOST_TEST_MODULE fixture_04
#include <boost/test/included/unit_test.hpp>

#include "CsvFileParser.h"
#include "log.h"
#include "utilities.h"
#include <numeric>
#include <string>
#include <thread>

using namespace std::string_literals;

struct MyGlobalFixture {
    MyGlobalFixture() {}

    ~MyGlobalFixture() {}

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

    void teardown()
    {
    }
};

BOOST_TEST_GLOBAL_FIXTURE(MyGlobalFixture);

BOOST_AUTO_TEST_CASE(test_case1)
{
}

BOOST_AUTO_TEST_CASE(test_case2)
{
}
