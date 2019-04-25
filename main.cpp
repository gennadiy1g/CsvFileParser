#include "CsvFileParser.h"
#include "log.h"
#include <boost/locale.hpp>
#include <numeric>
#include <string>
#include <thread>

using namespace std::string_literals;

namespace logging = boost::log;
namespace keyword = boost::log::keywords;
namespace express = boost::log::expressions;

void initLocalization()
{
    // Get global backend, and select winapi backend as default for all categories
    boost::locale::localization_backend_manager::global().select("winapi");

    /* Create and install global locale. Non UTF-8 encodings are not supported by winapi backend.
     * See https://www.boost.org/doc/libs/1_69_0/libs/locale/doc/html/using_localization_backends.html
     *
     * GCC supports localization only under Linux. On all other platforms, attempting to create locales
     * other than "C" or "POSIX" would fail.
     * See https://www.boost.org/doc/libs/1_69_0/libs/locale/doc/html/std_locales.html */
    std::locale::global(boost::locale::generator().generate("en_US.UTF-8"));

    // This is needed to prevent C library to
    // convert strings to narrow
    // instead of C++ on some platforms
    std::ios_base::sync_with_stdio(false);
}

void initLogging()
{
    logging::add_file_log(
        keyword::file_name = "trace.log",
        keyword::format = (express::stream
            << express::attr<unsigned int>("LineID") << ' ' << trivia::severity << ' '
            << express::format_date_time<boost::posix_time::ptime>("TimeStamp", " %Y-%m-%d %H:%M:%S.%f ")
            << express::attr<logging::thread_id>("ThreadID") << ' ' << express::message));
    logging::add_common_attributes();
#ifdef NDEBUG
    logging::core::get()->set_filter(trivia::severity >= trivia::info);
#endif
}

int main(int argc, char** argv)
{
    try {
        initLocalization();

        initLogging();

        auto& gLogger = GlobalLogger::get();
        BOOST_LOG_SEV(gLogger, trivia::trace) << "->" << FUNCTION_FILE_LINE;
        BOOST_LOG_SEV(gLogger, trivia::trace) << L"Привіт Світ! " << FUNCTION_FILE_LINE;

        auto backends = boost::locale::localization_backend_manager::global().get_all_backends();
        std::string backendsList = std::accumulate(backends.cbegin(), backends.cend(), ""s,
            [](const std::string& a, const std::string& b) { return a + (a == "" ? "" : ", ") + b; });
        BOOST_LOG_SEV(gLogger, trivia::debug) << "Localization backends: " << backendsList << '.';

        BOOST_LOG_SEV(gLogger, trivia::info) << std::thread::hardware_concurrency() << " concurrent threads are supported.";

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

        BOOST_LOG_SEV(gLogger, trivia::trace) << "<-" << FUNCTION_FILE_LINE;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
        return 1;
    }
}
