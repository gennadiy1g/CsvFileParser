#include "CsvFileParser.h"
#include "log.h"
#include <boost/locale.hpp>
#include <numeric>
#include <string>
#include <thread>

using namespace std::string_literals;

namespace blog = boost::log;
namespace blkeywords = boost::log::keywords;
namespace blexpressions = boost::log::expressions;

void initLocalization()
{
    // Get global backend, and select winapi backend as default for all categories
    blocale::localization_backend_manager::global().select("winapi");

    /* Create and install global locale. Non UTF-8 encodings are not supported by winapi backend.
     * See https://www.boost.org/doc/libs/1_69_0/libs/locale/doc/html/using_localization_backends.html
     *
     * GCC supports localization only under Linux. On all other platforms, attempting to create locales
     * other than "C" or "POSIX" would fail.
     * See https://www.boost.org/doc/libs/1_69_0/libs/locale/doc/html/std_locales.html */
    std::locale::global(blocale::generator().generate("en_US.UTF-8"));

    // This is needed to prevent C library to
    // convert strings to narrow
    // instead of C++ on some platforms
    std::ios_base::sync_with_stdio(false);
}

void initLogging()
{
    blog::add_file_log(
        blkeywords::file_name = "trace.log",
        blkeywords::format = (blexpressions::stream
            << blexpressions::attr<unsigned int>("LineID") << ' ' << bltrivial::severity << ' '
            << blexpressions::format_date_time<boost::posix_time::ptime>("TimeStamp", " %Y-%m-%d %H:%M:%S.%f ")
            << blexpressions::attr<blog::thread_id>("ThreadID") << ' ' << blexpressions::message));
    blog::add_common_attributes();
#ifdef NDEBUG
    blog::core::get()->set_filter(bltrivial::severity >= bltrivial::info);
#endif
}

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
