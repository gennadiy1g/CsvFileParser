#include "CsvFileParser.h"
#include "app.h"
#include <boost/locale.hpp>
#include <numeric>
#include <string>

using namespace std::string_literals;

namespace logg = boost::log;
namespace keyw = boost::log::keywords;
namespace expr = boost::log::expressions;
namespace sink = boost::log::sinks;

void initLocalization()
{
    // Get global backend, and select winapi backend as default for all categories
    boost::locale::localization_backend_manager::global().select("winapi");

    // Enable option to cache all generated locales
    boost::locale::generator gen;
    gen.locale_cache_enabled(true);

    /* Create and install global locale. Non UTF-8 encodings are not supported by winapi backend.
     * See https://www.boost.org/doc/libs/1_69_0/libs/locale/doc/html/using_localization_backends.html
     *
     * GCC supports localization only under Linux. On all other platforms, attempting to create locales
     * other than "C" or "POSIX" would fail.
     * See https://www.boost.org/doc/libs/1_69_0/libs/locale/doc/html/std_locales.html */
    std::locale::global(gen.generate("en_US.UTF-8"));

    // This is needed to prevent C library to
    // convert strings to narrow
    // instead of C++ on some platforms
    std::ios_base::sync_with_stdio(false);
}

void initLogging()
{
    auto sink = logg::add_file_log(
        keyw::file_name = "trace.log",
        keyw::format = (expr::stream
            << expr::attr<unsigned int>("LineID") << ' ' << triv::severity << ' '
            << expr::format_date_time<boost::posix_time::ptime>("TimeStamp", " %Y-%m-%d %H:%M:%S.%f ")
            << expr::attr<logg::thread_id>("ThreadID") << ' ' << expr::message));
    logg::add_common_attributes();
#ifdef NDEBUG
    logg::core::get()->set_filter(triv::severity >= triv::info);
#endif
}

int main(int argc, char** argv)
{
    try {
        initLocalization();

        initLogging();

        auto& gLogger = GlobalLogger::get();
        BOOST_LOG_SEV(gLogger, triv::trace) << "->" << FUNCTION_FILE_LINE;
        BOOST_LOG_SEV(gLogger, triv::trace) << L"Привіт Світ! " << FUNCTION_FILE_LINE;

        auto backends = boost::locale::localization_backend_manager::global().get_all_backends();
        std::string backendsList = std::accumulate(backends.cbegin(), backends.cend(), ""s,
            [](const std::string& a, const std::string& b) { return a + (a == "" ? "" : ", ") + b; });
        BOOST_LOG_SEV(gLogger, triv::debug) << "Localization backends: " << backendsList << '.';

        BOOST_LOG_SEV(gLogger, triv::info) << std::thread::hardware_concurrency() << " concurrent threads are supported.";

        {
            CsvFileParser parser(R"^(C:\Users\genna_000\Documents\Experiments\test data\ZX0training_UTF-8.csv)^");
            parser.parse(L',', L'"', L'\\');
        }
        {
            CsvFileParser parser(R"^(C:\Users\genna_000\Documents\Experiments\test data\russian_UTF-8.csv)^");
            parser.parse(L',', L'"', L'\\');
        }
        {
            // Processing of this file causes an exception to be thrown
            CsvFileParser parser(R"^(C:\Users\genna_000\Documents\Experiments\test data\ZX0training_CP-863.csv)^");
            parser.parse(L',', L'"', L'\\');
        }

        BOOST_LOG_SEV(gLogger, triv::trace) << "<-" << FUNCTION_FILE_LINE;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
        return 1;
    }
}
