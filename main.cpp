#include "CsvFileParser.h"
#include "app.h"
#include <boost/locale/generator.hpp>

void initLogging()
{
    auto sink = logg::add_file_log(
        keyw::file_name = "trace.log",
        keyw::format = (expr::stream
            << expr::attr<unsigned int>("LineID") << ' ' << triv::severity << ' '
            << expr::format_date_time<boost::posix_time::ptime>("TimeStamp", " %Y-%m-%d %H:%M:%S.%f ")
            << expr::attr<logg::thread_id>("ThreadID") << ' ' << expr::message));
    sink->imbue(boost::locale::generator()("en_US.UTF-8"));
    logg::add_common_attributes();
#ifdef NDEBUG
    logg::core::get()->set_filter(triv::severity >= triv::info);
#endif
}

int main(int argc, char** argv)
{
    // Enable option to cache all generated locales
    boost::locale::generator gen;
    gen.locale_cache_enabled(true);

    initLogging();

    auto& gLogger = GlobalLogger::get();
    BOOST_LOG_SEV(gLogger, triv::trace) << "->" << FUNCTION_FILE_LINE;
    BOOST_LOG_SEV(gLogger, triv::trace) << L"Привіт Світ! " << FUNCTION_FILE_LINE;

    BOOST_LOG_SEV(gLogger, triv::info) << std::thread::hardware_concurrency() << " concurrent threads are supported.";

    CsvFileParser parser(R"^(C:\Users\genna_000\Documents\Experiments\test data\russian.csv)^");
    parser.parse(L',', L'"', L'\\');

    BOOST_LOG_SEV(gLogger, triv::trace) << "<-" << FUNCTION_FILE_LINE;
    return 0;
}
