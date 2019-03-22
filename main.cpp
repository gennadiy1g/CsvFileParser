#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/sinks/text_file_backend.hpp>
#include <boost/log/sources/global_logger_storage.hpp>
#include <boost/log/sources/logger.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/utility/setup/file.hpp>

#include <iostream>
#include <thread>

namespace logging = boost::log;
namespace src = boost::log::sources;
namespace keywords = boost::log::keywords;
namespace trivial = boost::log::trivial;

BOOST_LOG_INLINE_GLOBAL_LOGGER_DEFAULT(gLogger, src::severity_logger_mt<trivial::severity_level>)

void initLogging()
{
    logging::add_file_log(
        keywords::file_name = "trace.log",
        keywords::format = "[%TimeStamp%]: %Message%");
    logging::add_common_attributes();
}

int main(int argc, char** argv)
{
    initLogging();
    src::severity_logger_mt<trivial::severity_level>& slg = gLogger::get();
    BOOST_LOG_SEV(slg, trivial::trace) << "Greetings from the global logger!";

    unsigned int n = std::thread::hardware_concurrency();
    std::cout << n << " concurrent threads are supported.\n";
    return 0;
}
