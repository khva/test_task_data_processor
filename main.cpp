#include "data_processor.hpp"
#include <charconv>
#include <chrono>
#include <fstream>
#include <iostream>
#include <optional>
#include <string_view>
#include <thread>
#include <tuple>


enum class run_mode_t
{
    show_help = 0,
    normal,
    check
};

struct run_params_t
{
    std::size_t buffer_size = 0;
    std::size_t worker_count = 0;
    run_mode_t mode = run_mode_t::show_help;
};


inline std::size_t default_worker_count()
{
    constexpr const std::size_t def_worker_count = 4;
    const auto core_count = std::thread::hardware_concurrency();
    return core_count > 0 ? static_cast<std::size_t>(core_count) : def_worker_count;
}

inline std::optional<std::size_t> parse_integer(std::string_view str)
{
    std::size_t integer = 0;
    const char* begin = str.data();
    const char* end = begin + str.size();

    const auto res = std::from_chars(begin, end, integer);
    return res.ec == std::errc{} || res.ptr != end ? std::optional<std::size_t>{ integer } : std::nullopt;
}

std::optional<run_mode_t> check_run_mode(std::string_view str)
{
    if (str == "normal")
        return run_mode_t::normal;

    if (str == "check")
        return run_mode_t::check;

    return std::nullopt;
}

run_params_t parse_parameters(int argc, char* argv[], std::size_t def_worker_count)
{
    auto make_params = [def_worker_count](
        std::size_t buffer_size,
        std::optional<std::size_t> worker_count,
        std::optional<run_mode_t> run_mode)
        -> run_params_t
    {
        return run_params_t
            {
                buffer_size,
                worker_count ? *worker_count : def_worker_count,
                run_mode ? *run_mode : run_mode_t::normal
            };
    };

    if (argc < 2)
        return make_params(0, 0, run_mode_t::show_help);

    const std::optional<std::size_t> buffer_size = parse_integer(argv[1]);
    if (!buffer_size.has_value() || *buffer_size == 0)
    {
        std::cout << "Error: the buffer size is invalid - `" << argv[1] << "`." << std::endl;
        return make_params(0, 0, run_mode_t::show_help);
    }

    if (argc < 3)
        return make_params(*buffer_size, std::nullopt, std::nullopt);

    std::optional<run_mode_t> run_mode = check_run_mode(argv[2]);
    if (run_mode.has_value())
    {
        if (argc > 3)
            std::cout << "Warning: parameters will be ignored after `" << argv[2] << "`." << std::endl;
        return make_params(*buffer_size, std::nullopt, *run_mode);
    }

    std::optional<std::size_t> worker_count = parse_integer(argv[2]);
    if (!worker_count.has_value() || *worker_count == 0)
    {
        std::cout << "Error: the number of worker threads is invalid - `" << argv[2] << "`." << std::endl;
        return make_params(0, 0, run_mode_t::show_help);
    }

    if (argc < 4)
        return make_params(*buffer_size, worker_count, run_mode);

    run_mode = check_run_mode(argv[3]);
    if (!run_mode.has_value())
    {
        std::cout << "Error: the run mode is invalid - `" << argv[3] << "`." << std::endl;
        return make_params(0, 0, run_mode_t::show_help);
    }

    if (argc > 4)
        std::cout << "Warning: parameters will be ignored after `" << argv[3] << "`." << std::endl;

    return make_params(*buffer_size, worker_count, run_mode);
}

void show_help(std::string_view name, std::size_t def_worker_count)
{
    std::cout << "\nUsage: " << name << " <buffer_size> [worker_count] [run_mode]" << std::endl;
    std::cout << "where:" << std::endl;
    std::cout << "    buffer_size   -  buffer size in bytes (greater than 0, required)" << std::endl;
    std::cout << "    worker_count  -  number of worker threads (greater than 0, default: " << def_worker_count << ')' << std::endl;
    std::cout << "    run_mode      -  program run mode, one of: normal, check (default: normal)" << std::endl;
    std::cout << "\nexample:" << std::endl;
    std::cout << "    " << name << " 10485760 8" << std::endl;
}


int main(int argc, char* argv[])
{
    try
    {
        const std::size_t def_worker_count = default_worker_count();
        run_params_t params = parse_parameters(argc, argv, def_worker_count);

        if (params.mode == run_mode_t::show_help)
        {
            show_help(argv[0], def_worker_count);
            return 1;
        }

        if (params.mode == run_mode_t::check)
            std::cout << "Generate input buffer" << std::endl;

        const auto gen_start = std::chrono::steady_clock::now();
        const dpr::data_t& in_buffer = dpr::generate_data(params.buffer_size);
        const std::chrono::duration<double> gen_diff = std::chrono::steady_clock::now() - gen_start;

        if (params.mode == run_mode_t::check)
            std::cout << "Generate - OK. Сompleted in " << gen_diff << "." << std::endl;

        std::cout << "Buffer size " << params.buffer_size << " bytes." << std::endl;
        std::cout << "Starting " << params.worker_count << " threads" << std::endl;

        const auto proc_start = std::chrono::steady_clock::now();
        const dpr::data_t& out_buffer = dpr::process_data(in_buffer, params.worker_count);
        const std::chrono::duration<double> proc_diff = std::chrono::steady_clock::now() - proc_start;

        if (params.mode == run_mode_t::check)
            std::cout << "Processing - OK. Сompleted in " << proc_diff << "." << std::endl;

        if (params.mode == run_mode_t::check)
        {
            std::cout << "Check data output buffer: " << std::endl;
            const auto check_start = std::chrono::steady_clock::now();
            const bool is_ok = dpr::check_data(in_buffer, out_buffer);
            const std::chrono::duration<double> check_diff = std::chrono::steady_clock::now() - check_start;
            std::cout << "Check - " << (is_ok ? "Ok" : "FAIL") << ". Сompleted in " << check_diff << "." << std::endl;
        }
        std::cout << "Done" << std::endl;
    }
    catch (const std::exception& err)
    {
        std::cerr
            << "An error has occurred during data processing. Error: `"
            << err.what() << "`." << std::endl;
    }
    catch (...)
    {
        std::cerr << "An error has occurred during data processing. Error: `unknown`." << std::endl;
    }

    return 0;
}
