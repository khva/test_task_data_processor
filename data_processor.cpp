#include "data_processor.hpp"
#include <iostream>
#include <optional>
#include <random>


namespace dpr
{

namespace detail
{
    constexpr const std::size_t chunk_size = 256;
    constexpr const std::uint8_t mask = 0xF;

    inline std::size_t calculate_chunk_count(std::size_t in_buffer_size)
    {
        return in_buffer_size / chunk_size + (in_buffer_size % chunk_size > 0 ? 1 : 0);
    }

    inline std::size_t calculate_worker_count(std::size_t in_buffer_size, std::size_t desired_worker_count)
    {
        const std::size_t chunk_count = calculate_chunk_count(in_buffer_size);
        return chunk_count < desired_worker_count ? chunk_count : desired_worker_count;
    }

    inline std::size_t calculate_chunk_size(std::size_t buffer_size, std::size_t start_pos)
    {
        return buffer_size > start_pos + chunk_size
            ? chunk_size : buffer_size < start_pos ? 0 : buffer_size - start_pos;
    }
}

data_processor_t::data_processor_t(const data_t& in_buffer, data_t& out_buffer, std::size_t worker_count)
    : m_in_buffer(in_buffer)
    , m_out_buffer(out_buffer)
    , m_callbacks()
    , m_workers()
    , m_stopping_workers()
    , m_fill_pos(0)
    , m_checked_count(0)
{
    prepare(worker_count);
}

void data_processor_t::run()
{
    for (auto& worker : m_workers)
        worker->activate();

    const std::size_t chunk_count = detail::calculate_chunk_count(m_in_buffer.size());

    while (m_checked_count != chunk_count)
    {
        if (execute_callbacks() == 0)
        {
            if (m_checked_count != chunk_count)
                std::this_thread::yield();
        }
    }

    for (auto& worker : m_workers)
        worker->stop();
}

void data_processor_t::prepare(std::size_t desired_worker_count)
{
    if (!m_callbacks.empty() || !m_workers.empty())
        throw processing_exception("Data processing has already started.");

    const std::size_t in_buffer_size = m_in_buffer.size();
    if (in_buffer_size == 0)
    {
        std::string msg(256, '\0');
        std::snprintf(
            msg.data(),msg.size() - 1,
            "The invalid input buffer size: %zu (must be greater 0).", in_buffer_size);
        throw processing_exception(msg.c_str());
    }

    const std::size_t out_buffer_size = m_in_buffer.size();
    if (out_buffer_size != in_buffer_size)
    {
        if (in_buffer_size == 0)
        {
            std::string msg(256, '\0');
            std::snprintf(
                msg.data(), msg.size() - 1,
                "The invalid output buffer size: %zu (expected %zu).", out_buffer_size, in_buffer_size);
            throw processing_exception(msg.c_str());
        }
    }

    const std::size_t worker_count = detail::calculate_worker_count(in_buffer_size, desired_worker_count);

    m_callbacks.reserve(worker_count);
    for (std::size_t i = 0; i < worker_count; ++i)
        m_callbacks.push_back(std::make_unique<queue_t>(queue_size));

    m_workers.reserve(worker_count);
    for (std::size_t i = 0; i < worker_count; ++i)
        m_workers.push_back(std::make_unique<worker_t>(i, queue_size));

    for (std::uint32_t n = 0; n < queue_size && m_fill_pos < in_buffer_size; ++n)
    {
        for (std::size_t i = 0; i < worker_count; ++i)
            make_fill_task(i, true);
    }
}

std::size_t data_processor_t::execute_callbacks()
{
    std::size_t calls_count = 0;

    for (auto& queue : m_callbacks)
    {
        if (queue->size() > 0)
        {
            std::optional<task_t> task = queue->pop();
            assert(task.has_value());

            (*task)();
            ++calls_count;
        }
    }

    return calls_count;
}


void data_processor_t::fill_input_chunk(std::size_t index, std::size_t start_pos)
{
    const std::size_t cur_chunk_size = detail::calculate_chunk_size(m_in_buffer.size(), start_pos);

    auto in_it = m_in_buffer.cbegin() + start_pos;
    auto out_it = m_out_buffer.begin() + start_pos;

    for (std::size_t pos = start_pos, end_pos = start_pos + cur_chunk_size; pos < end_pos; ++pos, ++in_it, ++out_it)
        *out_it = *in_it ^ detail::mask;

    [[maybe_unused]] const bool flag = m_callbacks[index]->
        push([index, start_pos, this]() { make_check_task(index, start_pos); });
    assert(flag);
}

void data_processor_t::check_input_chunk(std::size_t index, std::size_t start_pos)
{
    const std::size_t cur_chunk_size = detail::calculate_chunk_size(m_in_buffer.size(), start_pos);

    auto in_it = m_in_buffer.cbegin() + start_pos;
    auto out_it = m_out_buffer.begin() + start_pos;

    bool is_correct = true;

    for (std::size_t pos = start_pos, end_pos = start_pos + cur_chunk_size; pos < end_pos; ++pos, ++in_it, ++out_it)
    {
        if ((*out_it ^ detail::mask) != *in_it)
        {
            is_correct = false;
            break;
        }
    }

    [[maybe_unused]] const bool flag = m_callbacks[index]->
        push([index, start_pos, is_correct, this]() { show_check_result(index, start_pos, is_correct); });
    assert(flag);
}

void data_processor_t::make_fill_task(std::size_t index, bool is_initial_tasks)
{
    const std::size_t buffer_size = m_in_buffer.size();

    if (m_fill_pos < buffer_size)
    {
        auto& worker = m_workers[index];

        [[maybe_unused]] const bool flag = worker->
            add_task([index, start_pos = m_fill_pos, this]() { fill_input_chunk(index, start_pos); });
        assert(flag);

        m_fill_pos += detail::calculate_chunk_size(buffer_size, m_fill_pos);
    }
}

void data_processor_t::make_check_task(std::size_t index, std::size_t start_pos)
{
    auto& worker = m_workers[index];

    [[maybe_unused]] const bool flag = worker->
        add_task([index, start_pos, this]() { check_input_chunk(index, start_pos); });
    assert(flag);
}

void data_processor_t::show_check_result(std::size_t index, std::size_t start_pos, bool is_correct)
{
    const std::string_view result = is_correct ? "OK" : "FAIL";
    std::cout << result << ". address " << start_pos << std::endl;

    ++m_checked_count;
    make_fill_task(index, false);
}


data_t generate_data(std::size_t data_size, std::optional<unsigned int> seed)
{
    data_t buffer(data_size);
    std::random_device rd;

    std::mt19937 gen(seed ? *seed : rd());
    std::uniform_int_distribution<> distrib(
        std::numeric_limits<uint8_t>::min(), std::numeric_limits<uint8_t>::max());

    for (auto& byte : buffer)
        byte = static_cast<std::uint8_t>(distrib(gen));

    return buffer;
}

data_t process_data(const data_t& in_buffer, std::size_t worker_count)
{
    data_t out_buffer(in_buffer.size());
    data_processor_t processor(in_buffer, out_buffer, worker_count);

    processor.run();
    return out_buffer;
}


bool check_data(const data_t& in_buffer, const data_t& out_buffer)
{
    const std::size_t buf_size = in_buffer.size();

    if (buf_size != out_buffer.size())
        return false;

    auto in_it = in_buffer.cbegin();
    auto out_it = out_buffer.cbegin();

    for (std::size_t pos = 0; pos < buf_size; ++pos, ++in_it, ++out_it)
    {
        if ((*out_it ^ detail::mask) != *in_it)
        {
            std::cout << "FAIL. address " << pos << std::endl;
            return false;
        }

        if (pos % detail::chunk_size == 0)
            std::cout << "OK. address " << pos << std::endl;
    }

    return true;
}

}   // namespace dpr
