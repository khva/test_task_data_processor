#pragma once

#include "ring_buffer.hpp"
#include "worker_thread.hpp"
#include <cstdint>
#include <memory>
#include <set>
#include <stdexcept>
#include <vector>


namespace dpr
{

using data_t = std::vector<std::uint8_t>;


class processing_exception : public std::exception
{
public:
    explicit processing_exception(std::string msg) : m_msg(std::move(msg)) {}
    ~processing_exception() = default;

    processing_exception(const processing_exception&) = default;
    processing_exception(processing_exception&&) = default;

    processing_exception& operator=(const processing_exception&) = default;
    processing_exception& operator=(processing_exception&&) = default;

    const char* what() const noexcept override { return m_msg.c_str(); }

private:
    std::string m_msg;
};


// Класс реализует алгоритм ассинхронного вычисления описанного в `test_task_description.txt`.
class data_processor_t
{
public:
    data_processor_t(const data_t& in_buffer, data_t& out_buffer, std::size_t worker_count);
    ~data_processor_t() = default;

    data_processor_t(const data_processor_t&) = delete;
    data_processor_t(data_processor_t&&) = delete;

    data_processor_t& operator=(const data_processor_t&) = delete;
    data_processor_t& operator=(data_processor_t&&) = delete;

    void run();

private:
    using task_t = std::function<void ()>;

    using queue_t = util::ring_buffer_t<task_t>;
    using queue_ptr_t = std::unique_ptr<queue_t>;

    using worker_t = util::worker_thread_t<task_t>;
    using worker_ptr_t = std::unique_ptr<worker_t>;

    constexpr static const std::uint32_t queue_size = 10;

    void prepare(std::size_t desired_worker_count);
    std::size_t execute_callbacks();

    // задачи выполняемые в рабочих потоках
    void fill_input_chunk(std::size_t index, std::size_t start_pos);
    void check_input_chunk(std::size_t index, std::size_t start_pos);

    // задачи выполняемые в раздающем потоке
    void make_fill_task(std::size_t index, bool is_initial_tasks);
    void make_check_task(std::size_t index, std::size_t start_pos);
    void show_check_result(std::size_t index, std::size_t start_pos, bool is_correct);

    const data_t& m_in_buffer;
    data_t& m_out_buffer;

    std::vector<queue_ptr_t> m_callbacks;
    std::vector<worker_ptr_t> m_workers;

    std::set<std::size_t> m_stopping_workers;
    std::size_t m_fill_pos;
    std::size_t m_checked_count;
};


data_t generate_data(std::size_t data_size, std::optional<unsigned int> seed = std::nullopt);
data_t process_data(const data_t& in_buffer, std::size_t worker_count);

bool check_data(const data_t& in_buffer, const data_t& out_buffer);

}   // namespace dpr
