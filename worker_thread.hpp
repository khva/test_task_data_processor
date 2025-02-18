#pragma once

#include "ring_buffer.hpp"
#include <atomic>
#include <cassert>
#include <chrono>
#include <functional>
#include <iostream>
#include <thread>


namespace util
{

// Класс реализует рабочий поток, который выбирает задания из очереди заданий.
// Задания должны иметь сигнатуру вида `void ()`.
template <class T>
class worker_thread_t
{
public:
    using task_t = T;

    worker_thread_t(std::size_t id, std::uint32_t max_task_count);
    ~worker_thread_t() = default;

    worker_thread_t(const worker_thread_t&) = delete;
    worker_thread_t(worker_thread_t&&) = delete;

    worker_thread_t& operator=(const worker_thread_t&) = delete;
    worker_thread_t& operator=(worker_thread_t&&) = delete;

    std::uint32_t pending_tasks_count() const noexcept;
    std::uint32_t tasks_buffer_capacity() const noexcept;

    bool add_task(task_t&& task);
    void activate() noexcept;

    void stop() noexcept;
    bool is_stopped() const noexcept;

private:
    enum class state_t { waiting, running, stopping, stopped };
    void run() noexcept;
    void execute_tasks() noexcept;

    util::ring_buffer_t<task_t> m_tasks;
    std::jthread m_thread;
    std::size_t m_id;
    std::atomic<state_t> m_state;
};

template <class T>
inline worker_thread_t<T>::worker_thread_t(std::size_t id, std::uint32_t max_task_count)
    : m_tasks(max_task_count)
    , m_thread()
    , m_id(id)
    , m_state(state_t::waiting)
{
    m_thread = std::jthread([this]() { run(); });
}

template <class T>
inline std::uint32_t worker_thread_t<T>::pending_tasks_count() const noexcept
{
    return m_tasks.size();
}

template <class T>
inline std::uint32_t worker_thread_t<T>::tasks_buffer_capacity() const noexcept
{
    return m_tasks.capacity();
}

template <class T>
inline bool worker_thread_t<T>::add_task(task_t&& task)
{
    return m_tasks.push(std::move(task));
}

template <class T>
inline void worker_thread_t<T>::activate() noexcept
{
    state_t expected = state_t::waiting;
    const bool flag = m_state.compare_exchange_weak(
        expected, state_t::running, std::memory_order_release, std::memory_order_relaxed);

    if (flag)
        m_state.notify_one();
}

template <class T>
inline void worker_thread_t<T>::stop() noexcept
{
    bool flag = false;
    state_t expected = state_t::running;
    do
    {
        flag = m_state.compare_exchange_weak(
            expected, state_t::stopping, std::memory_order_release, std::memory_order_relaxed);
    }
    while (!(flag || expected == state_t::stopping || expected == state_t::stopped));

    m_state.notify_one();
}

template <class T>
inline bool worker_thread_t<T>::is_stopped() const noexcept
{
    return m_state == state_t::stopped;
}

template <class T>
inline void worker_thread_t<T>::run() noexcept
{
    try
    {
        m_state.wait(state_t::waiting);

        while (m_state != state_t::stopping)
            execute_tasks();
    }
    catch (const std::exception& err)
    {
        std::cerr
            << "An error has occurred in the "<< m_id << " worker thread. Error: `"
            << err.what()  << "`." << std::endl;
    }
    catch (...)
    {
        std::cerr
            << "An error has occurred in the " << m_id << " worker thread. Error: `unknown`."
            << std::endl;
    }
    m_state = state_t::stopped;
}

template <class T>
inline void worker_thread_t<T>::execute_tasks() noexcept
{
    if (m_tasks.size() == 0)
    {
        std::this_thread::yield();
        return;
    }

    do
    {
        try
        {
            std::optional<task_t> task = m_tasks.pop();
            assert(task.has_value());
            (*task)();
        }
        catch (const std::exception& err)
        {
            std::cerr
                << "A task has failed in the " << m_id << " worker thread. Error: `"
                << err.what() << "`." << std::endl;
        }
        catch (...)
        {
            std::cerr
                << "A task has failed in the " << m_id << " worker thread. Error: `unknown`."
                << std::endl;
        }
    }
    while (m_tasks.size() > 0);
}

}   // namespace util
