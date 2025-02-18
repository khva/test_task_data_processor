#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include <optional>
#include <tuple>
#include <vector>


namespace util
{

// Класс реализует кольцевой буфер для передачи данных от одного потока другому: 1 писатель и 1 читатель.
template<class T>
class ring_buffer_t
{
public:
    explicit ring_buffer_t(std::uint32_t capacity);
    ~ring_buffer_t() = default;

    ring_buffer_t(const ring_buffer_t&) = delete;
    ring_buffer_t(ring_buffer_t&&) = delete;

    ring_buffer_t& operator=(const ring_buffer_t&) = delete;
    ring_buffer_t& operator=(ring_buffer_t&&) = delete;

    bool push(T&& item);
    std::optional<T> pop();

    std::uint32_t size() const noexcept;
    std::uint32_t capacity() const noexcept;

private:
    std::uint32_t get_insert_pos(std::uint32_t pos, std::uint32_t size) const noexcept;
    std::uint32_t next_extract_position(std::uint32_t pos) const noexcept;

    std::vector<T> m_buffer;
    std::atomic<std::uint64_t> m_range;
    const std::uint32_t m_capacity;
};


namespace detail
{
    inline std::uint64_t pack(std::uint32_t pos, std::uint32_t size) noexcept
    {
        return static_cast<std::uint64_t>(pos) << 32 | static_cast<std::uint64_t>(size);
    }

    inline std::tuple<std::uint32_t, std::uint32_t> unpack(std::uint64_t packed) noexcept
    {
        return { static_cast<std::uint32_t>(packed >> 32), static_cast<std::uint32_t>(packed & 0xFFFFFFFF) };
    }
}

template<class T>
inline ring_buffer_t<T>::ring_buffer_t(std::uint32_t capacity)
    : m_buffer(capacity)
    , m_range(0)
    , m_capacity(capacity)
{
}

template<class T>
bool ring_buffer_t<T>::push(T&& item)
{
    std::uint64_t cur_range = m_range;
    auto [pos, size] = detail::unpack(cur_range);

    if (size >= m_capacity)
        return false;

    const std::uint32_t insert_pos = get_insert_pos(pos, size);
    m_buffer[insert_pos] = std::move(item);

    std::uint64_t new_range = detail::pack(pos, size + 1);
    bool ok = m_range.compare_exchange_weak(cur_range, new_range,
        std::memory_order_release, std::memory_order_relaxed);

    while (!ok)
    {
        const auto [mod_pos, mod_size] = detail::unpack(cur_range);
        assert(pos != mod_pos && size > mod_size);

        pos = mod_pos;
        size = mod_size;

        cur_range = detail::pack(pos, size);
        new_range = detail::pack(pos, size + 1);

        ok = m_range.compare_exchange_weak(
            cur_range, new_range, std::memory_order_release, std::memory_order_relaxed);
    }

    return true;
}

template<class T>
std::optional<T> ring_buffer_t<T>::pop()
{
    std::optional<T> res;

    std::uint64_t cur_range = m_range;
    auto [pos, size] = detail::unpack(cur_range);

    if (size == 0)
        return res;

    res = std::move(m_buffer[pos]);
    const std::uint32_t next_pos = next_extract_position(pos);

    std::uint64_t new_range = detail::pack(next_pos, size - 1);
    bool ok = m_range.compare_exchange_weak(
        cur_range, new_range,std::memory_order_release, std::memory_order_relaxed);

    while (!ok)
    {
        const auto [mod_pos, mod_size] = detail::unpack(cur_range);
        assert(pos == mod_pos && size < mod_size);

        size = mod_size;

        cur_range = detail::pack(pos, size);
        new_range = detail::pack(next_pos, size - 1);

        ok = m_range.compare_exchange_weak(
            cur_range, new_range, std::memory_order_release, std::memory_order_relaxed);
    }

    return res;
}

template<class T>
inline std::uint32_t ring_buffer_t<T>::size() const noexcept
{
    const auto[_, size] = detail::unpack(m_range);
    return size;
}

template<class T>
inline std::uint32_t ring_buffer_t<T>::capacity() const noexcept
{
    return m_capacity;
}

template<class T>
inline std::uint32_t ring_buffer_t<T>::get_insert_pos(std::uint32_t pos, std::uint32_t size) const noexcept
{
    const std::uint32_t raw_pos = pos + size;
    return raw_pos < m_capacity ? raw_pos : raw_pos - m_capacity;
}

template<class T>
inline std::uint32_t ring_buffer_t<T>::next_extract_position(std::uint32_t pos) const noexcept
{
    const std::uint32_t raw_pos = pos + 1;
    return raw_pos < m_capacity ? raw_pos : raw_pos - m_capacity;
}

}   // namespace util
