#pragma once
#include <vector>
#include <string>
#include <cstring>
#include <algorithm>

using namespace std;

// Empacotamento de strings
struct
Packed
{
    vector<int> lens;
    vector<char> chars;
};

static inline Packed
pack_strings(const vector<string>& v)
{
    Packed p; p.lens.reserve(v.size());
    size_t total = 0;

    // Guarda todos os comprimentos
    for (const auto& s : v)
    {
        p.lens.push_back((int)s.size());
        total += s.size();
    }
    p.chars.resize(total);

    size_t pos = 0;
    
    // Copia todos os chars em um buffer só
    for (const auto& s : v)
    {
        memcpy(p.chars.data()+pos, s.data(), s.size());
        pos += s.size();
    }
    return p;
}

static inline vector<string>
unpack_strings(const vector<int>& lens, const vector<char>& chars)
{
    vector<string> out; out.reserve(lens.size());
    size_t pos = 0;

    // Percorre lens, recorda cada string do buffer chars
    // e devolve o vetor de strings.
    for (int L : lens)
    {
        out.emplace_back(chars.data()+pos, chars.data()+pos+L);
        pos += L;
    }
    return out;
}

// Prefix sums para displacements
static inline vector<int>
prefix_sums(const vector<int>& counts)
{
    vector<int> displs(counts.size(), 0);

    for (size_t i = 1; i < counts.size(); ++i)
        displs[i] = displs[i-1] + counts[i-1];
    return displs;
}

// Achata um vetor 2D (na ordem 0..p-1) em um único buffer.
template <class T>
static inline vector<T>
flatten_2d(const vector<vector<T>>& v2,
           const vector<int>& /*displs*/,
           const vector<int>& /*counts*/)
{
    size_t total = 0;

    for (auto& v : v2)
        total += v.size();

    vector<T> out; out.reserve(total);
    
    for (auto& v : v2)
        out.insert(out.end(), v.begin(), v.end());
    return out;
}
