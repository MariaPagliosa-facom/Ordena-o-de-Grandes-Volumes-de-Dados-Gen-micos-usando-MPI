#include <mpi.h>
#include <fstream>
#include <iostream>
#include <algorithm>
#include "helpers.h"
#include "phases.h"

using namespace std;

void
distribution_phase(const char* in_path,
    int size,
    int rank,
    long long& N,
    vector<string>& local,
    int& local_bytes,
    double& tA,
    double& tB,
    int& LOC_MIN,
    int& LOC_MAX)
{
    vector<string> all;
    
    // Leitura no rank 0
    if (rank == 0)
    {
        ifstream fin(in_path);
        
        if (!fin)
        {
            cerr << "Erro ao abrir " << in_path << "\n";
            MPI_Abort(MPI_COMM_WORLD, 2);
        }
        
        string line;
        
        while (getline(fin, line))
        all.push_back(move(line));
        N = (long long)all.size();
    }
    MPI_Barrier(MPI_COMM_WORLD);
    tA = MPI_Wtime();  // início Fase A
    // Broadcast de N e cálculo da partição
    MPI_Bcast(&N, 1, MPI_LONG_LONG, 0, MPI_COMM_WORLD);

    long long base = N / size;
    long long rest = N % size;
    int local_n = (int)(base + (rank < rest ? 1 : 0));

    // Metadados por linhas
    vector<int> sendcounts_len, displs_len, lens_total;
    Packed P; // vamos empacotar 'all' no rank 0

    if (rank == 0)
    {
        sendcounts_len.resize(size);
        displs_len.resize(size);
        P = pack_strings(all);
        lens_total = P.lens;  // comprimentos de todas as strings

        long long off = 0;

        for (int r = 0; r < size; ++r)
        {
            long long cnt = base + (r < rest ? 1 : 0);

            sendcounts_len[r] = (int)cnt;
            displs_len[r] = (int)off;
            off += cnt;
        }
    }

    vector<int> local_lens(local_n);

    // Scatterv dos comprimentos para cada rank
    MPI_Scatterv(
        rank == 0 ? lens_total.data() : nullptr,
        rank == 0 ? sendcounts_len.data(): nullptr,
        rank == 0 ? displs_len.data() : nullptr,
        MPI_INT,
        local_lens.data(),
        local_n,
        MPI_INT,
        0,
        MPI_COMM_WORLD);

    // Calcula bytes locais
    local_bytes = 0;

    for (int L : local_lens)
        local_bytes += L;

    // Metadados por bytes + Scatterv dos chars
    vector<int> sendcounts_bytes, displs_bytes;
    vector<char> chars_total; // buffer de todos os chars (só r0)

    if (rank == 0)
    {
        sendcounts_bytes.resize(size);
        displs_bytes.resize(size);

        // Prefixo de bytes com base em lens_total
        vector<long long> pref_bytes((size_t)N + 1, 0);

        for (long long i = 0; i < N; ++i)
            pref_bytes[(size_t)(i+1)] = pref_bytes[(size_t)i] + lens_total[(size_t)i];
        for (int r = 0; r < size; ++r)
        {
            long long startL = displs_len[r];
            long long cntL = sendcounts_len[r];
            long long endL = startL + cntL;
            long long startB = pref_bytes[(size_t)startL];
            long long endB = pref_bytes[(size_t)endL];

            displs_bytes[r] = (int)startB;
            sendcounts_bytes[r] = (int)(endB - startB);
        }
        chars_total = P.chars; // já concatenado
    }

    vector<char> local_chars((size_t)local_bytes);

    MPI_Scatterv(
        rank == 0 ? chars_total.data() : nullptr,
        rank == 0 ? sendcounts_bytes.data() : nullptr,
        rank == 0 ? displs_bytes.data() : nullptr,
        MPI_CHAR,
        local_chars.data(),
        local_bytes,
        MPI_CHAR,
        0,
        MPI_COMM_WORLD);

    // Reconstrução do vetor 'local'
    local.clear();
    local.reserve(local_n);
    {
        auto data = local_chars.data();

        for (int L : local_lens)
        {
            local.emplace_back(data, data + L);
            data += L;
        }
    }
    
    int loc_n = (int)local.size();
    
    MPI_Reduce(&loc_n, &LOC_MIN, 1, MPI_INT, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&loc_n, &LOC_MAX, 1, MPI_INT, MPI_MAX, 0, MPI_COMM_WORLD);
    // Marca fim da fase
    MPI_Barrier(MPI_COMM_WORLD);
    tB = MPI_Wtime();
}

void
local_sort(vector<string>& local, double& tB, double& tC)
{
    sort(local.begin(), local.end());
    MPI_Barrier(MPI_COMM_WORLD);
    tC = MPI_Wtime(); // fim Fase B (ordenação local)
}

void
splitters_phase(const vector<string>& local,
  int size,
  int rank,
  vector<string>& splitters,
  double& tD)
{
    vector<string> samples;
    int s = (size > 1) ? (size - 1) : 0; // p-1 amostras por rank

    if (!local.empty() && s > 0)
    {
        // Índices regulares: k * n / (s+1), k=1..s
        for (int k = 1; k <= s; ++k)
        {
            size_t idx = (size_t)((1LL * k * local.size()) / (s + 1));

            if (idx >= local.size())
                idx = local.size() - 1;
            samples.push_back(local[idx]);
        }
    }

    Packed Ps = pack_strings(samples);
    const vector<int>& samples_lens = Ps.lens;
    const vector<char>& samples_chars = Ps.chars;

    // Quantidades (número de strings) por rank
    int samples_count_local = (int)samples.size();
    vector<int> samples_counts_all;  // só usado no rank 0

    if (rank == 0)
        samples_counts_all.resize(size);
    MPI_Gather(
        &samples_count_local,
        1,
        MPI_INT,
        rank == 0 ? samples_counts_all.data() : nullptr,
        1,
        MPI_INT,
        0,
        MPI_COMM_WORLD);
    
    //Gatherv dos comprimentos das amostras (INT)
    vector<int> recv_lens_counts, recv_lens_displs;
    vector<int> samples_lens_all;    // só rank 0
    int total_samples = 0;

    if (rank == 0)
    {
        recv_lens_counts.resize(size);
        recv_lens_displs.resize(size);

        int off = 0;

        for (int r = 0; r < size; ++r)
        {
            recv_lens_counts[r] = samples_counts_all[r];
            recv_lens_displs[r] = off;
            off += samples_counts_all[r];
        }
        total_samples = off;
        samples_lens_all.resize(total_samples);
    }

    MPI_Gatherv(
        samples_lens.data(),
        samples_count_local,
        MPI_INT,
        rank == 0 ? samples_lens_all.data() : nullptr,
        rank == 0 ? recv_lens_counts.data() : nullptr,
        rank == 0 ? recv_lens_displs.data() : nullptr,
        MPI_INT,
        0,
        MPI_COMM_WORLD);

    // Gatherv dos caracteres das amostras (CHAR)
    vector<int> samples_bytes_all_counts, samples_bytes_all_displs;
    vector<char> samples_chars_all;  // só rank 0
    int samples_bytes_local = (int)samples_chars.size();

    if (rank == 0)
    {
        // converte "lens por amostra" em deslocamentos em BYTES por rank
        samples_bytes_all_counts.resize(size);
        samples_bytes_all_displs.resize(size);

        // soma bytes por rank com base em samples_counts_all e samples_lens_all
        int off_lens = 0;
        int off_bytes = 0;

        for (int r = 0; r < size; ++r)
        {
            long long sum_r = 0;

            for (int i = 0; i < samples_counts_all[r]; ++i)
                sum_r += samples_lens_all[off_lens + i];
            samples_bytes_all_counts[r] = (int)sum_r;
            samples_bytes_all_displs[r] = off_bytes;
            off_lens += samples_counts_all[r];
            off_bytes += (int)sum_r;
        }
        samples_chars_all.resize(off_bytes);
    }

    MPI_Gatherv(
        samples_chars.data(),
        samples_bytes_local,
        MPI_CHAR,
        rank == 0 ? samples_chars_all.data() : nullptr,
        rank == 0 ? samples_bytes_all_counts.data() : nullptr,
        rank == 0 ? samples_bytes_all_displs.data() : nullptr,
        MPI_CHAR,
        0,
        MPI_COMM_WORLD);

    // Rank 0 reconstrói amostras, escolhe splitters e faz broadcast
    int m = (size > 1) ? (size - 1) : 0; // quantidade de splitters

    if (rank == 0)
    {
        // Reconstroi vetor de amostras globais
        vector<string> samples_all;

        samples_all.reserve(total_samples);

        int pos_lens = 0;
        auto data = samples_chars_all.data();

        for (int r = 0; r < size; ++r)
            for (int i = 0; i < samples_counts_all[r]; ++i)
            {
                int L = samples_lens_all[pos_lens++];

                samples_all.emplace_back(data, data + L);
                data += L;
            }

        // Ordena amostras globais
        sort(samples_all.begin(), samples_all.end());

        // Seleciona m splitters em posições regulares
        splitters.resize(m);
        if (!samples_all.empty() && m > 0)
            for (int k = 1; k <= m; ++k)
            {
                size_t idx = (size_t)((1LL * k * samples_all.size()) / size);

                if (idx >= samples_all.size())
                    idx = samples_all.size() - 1;
                splitters[k - 1] = samples_all[idx];
            }
    }

    // Empacota splitters para broadcast (lens + chars)
    int split_count = m;
    int split_bytes = 0;
    vector<int> split_lens;
    vector<char> split_chars;

    if (rank == 0)
    {
        split_lens.reserve(split_count);
        for (auto& s : splitters)
        {
            split_lens.push_back((int)s.size());
            split_bytes += (int)s.size();
        }
        split_chars.resize(split_bytes);

        int pos = 0;
        
        for (auto& s : splitters)
        {
            memcpy(split_chars.data() + pos, s.data(), s.size());
            pos += (int)s.size();
        }
    }

    // Broadcast da contagem de splitters, depois lens, depois chars
    MPI_Bcast(&split_count, 1, MPI_INT, 0, MPI_COMM_WORLD);
    splitters.clear();
    splitters.resize(split_count);

    if (split_count > 0)
    {
        if (rank != 0)
            split_lens.resize(split_count);
        MPI_Bcast(split_lens.data(), split_count, MPI_INT, 0, MPI_COMM_WORLD);

        int total_split_bytes = 0;

        for (int L : split_lens)
            total_split_bytes += L;
        if (rank != 0)
            split_chars.resize(total_split_bytes);

        MPI_Bcast(split_chars.data(), total_split_bytes, MPI_CHAR, 0, MPI_COMM_WORLD);

        // reconstruir splitters em todos os ranks
        splitters.clear();
        splitters.reserve(split_count);

        auto data = split_chars.data();

        for (int L : split_lens)
        {
            splitters.emplace_back(data, data + L);
            data += L;
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);
    tD = MPI_Wtime();  // fim Fase C (amostragem + splitters + Bcast)
}

void
global_exchange(const vector<string>& local,
    const vector<string>& splitters,
    int size,
    int rank,
    vector<string>& received,
    double& tD,
    double& tE,
    int& RECV_MIN,
    int& RECV_MAX,
    int& RECV_SUM)
{
    // Particionamento local em p baldes
    vector<vector<int>> buckets_lens(size);
    vector<vector<char>> buckets_chars(size);

    for (const auto& s : local)
    {
        int d = 0;
        if (!splitters.empty())
        {
            auto it = upper_bound(splitters.begin(), splitters.end(), s);
            d = (int)distance(splitters.begin(), it); // [0..p-1]
        }
        buckets_lens[d].push_back((int)s.size());

        size_t old = buckets_chars[d].size();
        
        buckets_chars[d].resize(old + s.size());
        memcpy(buckets_chars[d].data() + old, s.data(), s.size());
    }

    // Metadados de envio (quantidades por destino)
    vector<int> sendcounts_len_p(size, 0), sendcounts_bytes_p(size, 0);

    for (int r = 0; r < size; ++r)
    {
        sendcounts_len_p[r] = (int)buckets_lens[r].size();
        sendcounts_bytes_p[r] = (int)buckets_chars[r].size();
    }

    // Troca de metadados: quanto receberei de cada rank
    vector<int> recvcounts_len_p(size, 0), recvcounts_bytes_p(size, 0);

    MPI_Alltoall(
        sendcounts_len_p.data(),
        1,
        MPI_INT,
        recvcounts_len_p.data(),
        1,
        MPI_INT,
        MPI_COMM_WORLD);
    MPI_Alltoall(
        sendcounts_bytes_p.data(),
        1,
        MPI_INT,
        recvcounts_bytes_p.data(),
        1,
        MPI_INT,
        MPI_COMM_WORLD);

    int recv_total_len = 0, recv_total_bytes = 0;
    for (int r = 0; r < size; ++r)
    {
        recv_total_len += recvcounts_len_p[r];
        recv_total_bytes += recvcounts_bytes_p[r];
    }

    // Deslocamentos + buffers achatados de envio ---
    vector<int> sdispls_len_p = prefix_sums(sendcounts_len_p);
    vector<int> sdispls_bytes_p = prefix_sums(sendcounts_bytes_p);
    vector<int> send_lens_concat = flatten_2d(buckets_lens);
    vector<char> send_chars_concat = flatten_2d(buckets_chars);

    // --- 8) Deslocamentos de recepção + buffers ---
    vector<int> rdispls_len_p = prefix_sums(recvcounts_len_p);
    vector<int> rdispls_bytes_p = prefix_sums(recvcounts_bytes_p);
    vector<int> recv_lens_concat(recv_total_len);
    vector<char> recv_chars_concat(recv_total_bytes);

    // Alltoallv: LENS e CHARS ---
    MPI_Alltoallv(
        send_lens_concat.data(),
        sendcounts_len_p.data(),
        sdispls_len_p.data(),
        MPI_INT,
        recv_lens_concat.data(),
        recvcounts_len_p.data(),
        rdispls_len_p.data(),
        MPI_INT,
        MPI_COMM_WORLD
    );
    MPI_Alltoallv(
        send_chars_concat.data(),
        sendcounts_bytes_p.data(),
        sdispls_bytes_p.data(),
        MPI_CHAR,
        recv_chars_concat.data(),
        recvcounts_bytes_p.data(),
        rdispls_bytes_p.data(),
        MPI_CHAR,
        MPI_COMM_WORLD
    );

    // Reconstrução + ordenação final local
    received = unpack_strings(recv_lens_concat, recv_chars_concat);
    sort(received.begin(), received.end());
    
    // Métricas agregadas (min/max/sum) do tamanho recebido
    int recv_n = (int)received.size();
    
    MPI_Reduce(&recv_n, &RECV_MIN, 1, MPI_INT, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&recv_n, &RECV_MAX, 1, MPI_INT, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&recv_n, &RECV_SUM, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    // Timer fim da fase
    MPI_Barrier(MPI_COMM_WORLD);
    tE = MPI_Wtime();
}

void
write_phase(const vector<string>& received,
    const char* out_path,
    int size,
    int rank,
    double& tE,
    double& tF)
{
    // Empacota o bloco final
    Packed Po = pack_strings(received);
    const vector<int>& out_lens = Po.lens;
    const vector<char>& out_chars = Po.chars;
    int out_count_local = (int)out_lens.size();
    int out_bytes_local = (int)out_chars.size();
    vector<int> all_counts, all_bytes;
    
    // Rank 0 quer saber quantas strings/bytes de cada rank
    if (rank == 0)
    {
        all_counts.resize(size);
        all_bytes.resize(size);
    }
    MPI_Gather(
        &out_count_local,
        1,
        MPI_INT,
        rank == 0 ? all_counts.data() : nullptr,
        1,
        MPI_INT,
        0,
        MPI_COMM_WORLD);
    MPI_Gather(
        &out_bytes_local,
        1,
        MPI_INT,
        rank == 0 ? all_bytes.data() : nullptr,
        1,
        MPI_INT,
        0,
        MPI_COMM_WORLD);

    // Deslocamentos + buffers globais no rank 0
    vector<int> out_lens_displs, out_chars_displs;
    vector<int> all_lens;   // todos os comprimentos
    vector<char> all_chars; // todos os chars
    int total_strings = 0, total_bytes = 0;

    if (rank == 0)
    {
        out_lens_displs = prefix_sums(all_counts);
        out_chars_displs = prefix_sums(all_bytes);
        total_strings = out_lens_displs.back() + all_counts.back();
        total_bytes = out_chars_displs.back() + all_bytes.back();
        all_lens.resize(total_strings);
        all_chars.resize(total_bytes);
    }
    // Gatherv: comprimentos
    MPI_Gatherv(
        out_lens.data(),
        out_count_local,
        MPI_INT,
        rank == 0 ? all_lens.data() : nullptr,
        rank == 0 ? all_counts.data() : nullptr,
        rank == 0 ? out_lens_displs.data() : nullptr,
        MPI_INT,
        0,
        MPI_COMM_WORLD);
    // Gatherv: caracteres
    MPI_Gatherv(
        out_chars.data(),
        out_bytes_local,
        MPI_CHAR,
        rank == 0 ? all_chars.data() : nullptr,
        rank == 0 ? all_bytes.data() : nullptr,
        rank == 0 ? out_chars_displs.data() : nullptr,
        MPI_CHAR,
        0,
        MPI_COMM_WORLD);
    MPI_Barrier(MPI_COMM_WORLD);
    tF = MPI_Wtime();
    
    // Rank 0 escreve o arquivo final
    if (rank == 0)
    {
        ofstream fout(out_path);
        if (!fout)
        {
            cerr << "Erro ao abrir saída " << out_path << "\n";
            MPI_Abort(MPI_COMM_WORLD, 4);
        }

        int pos = 0;

        for (int i = 0; i < total_strings; ++i)
        {
            int L = all_lens[i];

            fout.write(all_chars.data() + pos, L);
            fout.put('\n');
            pos += L;
        }
        fout.close();
    }
}
