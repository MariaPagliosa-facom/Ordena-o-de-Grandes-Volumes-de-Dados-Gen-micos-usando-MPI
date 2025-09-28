#pragma once
#include <vector>
#include <string>

using namespace std;

void distribution_phase(const char* in_path, int size, int rank,
                       long long& N,
                       vector<string>& local, int& local_bytes,
                       double& tA, double& tB,
                       int& LOC_MIN, int& LOC_MAX);

void local_sort(vector<string>& local,
                     double& tB, double& tC);

void splitters_phase(const vector<string>& local,
                    int size, int rank,
                    vector<string>& splitters,
                    double& tD);

void global_exchange(const vector<string>& local,
                       const vector<string>& splitters,
                       int size, int rank,
                       vector<string>& received,
                       double& tD, double& tE,
                       int& RECV_MIN, int& RECV_MAX, int& RECV_SUM);

void write_phase(const vector<string>& received,
                         const char* out_path,
                         int size, int rank,
                         double& tE, double& tF);

