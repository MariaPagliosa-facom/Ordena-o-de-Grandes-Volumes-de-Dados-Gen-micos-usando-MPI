#include "helpers.h"
#include <iostream>
#include <mpi.h>
#include "phases.h"

using namespace std;

int
main(int argc, char** argv)
{
    MPI_Init(&argc, &argv);

    int size = 0, rank = 0;
    MPI_Comm_size(MPI_COMM_WORLD, &size); // quantos processos
    MPI_Comm_rank(MPI_COMM_WORLD, &rank); // id deste processo

    if (argc < 3)
    {
        if (rank == 0)
            cerr << "Uso: " << argv[0] << " <input_file> <output_file>\n";
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    
    // Timers
    double tA, tB, tC, tD, tE, tF; // marcas
    double dA, dB, dC, dD, dE, dTOT; // durações locais
    double DA, DB, DC, DD, DE, DTOT; // durações (máximo entre os ranks)

    long long N = 0;
    vector<string> local;
    int local_bytes = 0;
    int LOC_MIN = 0, LOC_MAX = 0;

    distribution_phase(argv[1], size, rank, N, local, local_bytes, tA, tB, LOC_MIN, LOC_MAX);
    local_sort(local, tB, tC);

    vector<string> splitters;
    splitters_phase(local, size, rank, splitters, tD);

    vector<string> received;
    int RECV_MIN = 0, RECV_MAX = 0, RECV_SUM = 0;

    global_exchange(local, splitters, size, rank, received, tD, tE, RECV_MIN, RECV_MAX, RECV_SUM);
    write_phase(received, argv[2], size, rank, tE, tF);

    // Durações locais
    dA   = tB - tA; // leitura + distribuição
    dB   = tC - tB; // sort local
    dC   = tD - tC; // amostragem + splitters (gather+bcast)
    dD   = tE - tD; // partição + all-to-all[v] + reconstruct + sort final
    dE   = tF - tE; // gather final + escrita
    dTOT = tF - tA; // total

    // Reduzir (máximo entre ranks) para o rank 0
    MPI_Reduce(&dA,   &DA,   1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&dB,   &DB,   1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&dC,   &DC,   1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&dD,   &DD,   1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&dE,   &DE,   1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&dTOT, &DTOT, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

    // Imprimir
    if (rank == 0)
    {
        cout.setf(ios::fixed);
        cout.precision(6);
        cout << "[SUMMARY] p=" << size
                << "\nN=" << N
                << " out=" << (argc >= 3 ? argv[2] : "(stdout)")
                << " | local_n[min..max]=" << LOC_MIN << ".." << LOC_MAX
                << " | recv_n[min..max]="  << RECV_MIN << ".." << RECV_MAX
                << " \nA=" << DA   // leitura+dist
                << " \nB=" << DB     // sort local
                << " \nC=" << DC     // splitters
                << " \nD=" << DD     // alltoall+final
                << " \nE=" << DE     // gather+IO
                << " \nTOTAL=" << DTOT
                << "\n";
    }

    MPI_Finalize();
    return 0;
}
