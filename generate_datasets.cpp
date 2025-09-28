#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <string>
#include <fstream>
#include <ctime>

using namespace std;

const string DNANB = "ACTG";
const int SEQ_MAX_LENGTH = 100; // max len allowed

string
generate_DNA()
{
    int length = 1 + rand() % SEQ_MAX_LENGTH;
    string seq;

    seq.reserve(length);
    for (int i = 0; i < length; ++i)
    {
        seq.push_back(DNANB[rand() % 4]);
    }
    return seq;
}

int
main(int argc, char* argv[])
{
    if (argc != 3)
    {
        cerr << "Use: " << argv[0] << " <number_of_seq> <output_file>\n";
        return 1;
    }

    long long n = atoll(argv[1]);
    string filename = argv[2];

    ofstream fout(filename);
    if (!fout)
    {
        cerr << "Error opening output file\n";
        return 1;
    }

    srand(time(NULL));

    for (long long i = 0; i < n; i++)
    {
        fout << generate_DNA() << "\n";
    }

    fout.close();
    return 0;
}