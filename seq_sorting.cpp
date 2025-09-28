#include <cstdio>
#include <string>
#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>
#include <ctime>
#include <algorithm>

using namespace std;

int main(int argc, char* argv[])
{
    if(argc !=3)
    {
        cerr << "Use: " << argv[0] << " <input_file> <output_file>";
        return 1;
    }

    string input_file = argv[1];
    string output_file = argv[2];

    ifstream fin(input_file);
    if (!fin)
    {
        cerr << "Error opening input file";
        return 1;
    }

    vector<string> sequence;
    string line;

    while (getline(fin, line))
    {
        if (!line.empty())
            sequence.push_back(line);
    }
    fin.close();

    clock_t start = clock();

    sort(sequence.begin(), sequence.end());

    clock_t end = clock();
    double time = double(end - start) / CLOCKS_PER_SEC;

    cout << "Seq time: " << time << " seconds\n";

    ofstream fout(output_file);
    for (auto& seq : sequence)
    {
        fout << seq << "\n";
    }
    fout.close();
    return 0;
}