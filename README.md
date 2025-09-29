# Ordenação de Grandes Volumes de Dados Genômicos usando MPI
Este trabalho tem como objetivo implementar e avaliar estratégias de ordenação de sequências genômicas, comparando uma solução sequencial com outra paralela desenvolvida com MPI (message Passing Interface) e implementada em C/C++.

**Para gerar os datasets:**  

```sh
g++ generate_datasets.cpp -o gen_dna
```
```sh
# 100 mil sequências
./gen_dna 100000 dna_100k.txt
 
# 1 milhão de sequências
./gen_dna 1000000 dna_1M.txt

# 10 milhões de sequências
./gen_dna 10000000 dna_10M.txt
```
