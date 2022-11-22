#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include <pthread.h>

struct thread_struct
{
    int thread_id;
    int reducers;
    int mapers;
    int ***power_list;
    int **reduce_power_list;
    int *reduce_power_sizes;
    char **inputfiles;
    char *filename;
    int *size;
    int size_power_list;
    int nr_files;
};

pthread_mutex_t mutex;
pthread_barrier_t barrier;

int checkPerfectPower(long int number, long int start, long int last, int exponent)
{
    long int mid;
    if (start <= last)
    {
        mid = (start + last) / 2;
    }

    if (start > last)
    {
        return -1;
    }

    if (pow(mid, exponent) == number)
    {
        return 1;
    }
    else if (pow(mid, exponent) > number)
    {
        return checkPerfectPower(number, start, mid - 1, exponent);
    }
    else
    {
        return checkPerfectPower(number, mid + 1, last, exponent);
    }
}

int remove_duplicate(int array[], int n)
{

    if (n == 0 || n == 1)
        return n;

    int temp[n];
    int j = 0;
    int i;

    for (i = 0; i < n - 1; i++)
    {
        if (array[i] != array[i + 1])
        {
            temp[j++] = array[i];
        }
    }
    temp[j++] = array[n - 1];

    for (i = 0; i < j; i++)
        array[i] = temp[i];

    return j;
}

void swap(int *xp, int *yp)
{
    int temp = *xp;
    *xp = *yp;
    *yp = temp;
}

void selectionSort(int arr[], int n)
{
    int i, j, min_idx;

    for (i = 0; i < n - 1; i++)
    {
        min_idx = i;
        for (j = i + 1; j < n; j++)
            if (arr[j] < arr[min_idx])
                min_idx = j;

        swap(&arr[min_idx], &arr[i]);
    }
}

void bubbleSort(int array[], int size)
{
    for (int step = 0; step < size - 1; ++step)
    {
        int swapped = 0;
        for (int i = 0; i < size - step - 1; ++i)
        {
            if (array[i] > array[i + 1])
            {
                int temp = array[i];
                array[i] = array[i + 1];
                array[i + 1] = temp;
                swapped = 1;
            }
        }
        if (swapped == 0)
        {
            break;
        }
    }
}

int partition(int array[], int low, int high) {
  
  // select the rightmost element as pivot
  int pivot = array[high];
  
  // pointer for greater element
  int i = (low - 1);

  // traverse each element of the array
  // compare them with the pivot
  for (int j = low; j < high; j++) {
    if (array[j] <= pivot) {
        
      // if element smaller than pivot is found
      // swap it with the greater element pointed by i
      i++;
      
      // swap element at i with element at j
      swap(&array[i], &array[j]);
    }
  }

  // swap the pivot element with the greater element at i
  swap(&array[i + 1], &array[high]);
  
  // return the partition point
  return (i + 1);
}

void quickSort(int array[], int low, int high) {
  if (low < high) {
    
    // find the pivot element such that
    // elements smaller than pivot are on left of pivot
    // elements greater than pivot are on right of pivot
    int pi = partition(array, low, high);
    
    // recursive call on the left of pivot
    quickSort(array, low, pi - 1);
    
    // recursive call on the right of pivot
    quickSort(array, pi + 1, high);
  }
}
void *thread_function(void *arg)
{
    struct thread_struct *data = (struct thread_struct *)arg;
    if (data->thread_id < data->mapers)
    {
        pthread_mutex_lock(&mutex);
        int N = data->nr_files;
        int P = data->mapers;
        int start = data->thread_id * N / P;
        int end;
        if ((data->thread_id + 1) * N / P > N)
            end = N;
        else
        {
            end = (data->thread_id + 1) * N / P;
        }
        for (int k = start; k < end; k++)
        {
            FILE *fp = fopen(data->inputfiles[k], "r"); // deschid in.txt
            if (fp == NULL)
            {
                printf("ERROR!!");
            }

            char buf[100];
            fgets(buf, 100, fp); // scot nr de numere
            int limit = atoi(buf);
            for (int i = 0; i < limit; i++)
            {
                char input[100];
                fgets(input, 100, fp);
                int number = atoi(input);

                for (int j = 2; j <= data->reducers + 1; j++)
                {
                    if (checkPerfectPower(number, 1, (int)sqrt(number), j) == 1)
                    {
                        data->power_list[data->thread_id][j - 2][data->size[j - 2]] = number;
                        data->size[j - 2]++;
                    }
                }
            }
            fclose(fp);
        }
        pthread_mutex_unlock(&mutex);
    }

    pthread_barrier_wait(&barrier);

    if (data->thread_id >= data->mapers)
    {
        pthread_mutex_lock(&mutex);
        int pos_exp = data->thread_id - data->mapers;
        for (int i = 0; i < data->mapers; i++)
        {
            for (int k = 0; k < 150; k++)
            {
                if (data->power_list[i][pos_exp][k])
                {
                    data->reduce_power_list[pos_exp][data->reduce_power_sizes[pos_exp]] = data->power_list[i][pos_exp][k];
                    data->reduce_power_sizes[pos_exp]++;
                }
                else
                {
                    break;
                }
            }
        }

        quickSort(data->reduce_power_list[pos_exp], 0, data->reduce_power_sizes[pos_exp] - 1);
        data->reduce_power_sizes[pos_exp] = remove_duplicate(data->reduce_power_list[pos_exp], data->reduce_power_sizes[pos_exp]);

        char buffer[12];
        sprintf(buffer, "out%d.txt", pos_exp + 2);
        FILE *outfp = fopen(buffer, "w");
        fprintf(outfp, "%d", data->reduce_power_sizes[pos_exp]);

        fclose(outfp);
        pthread_mutex_unlock(&mutex);
    }
    pthread_barrier_wait(&barrier);
    pthread_exit(NULL);
}

int main(int argc, char *argv[])
{
    int number_of_mappers;
    int number_of_reducers;
    char filename[100];
    number_of_mappers = atoi(argv[1]);
    number_of_reducers = atoi(argv[2]);

    pthread_t threads[number_of_mappers + number_of_reducers];
    int rc;

    strcpy(filename, argv[3]);
    //strcat(filename, "/test.txt");
    FILE *test_ptr = fopen(filename, "r+");
    if (test_ptr == NULL)
    {
        printf("ERROR!!");
    }
    int nr_fisiere;
    char *buffer;
    buffer = calloc(100, sizeof(char));
    fscanf(test_ptr, "%s", buffer);
    nr_fisiere = atoi(buffer);

    int **reduce_mat = (int **)malloc(number_of_reducers * sizeof(int *));
    for (int j = 0; j < number_of_reducers; j++)
    {
        reduce_mat[j] = (int *)malloc(sizeof(int) * 1000);
    }

    int ***matrix_pow = (int ***)malloc(number_of_mappers * sizeof(int **));
    for (int k = 0; k < number_of_mappers; k++)
    {
        matrix_pow[k] = (int **)malloc(sizeof(int *) * number_of_reducers);
        for (int j = 0; j < number_of_reducers; j++)
        {
            matrix_pow[k][j] = (int *)malloc(sizeof(int) * 1000);
        }
    }

    char **inputs = malloc(nr_fisiere * sizeof(char *));
    for (int i = 0; i < nr_fisiere; i++)
    {
        inputs[i] = malloc(130 * sizeof(char));
    }

    for (int i = 0; i < nr_fisiere; i++)
    {
        fscanf(test_ptr, "%s", inputs[i]);
    }

    struct thread_struct *thread_args = (struct thread_struct *)malloc(sizeof(struct thread_struct) * (number_of_mappers + number_of_reducers));

    for (int i = 0; i < number_of_mappers + number_of_reducers; i++)
    {
        thread_args[i].filename = argv[3];
        thread_args[i].mapers = number_of_mappers;
        thread_args[i].reducers = number_of_reducers;
        thread_args[i].inputfiles = inputs;
        thread_args[i].nr_files = nr_fisiere;
        thread_args[i].size_power_list = 0;
        thread_args[i].reduce_power_sizes = (int *)malloc(sizeof(int) * number_of_reducers);
        thread_args[i].size = (int *)malloc(sizeof(int) * number_of_reducers);
        for (int j = 0; j < number_of_reducers; j++)
        {
            thread_args[i].size[j] = 0;
        }
        thread_args[i].power_list = matrix_pow;
        thread_args[i].reduce_power_list = reduce_mat;
    }

    int verif = pthread_mutex_init(&mutex, NULL);
    if (verif != 0)
    {
        printf("\n mutex init failed\n");
        exit(-1);
    }

    int verif1 = pthread_barrier_init(&barrier, NULL, number_of_mappers + number_of_reducers);
    if (verif1 != 0)
    {
        printf("eroare\n");
        exit(-1);
    }

    for (int i = 0; i < number_of_mappers + number_of_reducers; i++)
    {
        thread_args[i].thread_id = i;
        rc = pthread_create(&threads[i], NULL, thread_function, &thread_args[i]);
        if (rc)
        {
            printf("Eroare la crearea thread-ului %d\n", i);
            exit(-1);
        }
    }

    for (int i = 0; i < number_of_mappers + number_of_reducers; i++)
    {
        rc = pthread_join(threads[i], NULL);

        if (rc)
        {
            printf("Eroare la asteptarea thread-ului %d\n", i);
            exit(-1);
        }
    }

    pthread_barrier_destroy(&barrier);
    pthread_mutex_destroy(&mutex);
    return 0;
}