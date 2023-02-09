// Reference:
// to calculate the difference in time I used the method from here-"https://stackoverflow.com/a/5362664"
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/time.h>


struct threads
{
    int thread_id;
    int start_data;
    int end_data;
};

// all the main array/pointers used in the 4 phases
int* entire_data_pointer;
int* regular_sample_pointer;
int* pivot_pointer;
int* partition_pointer;
int* marged_partition_pointer;

int p;
int w;
int n;
int ro;
pthread_barrier_t barrier;

// creates a thread object
// thread_id = id of the thread
// start_data = index of the starting element for this thread
// end_data = index of the ending element for this thread
struct threads* createThreadObject(int t_id, int eachThreadDataNumber){
    struct threads* t = malloc(sizeof(struct threads));
    t->thread_id = t_id;
    t->start_data = t_id*eachThreadDataNumber;
    t->end_data = t_id*eachThreadDataNumber + eachThreadDataNumber;
    return t;    
}


// comparison function for sorting 
int cmpfunc (const void * a, const void * b) {
   return ( *(int*)a - *(int*)b );
}
/*
phase-1 
Does local sorting and takes sample.
It will take a thread and sort its data using the built-in quicksort
*/
void phase_1(struct threads* t){
    // start counting to measure the time taken in this phase

    struct timeval start_time1;
    gettimeofday(&start_time1, NULL);
    
    // initialize necessary variables
    int data_start = t->start_data;
    int data_end = t->end_data;
    int t_id = t->thread_id;
    int each_t_sample_number = 0;
    int number = data_end-data_start;
    int* starting_pos = entire_data_pointer+data_start;
    // sort the data
    qsort(starting_pos,number,sizeof(int),cmpfunc);
    
    // get the regular samples
    for (int j = 0; j < p; j++)
    {
        // heuristic is 1, w+1, 2w+1, ...,(p-1)w+1 to collect sample but array starts from 0 so can't do +1
        regular_sample_pointer[t_id*p+each_t_sample_number]=entire_data_pointer[data_start+ (j*w)];
        each_t_sample_number += 1;
    }
    
    // end counting time
    struct timeval end_time1;
    gettimeofday(&end_time1,NULL);
    long int elapsed = (long int)(end_time1.tv_sec-start_time1.tv_sec)*1000000 + (end_time1.tv_usec-start_time1.tv_usec);

    printf("Thread %d - Phase 1- Sorted- %d numbers time- %ld ms\n",t_id,(data_end-data_start), elapsed);
}

/*  Phase 2
Only the main thread will handle this phase since its sequential.
This function will sort the regular samples and find the pivots
*/
void phase_2(struct threads* t){
    int t_id = t->thread_id;

    // start counting time
    struct timeval start_time2;
    gettimeofday(&start_time2, NULL);    
    pthread_barrier_wait(&barrier);
    
    if (t_id == 0)
    {
        // sort the regular sample, there are p^2 items in the regular sample array
        int size = p*p;
        qsort(regular_sample_pointer, size, sizeof(int), cmpfunc);

        // find the pivot
        int pivot_index = 0; 
        for (int j = 0; j < p; j++)
        {
            pivot_pointer[pivot_index] = regular_sample_pointer[p*j+ro-1];
            pivot_index += 1;
        }
    }
    // stop counting time
    struct timeval end_time2;
    gettimeofday(&end_time2,NULL);
    long int elapsed = (long int)(end_time2.tv_sec-start_time2.tv_sec)*1000000 + (end_time2.tv_usec-start_time2.tv_usec);
    printf("Thread %d - Phase 2- time- %ld ms\n",t_id, elapsed);
}

/*
Phase 3-
Exchanges partitions based on the pivot so that the i'th thread recieves the i'th partitions from all other threads
the partition pointer array consists of the indexes of the partitions for each thread
Since, its shared memory the only thing that will be done in this phase is just reading and writing the partition
indexes into shared memory.
*/
void phase_3(struct threads* t){
    
    // start counting time
    struct timeval start_time3;
    gettimeofday(&start_time3, NULL);

    // initialize variables
    int data_start = t->start_data;
    int data_end = t->end_data;
    int t_id = t->thread_id;

    int pivot_counter = 0;
    int partition_counter = 1;
    int starting_pos = t_id*(p+1); 
    partition_pointer[starting_pos] = data_start;
    partition_pointer[starting_pos+p] = data_end;

    // partition the data
    int i = data_start;
    while (i < data_end && pivot_counter < p-1){
        if (pivot_pointer[pivot_counter] <= entire_data_pointer[i]){
            partition_pointer[starting_pos+partition_counter] = i;
            pivot_counter += 1;
            partition_counter += 1;
        }
        i += 1;
    }

    // stop counting time
    struct timeval end_time3;
    gettimeofday(&end_time3,NULL);
    long int elapsed = (long int)(end_time3.tv_sec-start_time3.tv_sec)*1000000 + (end_time3.tv_usec-start_time3.tv_usec);
    
    printf("Thread %d - Phase 3- time- %ld ms\n",t_id, elapsed);
}

/*
Phase 4
merges the data together
*/

void phase_4(struct threads* t){
    /// start counting time
    struct timeval start_time4;
    gettimeofday(&start_time4, NULL);

    //range is an array that consists of the partition range of each thread. [Sp,Ep] Sp=Starting point, Ep=Ending point
    int range[p*2];
    int t_id = t->thread_id;

    int range_index = 0;

    for (int i = 0; i < p; i++)
    {
        range[range_index] = partition_pointer[i*(p+1)+t_id];
        range_index++;
        range[range_index] = partition_pointer[i*(p+1)+t_id+1];
        range_index++;
    }
    // getting the total length of all the partitions in each processor used while merging the data into the original array
    int length = 0;
    for (int i = 0; i < p*2; i+=2){
        int diff = range[i+1] - range[i];
        length += diff;
    }

    int* merged= malloc(sizeof(int)* length);

    marged_partition_pointer[t_id] = length;

    int merged_index = 0;

    // k_way merge
    while (merged_index < length){
        int min = INT_MAX;
        int min_pos = 0;

        for(int i = 0; i < p*2; i+=2){
            if(range[i]!=range[i+1]){
                if(entire_data_pointer[range[i]]<min){
                    min = entire_data_pointer[range[i]];
                    min_pos = i;
                }
            }
        }
        if(min==INT_MAX){
            break;
        }
        else{
            range[min_pos] += 1;
            merged[merged_index] = min;
            merged_index ++;
        }
    }

    // end counting time
    struct timeval end_time4;
    gettimeofday(&end_time4,NULL);
    long int elapsed = (long int)(end_time4.tv_sec-start_time4.tv_sec)*1000000 + (end_time4.tv_usec-start_time4.tv_usec);
    
    printf("Thread %d - Phase 4- time- %ld ms\n",t_id, elapsed);

    // merge has the sorted array
    // wait for all other thread to finish and then merge it with the main array
    pthread_barrier_wait(&barrier);

    // merge into main array
    // start counting time
    struct timeval start_time5;
    gettimeofday(&start_time5, NULL);

    int start = 0;
    for (int i = 0; i < t_id; i++)
    {
        start += marged_partition_pointer[i];
    }
    for (int i = start; i < start+marged_partition_pointer[t_id]; i++)
    {
        entire_data_pointer[i] = merged[i-start];
    }
    
    // end counting time
    struct timeval end_time5;
    gettimeofday(&end_time5,NULL);
    long int elapsed_5 = (long int)(end_time5.tv_sec-start_time5.tv_sec)*1000000 + (end_time5.tv_usec-start_time5.tv_usec);
    
    printf("Thread %d - Merging into main array- time- %ld ms\n",t_id, elapsed_5);    
}

/*
main psrs algorithm
main idea taken from slides in the eclass
*/

void psrs(struct threads* t){
    // initialize the variables
    int t_id = t->thread_id;

    phase_1(t);

    phase_2(t);
    pthread_barrier_wait(&barrier);
    
    phase_3(t);
    pthread_barrier_wait(&barrier);
    

    phase_4(t);
    pthread_barrier_wait(&barrier);

    free(t);

    if(t_id==0){
        return;
    }
    pthread_exit(0);
}

int* createData(int n){
    srand(10);
    int* data = malloc(sizeof(int) * n);
    for (int i = 0; i < n; i++)
    {
        data[i] = (int) rand();
    }
    return data;
}

// prints array
void printArray(int* array, int size) {
	for (int i = 0; i < size; i++) {
		printf("%d ", array[i]);
	}
	printf("\n");
}

// checks if the array is sorted
void isSorted(int size) {
    int* entire_data_pointer_2 = createData(size);
    qsort(entire_data_pointer_2,size,sizeof(int),cmpfunc);
    for (int i = 0; i < size - 1; i++) {
		if (entire_data_pointer[i] != entire_data_pointer_2[i]) {
            printf("Not sorted: %d\n", entire_data_pointer[i]);
			return;	
		}
	}
	printf("Sorted\n");
    free(entire_data_pointer_2);
}

int main(int argc, char *argv[]){

    if (argc != 3){
        printf(stderr);
        printf("2 arguments required n and thread number\n");
        exit(1);
    }
    // initializing the global variables
    p = atoi(argv[1]);
    n = atoi(argv[2]);
    w = (n/(p*p));
    ro = p/2;

    int each_thread_data_amount = n/p;

    pthread_barrier_init(&barrier,NULL,p);
    pthread_t* thread_control_board = malloc(sizeof(pthread_t) * p);

    entire_data_pointer = createData(n);
    regular_sample_pointer= malloc(sizeof(int)*(p*p));
    pivot_pointer = malloc(sizeof(int) * (p-1));
    marged_partition_pointer = malloc(sizeof(int)*p);
    partition_pointer = malloc(sizeof(int) * p *(p+1));

    // create all the threads
    int i = 1;
    for ( ; i< p-1; i++)
    {
        struct threads* t = createThreadObject(i,each_thread_data_amount);
        pthread_create(&thread_control_board[i],NULL,psrs,(void *)t);
    }
    if (i < p){
        struct threads* t = createThreadObject(i,each_thread_data_amount);
        t->end_data = n;
        pthread_create(&thread_control_board[i],NULL,psrs,(void *)t);
    }

    struct threads* main_thread = createThreadObject(0, each_thread_data_amount);
    
    // main psrs part
    struct timeval start_time;
    gettimeofday(&start_time, NULL);
    
    psrs((void *)main_thread);
    
    struct timeval end_time;
    gettimeofday(&end_time,NULL);
    long int elapsed = (long int)(end_time.tv_sec-start_time.tv_sec)*1000000 + (end_time.tv_usec-start_time.tv_usec);
    
    printf("Total time- %ld ms\n", elapsed);

    //check if sorted
    isSorted(n);

    // cleanup process
    free(entire_data_pointer);
    free(regular_sample_pointer);
    free(marged_partition_pointer);
    free(pivot_pointer);
    free(partition_pointer);
    free(thread_control_board);
    pthread_barrier_destroy(&barrier);

    return 0;
}
