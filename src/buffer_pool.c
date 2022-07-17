#include "buffer_pool.h"
#include "file_io.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void init_buffer_pool(const char *filename, BufferPool *pool) {
    // FILE *debug_fp = fopen("buffer_pool.log", "a");
    // fprintf(debug_fp, "buffer pool init begin\n");
    FileInfo file;
    open_file(&file, filename);
    pool->file = file;
    for (int i = 0; i != CACHE_PAGE; i++){
        pool->addrs[i] = -1;
        pool->cnt[i] = -1;
        pool->ref[i] = 0;
    }
    // memset(pool->cnt, -1, sizeof(pool->cnt));
    // memset(pool->ref, 0, sizeof(pool->ref));

}

void close_buffer_pool(BufferPool *pool) {

    FileInfo file = pool->file;
    for (int i = 0; i != CACHE_PAGE; i++) {
        write_page(&(pool->pages[i]), &(pool->file), pool->addrs[i]);
    }
    close_file(&file);

}

Page *get_page(BufferPool *pool, off_t addr) {

    for (int i = 0; i != CACHE_PAGE; i++){
        if (pool->addrs[i] == addr){
            pool->ref[i]++;
            pool->cnt[i]++;
            return &pool->pages[i];
        }
    }

    int free_i = -1;
    int free_num = 0;
    int free_pages[CACHE_PAGE] = {0};
    for (int i = 0; i != CACHE_PAGE; i++) {
        if (pool->ref[i] == 0) {
            free_pages[free_num] = i;
            free_num++;
        }
    }

    for (int i = 0; i != free_num; i++) {

        if (free_i == -1) {
            free_i = free_pages[i];
        }
        else if (pool->cnt[free_i] > pool->cnt[free_pages[i]]) {
            free_i = free_pages[i];
        }

    }
    if (free_i == -1){

        return NULL;
    }
    else{
        if (pool->cnt[free_i] != -1) {
            write_page(&pool->pages[free_i], &pool->file, pool->addrs[free_i]);
        }
        else {
        }
        FileIOResult result = read_page(&pool->pages[free_i], &pool->file, addr);
        //printf("get addr = %lld\n",addr);
        //printf("%d",result);
        if (result == ADDR_OUT_OF_RANGE && addr >= 0) {

            pool->file.length = addr + PAGE_SIZE;
            memset(&(pool->pages[free_i]), 0, PAGE_SIZE);
            pool->addrs[free_i] = addr;
            for (int i = 0; i != CACHE_PAGE; i++) {
                if (pool->cnt[i] != -1) pool->cnt[i] = 0;
            }
            pool->cnt[free_i] = 1;
            pool->ref[free_i] = 1;

            return &pool->pages[free_i];
        }
        if (result == FILE_IO_SUCCESS) {
            pool->addrs[free_i] = addr;
            for (int i = 0; i != CACHE_PAGE; i++) {
                if (pool->cnt[i] != -1) pool->cnt[i] = 0;
            }
            pool->cnt[free_i] = 1;
            pool->ref[free_i] = 1;
            //printf("int success\n");

            return &pool->pages[free_i];
        }else{
            //printf("get invalid page\n");

        }
    }
}

void release(BufferPool *pool, off_t addr) {

    for (int i = 0; i != CACHE_PAGE; i++){
    }
    for (int i = 0; i != CACHE_PAGE; i++){
        if (pool->addrs[i] == addr){
            write_page(&(pool->pages)[i],&(pool->file),addr);
            pool->ref[i]--;
            return;
        }
    }
    return;
}

/* void print_buffer_pool(BufferPool *pool) {
}

/* void validate_buffer_pool(BufferPool *pool) {
} */
