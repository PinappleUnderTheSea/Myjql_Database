#include "hash_map.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void hash_table_init(const char *filename, BufferPool *pool, off_t n_directory_blocks) {
    init_buffer_pool(filename, pool);
    if (pool->file.length != 0) {
        return;
    }
    HashMapControlBlock *ctrl = (HashMapControlBlock*)get_page(pool, 0);
    ctrl->n_directory_blocks = n_directory_blocks;
    ctrl->free_block_head = -1;
    ctrl->lastpage = (n_directory_blocks + 1) * PAGE_SIZE;
    release(pool, 0);
    for (int i = 0; i != n_directory_blocks; i++) {
        HashMapDirectoryBlock *dir_block = (HashMapDirectoryBlock*)get_page(pool, PAGE_SIZE * (i + 1));
        memset(dir_block->directory, -1, PAGE_SIZE);
        release(pool, PAGE_SIZE * (i + 1));
    }
}

void hash_table_close(BufferPool *pool) {
    close_buffer_pool(pool);
}



void hash_table_insert(BufferPool *pool, short size, off_t addr) {
    HashMapDirectoryBlock *dir_block = (HashMapDirectoryBlock*)get_page(pool, (size / HASH_MAP_DIR_BLOCK_SIZE + 1) * PAGE_SIZE);
    off_t now_page_addr = dir_block->directory[size % HASH_MAP_DIR_BLOCK_SIZE];
    if (now_page_addr == -1) {
        HashMapControlBlock *ctrl = (HashMapControlBlock*)get_page(pool, 0);
        if (ctrl->free_block_head != -1) {
            dir_block->directory[size % HASH_MAP_DIR_BLOCK_SIZE] = ctrl->free_block_head;
            release(pool, (size / HASH_MAP_DIR_BLOCK_SIZE + 1) * PAGE_SIZE);
            HashMapBlock *fst_free_block = (HashMapBlock*)get_page(pool, ctrl->free_block_head);
            off_t nxt_free_block = fst_free_block->next;
            fst_free_block->next = -1;
            fst_free_block->n_items = 1;
            fst_free_block->table[0] = addr;
            release(pool, ctrl->free_block_head);
            ctrl->free_block_head = nxt_free_block;
            release(pool, 0);
            return;
        }
        else {
            off_t lastpage = ctrl->lastpage;
            dir_block->directory[size % HASH_MAP_DIR_BLOCK_SIZE] = lastpage;
            release(pool, (size / HASH_MAP_DIR_BLOCK_SIZE + 1) * PAGE_SIZE);
            HashMapBlock *new_block = (HashMapBlock*)get_page(pool, lastpage);
            new_block->n_items = 1;
            new_block->table[0] = addr;
            new_block->next = -1;
            release(pool, lastpage);
            ctrl->lastpage += PAGE_SIZE;
            release(pool, 0);

            return;
        }
    }
    else {
        release(pool, (size / HASH_MAP_DIR_BLOCK_SIZE + 1) * PAGE_SIZE);
        HashMapBlock *blk = (HashMapBlock*)get_page(pool, now_page_addr);
        if (blk->n_items != HASH_MAP_BLOCK_SIZE) {
            blk->table[blk->n_items] = addr;
            blk->n_items++;
            release(pool, now_page_addr);
            return;
        }
        else {
            HashMapControlBlock *ctrl = (HashMapControlBlock*)get_page(pool, 0);
            off_t lastpage = ctrl->lastpage;
            release(pool, now_page_addr);
            HashMapBlock *new_block = (HashMapBlock*)get_page(pool, lastpage);
            new_block->next = now_page_addr;
            new_block->n_items = 1;
            new_block->table[0] = addr;
            release(pool, lastpage);
            HashMapDirectoryBlock *dir_block = (HashMapDirectoryBlock*)get_page(pool, (size / HASH_MAP_DIR_BLOCK_SIZE + 1) * PAGE_SIZE);
            dir_block->directory[size % HASH_MAP_DIR_BLOCK_SIZE] = lastpage;
            ctrl->lastpage += PAGE_SIZE;
            release(pool, 0);
            release(pool, (size / HASH_MAP_DIR_BLOCK_SIZE + 1) * PAGE_SIZE);
            return;
        }
    }
}

off_t hash_table_pop_lower_bound(BufferPool *pool, short size) {

    HashMapControlBlock *ctrl = (HashMapControlBlock*)get_page(pool, 0);
    off_t n_directory_blocks = ctrl->n_directory_blocks;
    release(pool, 0);
    off_t dir_page_num = (size / HASH_MAP_DIR_BLOCK_SIZE + 1) * PAGE_SIZE;
    HashMapDirectoryBlock *dir_block = (HashMapDirectoryBlock*)get_page(pool, dir_page_num);
    off_t dir_table_num = size % HASH_MAP_DIR_BLOCK_SIZE;
    off_t addr = -1;

    while (dir_block->directory[dir_table_num] == -1 && (dir_page_num / PAGE_SIZE - 1) * HASH_MAP_DIR_BLOCK_SIZE + dir_table_num < n_directory_blocks * HASH_MAP_DIR_BLOCK_SIZE) {
        release(pool, dir_page_num);
        if (++dir_table_num == HASH_MAP_DIR_BLOCK_SIZE) {
            dir_table_num = 0;
            dir_page_num += PAGE_SIZE;
        }
        dir_block = (HashMapDirectoryBlock*)get_page(pool, dir_page_num);

    }
    if ((dir_page_num / PAGE_SIZE - 1) * HASH_MAP_DIR_BLOCK_SIZE + dir_table_num != n_directory_blocks * HASH_MAP_DIR_BLOCK_SIZE) {

        off_t map_block_addr = dir_block->directory[dir_table_num];
        release(pool, dir_page_num);
        HashMapBlock *map_block = (HashMapBlock*)get_page(pool, map_block_addr);
        addr = map_block->table[0];
        release(pool, map_block_addr);
        hash_table_pop(pool, (dir_page_num / PAGE_SIZE - 1) * HASH_MAP_DIR_BLOCK_SIZE + dir_table_num, addr);
    }
    else {
        release(pool, dir_page_num);
    }

    return addr;
}

void hash_table_pop(BufferPool *pool, short size, off_t addr) {
    HashMapDirectoryBlock *dir_block = (HashMapDirectoryBlock*)get_page(pool, (size / HASH_MAP_DIR_BLOCK_SIZE + 1) * PAGE_SIZE);
    off_t now_page = dir_block->directory[size % HASH_MAP_DIR_BLOCK_SIZE];
    release(pool, (size / HASH_MAP_DIR_BLOCK_SIZE + 1) * PAGE_SIZE);
    off_t frn_page = now_page;
    while (1) {
        HashMapBlock *now_block = (HashMapBlock*)get_page(pool, now_page);
        printf("now block get\n");
        off_t flag = -1;
        off_t nxt_page = now_block->next;
        printf("next pg get\n");
        for (off_t i = 0; i != now_block->n_items; i++){
            if (now_block->table[i] == addr)    flag = i;
        }
        if (flag == -1) {
            release(pool, now_page);
            frn_page = now_page;
            now_page = nxt_page;
        }
        else {
            off_t n_items = now_block->n_items;
            if (n_items == 1) {
                if (frn_page == now_page) {
                    HashMapDirectoryBlock *frn_block = (HashMapDirectoryBlock*)get_page(pool, (size / HASH_MAP_DIR_BLOCK_SIZE + 1) * PAGE_SIZE);
                    frn_block->directory[size % HASH_MAP_DIR_BLOCK_SIZE] = now_block->next;
                    release(pool, (size / HASH_MAP_DIR_BLOCK_SIZE + 1) * PAGE_SIZE);
                    printf("p1\n");
                }
                else {
                    HashMapBlock *frn_block = (HashMapBlock*)get_page(pool, frn_page);
                    frn_block->next = now_block->next;
                    release(pool, frn_page);
                }
                now_block->n_items = 0;
                now_block->table[0] = 0;
                HashMapControlBlock *ctrl = (HashMapControlBlock*)get_page(pool, 0);
                off_t free_block_ptr = ctrl->free_block_head;
                now_block->next = free_block_ptr;
                release(pool, now_page);
                ctrl->free_block_head = now_page;
                release(pool, 0);

                return;
            }
            else {
                for (off_t i = flag + 1; i < n_items; i++) {
                    now_block->table[i - 1] = now_block->table[i];
                }
                now_block->n_items--;
                now_block->table[now_block->n_items] = 0;
                release(pool, now_page);
                return;
            }
        }
    }
}

/* void print_hash_table(BufferPool *pool) {
    HashMapControlBlock *ctrl = (HashMapControlBlock*)get_page(pool, 0);
    HashMapDirectoryBlock *dir_block;
    off_t block_addr, next_addr;
    HashMapBlock *block;
    int i, j;
    printf("----------HASH TABLE----------\n");
    for (i = 0; i < ctrl->max_size; ++i) {
        dir_block = (HashMapDirectoryBlock*)get_page(pool, (i / HASH_MAP_DIR_BLOCK_SIZE + 1) * PAGE_SIZE);
        if (dir_block->directory[i % HASH_MAP_DIR_BLOCK_SIZE] != 0) {
            printf("%d:", i);
            block_addr = dir_block->directory[i % HASH_MAP_DIR_BLOCK_SIZE];
            while (block_addr != 0) {
                block = (HashMapBlock*)get_page(pool, block_addr);
                printf("  [" FORMAT_OFF_T "]", block_addr);
                printf("{");
                for (j = 0; j < block->n_items; ++j) {
                    if (j != 0) {
                        printf(", ");
                    }
                    printf(FORMAT_OFF_T, block->table[j]);
                }
                printf("}");
                next_addr = block->next;
                release(pool, block_addr);
                block_addr = next_addr;
            }
            printf("\n");
        }
        release(pool, (i / HASH_MAP_DIR_BLOCK_SIZE + 1) * PAGE_SIZE);
    }
    release(pool, 0);
    printf("------------------------------\n");
} */