#include "str.h"

#include "table.h"

void read_string(Table *table, RID rid, StringRecord *record) {
    table_read(table, rid, (ItemPtr)&(record->chunk));
    record->idx = 0;
}

int has_next_char(StringRecord *record) {
    if (get_str_chunk_size(&(record->chunk)) != record->idx) {
        return 1;
    }
    else {
        if (get_rid_idx(get_str_chunk_rid(&(record->chunk))) == -1) {
            return 0;
        }
        else {
            return 1;
        }
    }
}

char next_char(Table *table, StringRecord *record) {
    if (get_str_chunk_size(&(record->chunk)) != record->idx) {
        return get_str_chunk_data_ptr(&(record->chunk))[record->idx++];
    }
    else {
        RID rid = get_str_chunk_rid(&(record->chunk));
        if (get_rid_idx(rid) == -1) {
            printf("next char doesn't exists\n");
            return 0;
        }
        else {
            table_read(table, rid, (ItemPtr)&(record->chunk));
            record->idx = 0;
            return get_str_chunk_data_ptr(&(record->chunk))[record->idx++];
        }
    }
}

int compare_string_record(Table *table, const StringRecord *a, const StringRecord *b) {
    StringRecord recA = *a, recB = *b;
    size_t sizeA = get_str_chunk_size(&(recA.chunk)), sizeB = get_str_chunk_size(&(recB.chunk));
    if (sizeA == 0) {
        return -1;
    }
    if (sizeB == 0) {
        return 1;
    }
    while (has_next_char(&recA) && has_next_char(&recB)) {
        char a = next_char(table, &recA);
        char b = next_char(table, &recB);
        if (a != b) return a > b ? 1 : -1;
    }
    if (!has_next_char(&recA) && has_next_char(&recB)) {
        return -1;
    }
    if (has_next_char(&recA) && !has_next_char(&recB)) {
        return 1;
    }
    return 0;
}

RID write_string(Table *table, const char *data, off_t size) {
    FILE *debug_fp = fopen("str.log", "a");
    fprintf(debug_fp, "write string begin\n");
    fprintf(debug_fp, "max size of a chunk is %ld\n", STR_CHUNK_MAX_SIZE - sizeof(RID) - sizeof(short));
    short lst_chunk_size = size % (STR_CHUNK_MAX_SIZE - sizeof(RID) - sizeof(short));
    fprintf(debug_fp, "lst_chunk_size = %d\n", lst_chunk_size);
    if (lst_chunk_size == 0)    lst_chunk_size = (STR_CHUNK_MAX_SIZE - sizeof(RID) - sizeof(short));
    StringChunk lst_chunk;
    RID lst_rid;
    get_rid_block_addr(lst_rid) = -1;
    get_rid_idx(lst_rid) = -1;
    get_str_chunk_rid(&lst_chunk) = lst_rid;
    fprintf(debug_fp, "RID(" FORMAT_OFF_T ", %d)\n", get_rid_block_addr(lst_rid), get_rid_idx(lst_rid));
    get_str_chunk_size(&lst_chunk) = lst_chunk_size;
    fprintf(debug_fp, "data = ");
    for (int i = 0; i != lst_chunk_size; i++) {
        get_str_chunk_data_ptr(&lst_chunk)[i] = data[size - lst_chunk_size + i];
        fprintf(debug_fp, "%c", data[size - lst_chunk_size + i]);
    }
    fprintf(debug_fp, "\n");
    RID rid; 
    rid = table_insert(table, (ItemPtr)&lst_chunk, calc_str_chunk_size(lst_chunk_size));
    short full_chunk_num = (size - lst_chunk_size) / (STR_CHUNK_MAX_SIZE - sizeof(RID) - sizeof(short));
    fprintf(debug_fp, "full_chunk_num = %d\n", full_chunk_num);
    for (int chunk_iter = full_chunk_num; chunk_iter > 0; chunk_iter--) {
        StringChunk chunk;
        get_str_chunk_rid(&chunk) = rid;
        fprintf(debug_fp, "RID(" FORMAT_OFF_T ", %d)\n", get_rid_block_addr(rid), get_rid_idx(rid));
        get_str_chunk_size(&chunk) = STR_CHUNK_MAX_SIZE - sizeof(RID) - sizeof(short);
        fprintf(debug_fp, "data = ");
        for (int i = 0; i != STR_CHUNK_MAX_SIZE - sizeof(RID) - sizeof(short); i++) {
            get_str_chunk_data_ptr(&chunk)[i] = data[(chunk_iter - 1) * (STR_CHUNK_MAX_SIZE - sizeof(RID) - sizeof(short)) + i];
            fprintf(debug_fp, "%c", data[(chunk_iter - 1) * (STR_CHUNK_MAX_SIZE - sizeof(RID) - sizeof(short)) + i]);
        }
        fprintf(debug_fp, "\n");
        rid = table_insert(table, (ItemPtr)&chunk, sizeof(chunk));
    }
    fclose(debug_fp);
    return rid;
}

void delete_string(Table *table, RID rid) {
    FILE *debug_fp = fopen("str.log", "a");
    fprintf(debug_fp, "delete string begin\n");
    int i = 0;
    while (get_rid_idx(rid) != -1) {
        i++;

        StringChunk chunk;
        table_read(table, rid, (ItemPtr)&chunk);
        table_delete(table, rid);
        rid = get_str_chunk_rid(&chunk);
    }
    fprintf(debug_fp, "delete chunk number = %ld\n", i);
    fprintf(debug_fp, "delete string done\n");
    fclose(debug_fp);
}

/* void print_string(Table *table, const StringRecord *record) {
    StringRecord rec = *record;
    printf("\"");
    while (has_next_char(&rec)) {
        printf("%c", next_char(table, &rec));
    }
    printf("\"");
} */

size_t load_string(Table *table, const StringRecord *record, char *dest, size_t max_size) {
    StringRecord rec = *record;
    size_t size = 0;
    while (has_next_char(&rec) && size < max_size) {
        dest[size++] = next_char(table, &rec);
    }
    if (size < max_size)    dest[size] = 0;
    return size;
}

/* void chunk_printer(ItemPtr item, short item_size) {
    if (item == NULL) {
        printf("NULL");
        return;
    }
    StringChunk *chunk = (StringChunk*)item;
    short size = get_str_chunk_size(chunk), i;
    printf("StringChunk(");
    print_rid(get_str_chunk_rid(chunk));
    printf(", %d, \"", size);
    for (i = 0; i < size; i++) {
        printf("%c", get_str_chunk_data_ptr(chunk)[i]);
    }
    printf("\")");
} */